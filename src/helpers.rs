use anyhow::Context as _;
use chrono::Local;
use cosmrs::{
    proto::{
        cosmos::base::abci::v1beta1::TxResponse,
        tendermint::abci::{Event, EventAttribute},
        Timestamp,
    },
    Tx,
};
use sqlx::Transaction;
use std::{collections::HashMap, fmt, io, str::FromStr};

use crate::{
    configuration::{AppState, State},
    dao::DataBase,
    error::Error,
    handler::{
        wasm_lp_deposit, wasm_lp_withdraw, wasm_ls_auto_close_position,
        wasm_ls_close, wasm_ls_close_position, wasm_ls_liquidation,
        wasm_ls_liquidation_warning, wasm_ls_open, wasm_ls_repay,
        wasm_ls_slippage_anomaly, wasm_reserve_cover_loss, wasm_tr_profit,
        wasm_tr_rewards,
    },
    model::{
        Block, CosmosTypes, RawMsgParams, RawTxParams, Raw_Message,
        Subscription,
    },
    types::{
        Claims, Interest_values, LP_Deposit_Type, LP_Withdraw_Type,
        LS_Auto_Close_Position_Type, LS_Close_Position_Type, LS_Closing_Type,
        LS_Liquidation_Type, LS_Liquidation_Warning_Type, LS_Opening_Type,
        LS_Repayment_Type, LS_Slippage_Anomaly_Type, PushData, PushHeader,
        Reserve_Cover_Loss_Type, TR_Profit_Type, TR_Rewards_Distribution_Type,
    },
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD as BASE64_URL, Engine};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use reqwest::Url;

#[derive(Debug)]
pub enum Formatter {
    ParsedStr(String),
    Str(String),
    Number(i64),
    NumberU64(u64),
}

pub fn formatter(mut parser: String, args: &[Formatter]) -> String {
    for (index, value) in args.iter().enumerate() {
        match value {
            Formatter::ParsedStr(s) => {
                let parsed_string = format!(r#""{}""#, s);
                parser = parser
                    .replace(format!("${}", index).as_str(), &parsed_string);
            },
            Formatter::Number(n) => {
                parser = parser
                    .replace(format!("${}", index).as_str(), &n.to_string());
            },
            Formatter::NumberU64(n) => {
                parser = parser
                    .replace(format!("${}", index).as_str(), &n.to_string());
            },
            Formatter::Str(n) => {
                parser = parser
                    .replace(format!("${}", index).as_str(), &n.to_owned());
            },
        }
    }
    parser
}

/// Extracts a required field from a HashMap, returning an error if not found.
/// Reduces verbosity of repeated `.get().ok_or(Error::FieldNotExist(...))?.to_owned()` pattern.
fn extract_field(
    map: &HashMap<String, String>,
    key: &str,
) -> Result<String, Error> {
    map.get(key)
        .cloned()
        .ok_or_else(|| Error::FieldNotExist(key.to_string()))
}

pub fn parse_wasm_ls_open(
    attributes: &Vec<EventAttribute>,
) -> Result<LS_Opening_Type, Error> {
    let ls_open = parse_data(attributes)?;
    let c = LS_Opening_Type {
        id: extract_field(&ls_open, "id")?,
        customer: extract_field(&ls_open, "customer")?,
        currency: extract_field(&ls_open, "currency")?,
        air: extract_field(&ls_open, "air")?,
        at: extract_field(&ls_open, "at")?,
        loan_pool_id: extract_field(&ls_open, "loan-pool-id")?,
        loan_amount: extract_field(&ls_open, "loan-amount")?,
        loan_symbol: extract_field(&ls_open, "loan-symbol")?,
        downpayment_amount: extract_field(&ls_open, "downpayment-amount")?,
        downpayment_symbol: extract_field(&ls_open, "downpayment-symbol")?,
    };

    Ok(c)
}

pub fn parse_wasm_ls_close(
    attributes: &Vec<EventAttribute>,
) -> Result<LS_Closing_Type, Error> {
    let ls_close = parse_data(attributes)?;
    let c = LS_Closing_Type {
        id: extract_field(&ls_close, "id")?,
        at: extract_field(&ls_close, "at")?,
    };

    Ok(c)
}

pub fn parse_wasm_ls_repayment(
    attributes: &Vec<EventAttribute>,
) -> Result<LS_Repayment_Type, Error> {
    let ls_repayment = parse_data(attributes)?;
    let items = parse_interest_values(&ls_repayment)?;
    let c = LS_Repayment_Type {
        height: extract_field(&ls_repayment, "height")?,
        to: extract_field(&ls_repayment, "to")?,
        payment_symbol: extract_field(&ls_repayment, "payment-symbol")?,
        payment_amount: extract_field(&ls_repayment, "payment-amount")?,
        at: extract_field(&ls_repayment, "at")?,
        loan_close: extract_field(&ls_repayment, "loan-close")?,
        prev_margin_interest: items.prev_margin_interest,
        prev_loan_interest: items.prev_loan_interest,
        curr_margin_interest: items.curr_margin_interest,
        curr_loan_interest: items.curr_loan_interest,
        principal: extract_field(&ls_repayment, "principal")?,
    };

    Ok(c)
}

pub fn parse_wasm_ls_close_position(
    attributes: &Vec<EventAttribute>,
) -> Result<Option<LS_Close_Position_Type>, Error> {
    let ls_close_position = parse_data(attributes)?;

    if ls_close_position.contains_key("height") {
        let items = parse_interest_values(&ls_close_position)?;
        let c = LS_Close_Position_Type {
            height: extract_field(&ls_close_position, "height")?,
            to: extract_field(&ls_close_position, "to")?,
            change: extract_field(&ls_close_position, "change")?,
            amount_amount: extract_field(&ls_close_position, "amount-amount")?,
            amount_symbol: extract_field(&ls_close_position, "amount-symbol")?,
            payment_symbol: extract_field(
                &ls_close_position,
                "payment-symbol",
            )?,
            payment_amount: extract_field(
                &ls_close_position,
                "payment-amount",
            )?,
            at: extract_field(&ls_close_position, "at")?,
            loan_close: extract_field(&ls_close_position, "loan-close")?,
            prev_margin_interest: items.prev_margin_interest,
            prev_loan_interest: items.prev_loan_interest,
            curr_margin_interest: items.curr_margin_interest,
            curr_loan_interest: items.curr_loan_interest,
            principal: extract_field(&ls_close_position, "principal")?,
        };
        return Ok(Some(c));
    }

    Ok(None)
}

pub fn parse_wasm_ls_liquidation(
    attributes: &Vec<EventAttribute>,
) -> Result<LS_Liquidation_Type, Error> {
    let ls_liquidation = parse_data(attributes)?;
    let items = parse_interest_values(&ls_liquidation)?;

    let c = LS_Liquidation_Type {
        height: extract_field(&ls_liquidation, "height")?,
        to: extract_field(&ls_liquidation, "to")?,
        amount_symbol: extract_field(&ls_liquidation, "amount-symbol")?,
        amount_amount: extract_field(&ls_liquidation, "amount-amount")?,
        payment_symbol: extract_field(&ls_liquidation, "payment-symbol")?,
        payment_amount: extract_field(&ls_liquidation, "payment-amount")?,
        at: extract_field(&ls_liquidation, "at")?,
        r#type: extract_field(&ls_liquidation, "cause")?,
        loan_close: extract_field(&ls_liquidation, "loan-close")?,
        prev_margin_interest: items.prev_margin_interest,
        prev_loan_interest: items.prev_loan_interest,
        curr_margin_interest: items.curr_margin_interest,
        curr_loan_interest: items.curr_loan_interest,
        principal: extract_field(&ls_liquidation, "principal")?,
    };

    Ok(c)
}

pub fn parse_wasm_ls_liquidation_warning(
    attributes: &Vec<EventAttribute>,
) -> Result<LS_Liquidation_Warning_Type, Error> {
    let ls_liquidation_warning = parse_data(attributes)?;
    let c = LS_Liquidation_Warning_Type {
        customer: extract_field(&ls_liquidation_warning, "customer")?,
        lease: extract_field(&ls_liquidation_warning, "lease")?,
        lease_asset: extract_field(&ls_liquidation_warning, "lease-asset")?,
        level: extract_field(&ls_liquidation_warning, "level")?,
        ltv: extract_field(&ls_liquidation_warning, "ltv")?,
    };

    Ok(c)
}

pub fn parse_wasm_ls_slippage_anomaly(
    attributes: &Vec<EventAttribute>,
) -> Result<LS_Slippage_Anomaly_Type, Error> {
    let ls_slippage_anomaly = parse_data(attributes)?;
    let c = LS_Slippage_Anomaly_Type {
        customer: extract_field(&ls_slippage_anomaly, "customer")?,
        lease: extract_field(&ls_slippage_anomaly, "lease")?,
        lease_asset: extract_field(&ls_slippage_anomaly, "lease-asset")?,
        max_slippage: extract_field(&ls_slippage_anomaly, "max_slippage")?,
    };

    Ok(c)
}

pub fn parse_wasm_ls_auto_close_position(
    attributes: &Vec<EventAttribute>,
) -> Result<LS_Auto_Close_Position_Type, Error> {
    let ls_auto_close_position = parse_data(attributes)?;
    let c = LS_Auto_Close_Position_Type {
        to: extract_field(&ls_auto_close_position, "to")?,
        take_profit_ltv: ls_auto_close_position.get("take-profit-ltv").cloned(),
        stop_loss_ltv: ls_auto_close_position.get("stop-loss-ltv").cloned(),
    };

    Ok(c)
}

pub fn parse_wasm_reserve_cover_loss(
    attributes: &Vec<EventAttribute>,
) -> Result<Reserve_Cover_Loss_Type, Error> {
    let reserve_cover_loss = parse_data(attributes)?;
    let c = Reserve_Cover_Loss_Type {
        to: extract_field(&reserve_cover_loss, "to")?,
        payment_symbol: extract_field(&reserve_cover_loss, "payment-symbol")?,
        payment_amount: extract_field(&reserve_cover_loss, "payment-amount")?,
    };

    Ok(c)
}

pub fn parse_interest_values(
    value: &HashMap<String, String>,
) -> Result<Interest_values, Error> {
    let prev_margin_interest =
        match value.get("prev-margin-interest") {
            Some(prev_margin_interest) => prev_margin_interest,
            None => value.get("overdue-margin-interest").ok_or(
                Error::FieldNotExist(String::from("prev-margin-interest")),
            )?,
        };

    let prev_loan_interest = match value.get("prev-loan-interest") {
        Some(prev_loan_interest) => prev_loan_interest,
        None => value
            .get("overdue-loan-interest")
            .ok_or(Error::FieldNotExist(String::from("prev-loan-interest")))?,
    };

    let curr_margin_interest = match value.get("curr-margin-interest") {
        Some(curr_margin_interest) => curr_margin_interest,
        None => {
            value
                .get("due-margin-interest")
                .ok_or(Error::FieldNotExist(String::from(
                    "curr-margin-interest",
                )))?
        },
    };

    let curr_loan_interest = match value.get("curr-loan-interest") {
        Some(curr_loan_interest) => curr_loan_interest,
        None => value
            .get("due-loan-interest")
            .ok_or(Error::FieldNotExist(String::from("curr-loan-interest")))?,
    };

    Ok(Interest_values {
        prev_margin_interest: prev_margin_interest.to_owned(),
        prev_loan_interest: prev_loan_interest.to_owned(),
        curr_margin_interest: curr_margin_interest.to_owned(),
        curr_loan_interest: curr_loan_interest.to_owned(),
    })
}

pub fn parse_wasm_lp_deposit(
    attributes: &Vec<EventAttribute>,
) -> Result<LP_Deposit_Type, Error> {
    let deposit = parse_data(attributes)?;

    let c = LP_Deposit_Type {
        height: extract_field(&deposit, "height")?,
        from: extract_field(&deposit, "from")?,
        to: extract_field(&deposit, "to")?,
        at: extract_field(&deposit, "at")?,
        deposit_amount: extract_field(&deposit, "deposit-amount")?,
        deposit_symbol: extract_field(&deposit, "deposit-symbol")?,
        receipts: extract_field(&deposit, "receipts")?,
    };

    Ok(c)
}

pub fn parse_wasm_lp_withdraw(
    attributes: &Vec<EventAttribute>,
) -> Result<LP_Withdraw_Type, Error> {
    let lp_withdraw = parse_data(attributes)?;

    let c = LP_Withdraw_Type {
        height: extract_field(&lp_withdraw, "height")?,
        from: extract_field(&lp_withdraw, "from")?,
        to: extract_field(&lp_withdraw, "to")?,
        at: extract_field(&lp_withdraw, "at")?,
        withdraw_amount: extract_field(&lp_withdraw, "withdraw-amount")?,
        withdraw_symbol: extract_field(&lp_withdraw, "withdraw-symbol")?,
        receipts: extract_field(&lp_withdraw, "receipts")?,
        close: extract_field(&lp_withdraw, "close")?,
    };

    Ok(c)
}

pub fn parse_wasm_tr_profit(
    attributes: &Vec<EventAttribute>,
) -> Result<TR_Profit_Type, Error> {
    let tr_profit = parse_data(attributes)?;

    let c = TR_Profit_Type {
        height: extract_field(&tr_profit, "height")?,
        at: extract_field(&tr_profit, "at")?,
        profit_symbol: extract_field(&tr_profit, "profit-amount-symbol")?,
        profit_amount: extract_field(&tr_profit, "profit-amount-amount")?,
    };

    Ok(c)
}

pub fn parse_wasm_tr_rewards_distribution(
    attributes: &Vec<EventAttribute>,
) -> Result<TR_Rewards_Distribution_Type, Error> {
    let tr_rewards_distribution = parse_data(attributes)?;

    let c = TR_Rewards_Distribution_Type {
        height: extract_field(&tr_rewards_distribution, "height")?,
        to: extract_field(&tr_rewards_distribution, "to")?,
        at: extract_field(&tr_rewards_distribution, "at")?,
        rewards_symbol: extract_field(
            &tr_rewards_distribution,
            "rewards-symbol",
        )?,
        rewards_amount: extract_field(
            &tr_rewards_distribution,
            "rewards-amount",
        )?,
    };

    Ok(c)
}

fn parse_data(
    attributes: &Vec<EventAttribute>,
) -> Result<HashMap<String, String>, Error> {
    let mut data: HashMap<String, String> = HashMap::new();
    for attribute in attributes {
        let value = attribute.value.to_owned();
        let key = attribute.key.to_owned();
        if data.contains_key(&key) {
            return Err(Error::DuplicateField(key));
        }
        data.insert(key, value);
    }

    Ok(data)
}

pub async fn parse_event(
    app_state: AppState<State>,
    event: &Event,
    index: usize,
    time_stamp: Timestamp,
    tx_hash: String,
    height: i64,
    tx: &mut Transaction<'_, DataBase>,
) -> Result<(), Error> {
    if let Ok(t) = EventsType::from_str(&event.r#type) {
        match t {
            EventsType::LS_Opening => {
                let wasm_ls_opening = parse_wasm_ls_open(&event.attributes)?;
                wasm_ls_open::parse_and_insert(
                    &app_state,
                    wasm_ls_opening,
                    tx_hash,
                    height,
                    tx,
                )
                .await?;
            },
            EventsType::LS_Closing => {
                let wasm_ls_closing = parse_wasm_ls_close(&event.attributes)?;
                wasm_ls_close::parse_and_insert(
                    &app_state,
                    wasm_ls_closing,
                    tx_hash,
                    tx,
                )
                .await?;
            },
            EventsType::LS_Close_Position => {
                let wasm_ls_close_position =
                    parse_wasm_ls_close_position(&event.attributes)?;
                if let Some(item) = wasm_ls_close_position {
                    wasm_ls_close_position::parse_and_insert(
                        &app_state, item, tx_hash, height, tx,
                    )
                    .await?;
                }
            },
            EventsType::LS_Repay => {
                let wasm_ls_repay = parse_wasm_ls_repayment(&event.attributes)?;
                wasm_ls_repay::parse_and_insert(
                    &app_state,
                    wasm_ls_repay,
                    tx_hash,
                    height,
                    tx,
                )
                .await?;
            },
            EventsType::LS_Liquidation => {
                let wasm_ls_liquidation =
                    parse_wasm_ls_liquidation(&event.attributes)?;
                wasm_ls_liquidation::parse_and_insert(
                    &app_state,
                    wasm_ls_liquidation,
                    tx_hash,
                    height,
                    tx,
                )
                .await?;
            },
            EventsType::LS_Liquidation_Warning => {
                let ls_liquidation_warning =
                    parse_wasm_ls_liquidation_warning(&event.attributes)?;
                wasm_ls_liquidation_warning::parse_and_insert(
                    &app_state,
                    ls_liquidation_warning,
                    time_stamp,
                    tx_hash,
                    tx,
                )
                .await?;
            },
            EventsType::LS_Slippage_Anomaly => {
                let ls_slippage_anomaly =
                    parse_wasm_ls_slippage_anomaly(&event.attributes)?;
                wasm_ls_slippage_anomaly::parse_and_insert(
                    &app_state,
                    ls_slippage_anomaly,
                    time_stamp,
                    tx_hash,
                    tx,
                )
                .await?;
            },
            EventsType::LS_Auto_Close_Position => {
                let ls_auto_close_position =
                    parse_wasm_ls_auto_close_position(&event.attributes)?;
                wasm_ls_auto_close_position::parse_and_insert(
                    &app_state,
                    ls_auto_close_position,
                    time_stamp,
                    tx_hash,
                    tx,
                )
                .await?;
            },
            EventsType::Reserve_Cover_Loss => {
                let reserve_cover_loss =
                    parse_wasm_reserve_cover_loss(&event.attributes)?;
                wasm_reserve_cover_loss::parse_and_insert(
                    &app_state,
                    reserve_cover_loss,
                    index,
                    time_stamp,
                    tx_hash,
                    tx,
                )
                .await?;
            },
            EventsType::LP_deposit => {
                let wasm_lp_deposit = parse_wasm_lp_deposit(&event.attributes)?;
                wasm_lp_deposit::parse_and_insert(
                    &app_state,
                    wasm_lp_deposit,
                    tx_hash,
                    tx,
                )
                .await?;
            },
            EventsType::LP_Withdraw => {
                let wasm_lp_withdraw =
                    parse_wasm_lp_withdraw(&event.attributes)?;
                wasm_lp_withdraw::parse_and_insert(
                    &app_state,
                    wasm_lp_withdraw,
                    tx_hash,
                    tx,
                )
                .await?;
            },
            EventsType::TR_Profit => {
                let wasm_tr_profit = parse_wasm_tr_profit(&event.attributes)?;
                wasm_tr_profit::parse_and_insert(
                    &app_state,
                    wasm_tr_profit,
                    tx_hash,
                    tx,
                )
                .await?;
            },
            EventsType::TR_Rewards_Distribution => {
                let wasm_tr_rewards_distribution =
                    parse_wasm_tr_rewards_distribution(&event.attributes)?;
                wasm_tr_rewards::parse_and_insert(
                    &app_state,
                    wasm_tr_rewards_distribution,
                    index,
                    tx_hash,
                    tx,
                )
                .await?;
            },
        }
    }
    Ok(())
}

pub async fn insert_txs(
    app_state: AppState<State>,
    txs: Vec<Option<TxResponse>>,
    height: i64,
    time_stamp: Timestamp,
) -> Result<bool, Error> {
    let block = app_state.database.block.get_one(height).await?;

    if block.is_none() {
        let mut tx = app_state.database.pool.begin().await?;
        for tx_results in txs.into_iter().flatten() {
            let hash = tx_results.txhash.to_owned();
            let tx_data =
                tx_results.tx.context("could not find Any message")?;
            parse_raw_tx(
                app_state.clone(),
                RawTxParams {
                    tx_hash: tx_results.txhash,
                    tx_data,
                    height,
                    code: tx_results.code,
                    time_stamp,
                    tx_events: &tx_results.events,
                },
                &mut tx,
            )
            .await?;
            for (index, event) in tx_results.events.iter().enumerate() {
                parse_event(
                    app_state.clone(),
                    event,
                    index,
                    time_stamp,
                    hash.to_owned(),
                    height,
                    &mut tx,
                )
                .await?;
            }
        }

        app_state
            .database
            .block
            .insert(Block { id: height }, &mut tx)
            .await?;

        tx.commit().await?;
    }

    Ok(true)
}

pub async fn parse_raw_tx(
    app_state: AppState<State>,
    params: RawTxParams<'_>,
    tx: &mut Transaction<'_, DataBase>,
) -> Result<(), Error> {
    let c = Tx::from_bytes(&params.tx_data.value)?;
    for (index, msg) in c.body.messages.iter().enumerate() {
        let fee = c.auth_info.fee.clone();
        let memo = c.body.memo.to_owned();
        let msg: Result<Raw_Message, anyhow::Error> =
            Raw_Message::from_any(RawMsgParams {
                index: index.try_into()?,
                value: msg.clone(),
                tx_hash: params.tx_hash.clone(),
                block: params.height,
                time_stamp: params.time_stamp,
                fee,
                memo,
                events: app_state.config.events_subscribe.clone(),
                tx_events: params.tx_events,
                code: params.code,
            });

        if let Ok(msg) = msg {
            app_state
                .database
                .raw_message
                .insert_if_not_exists(msg, tx)
                .await?;
        }
    }

    Ok(())
}

pub fn send_push_task(
    state: AppState<State>,
    subscription: Subscription,
    push_header: PushHeader,
    push_data: PushData,
) {
    let permits = state.push_permits.clone();
    tokio::spawn(async move {
        // Acquire permit to limit concurrent push tasks
        let _permit = match permits.acquire().await {
            Ok(permit) => permit,
            Err(_) => {
                tracing::error!("Push notification semaphore closed");
                return;
            },
        };
        let res = send_push(state, subscription, push_header, push_data).await;
        if let Err(e) = res {
            tracing::error!("Push notification failed: {}", e);
        };
    });
}

pub async fn send_push(
    state: AppState<State>,
    subscription: Subscription,
    push_header: PushHeader,
    push_data: PushData,
) -> Result<(), Error> {
    let url = Url::parse(&subscription.endpoint)?;
    let exp = Local::now().timestamp_millis() / 1000 + push_header.ttl;

    let scheme = url.scheme();
    let host = if let Some(h) = url.host() {
        h.to_string()
    } else {
        return Err(Error::InvalidOption {
            option: String::from("host"),
        });
    };

    let aud = format!("{}://{}", scheme, host);
    let sub = format!("mailto:{}", &state.config.mail_to);

    let key = EncodingKey::from_ec_pem(&state.config.vapid_private_key)?;
    let claims = Claims { aud, sub, exp };
    let token = encode(&Header::new(Algorithm::ES256), &claims, &key)?;

    let p256dh = BASE64_URL.decode(subscription.p256dh)?;
    let auth = BASE64_URL.decode(subscription.auth)?;

    let data = ece::encrypt(&p256dh, &auth, push_data.to_string().as_bytes())?;
    let endpoint = subscription.endpoint.to_string();

    let status = state
        .http
        .post_push(subscription.endpoint, token, push_header, data)
        .await?;

    if state.config.status_code_to_delete.contains(&status) {
        state.database.subscription.deactivate(endpoint).await?;
    }

    Ok(())
}

#[derive(Debug)]
pub enum EventsType {
    LS_Opening,
    LS_Closing,
    LS_Close_Position,
    LS_Repay,
    LS_Liquidation,
    LS_Liquidation_Warning,
    LS_Slippage_Anomaly,
    LS_Auto_Close_Position,
    Reserve_Cover_Loss,

    LP_deposit,
    LP_Withdraw,
    TR_Profit,
    TR_Rewards_Distribution,
}

impl EventsType {
    /// Returns the canonical string representation of this event type.
    /// Single source of truth for event type string mappings.
    pub fn as_str(&self) -> &'static str {
        match self {
            EventsType::LS_Opening => "wasm-ls-open",
            EventsType::LS_Closing => "wasm-ls-close",
            EventsType::LS_Close_Position => "wasm-ls-close-position",
            EventsType::LS_Repay => "wasm-ls-repay",
            EventsType::LS_Liquidation => "wasm-ls-liquidation",
            EventsType::LS_Liquidation_Warning => "wasm-ls-liquidation-warning",
            EventsType::LS_Slippage_Anomaly => "wasm-ls-slippage-anomaly",
            EventsType::LS_Auto_Close_Position => "wasm-ls-auto-close-position",
            EventsType::Reserve_Cover_Loss => "wasm-reserve-cover-loss",
            EventsType::LP_deposit => "wasm-lp-deposit",
            EventsType::LP_Withdraw => "wasm-lp-withdraw",
            EventsType::TR_Profit => "wasm-tr-profit",
            EventsType::TR_Rewards_Distribution => "wasm-tr-rewards",
        }
    }
}

impl fmt::Display for EventsType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl From<EventsType> for String {
    fn from(value: EventsType) -> Self {
        value.as_str().to_string()
    }
}

impl FromStr for EventsType {
    type Err = io::Error;

    fn from_str(value: &str) -> Result<EventsType, Self::Err> {
        match value {
            "wasm-ls-open" => Ok(EventsType::LS_Opening),
            "wasm-ls-close" => Ok(EventsType::LS_Closing),
            "wasm-ls-close-position" => Ok(EventsType::LS_Close_Position),
            "wasm-ls-repay" => Ok(EventsType::LS_Repay),
            "wasm-ls-liquidation" => Ok(EventsType::LS_Liquidation),
            "wasm-ls-liquidation-warning" => {
                Ok(EventsType::LS_Liquidation_Warning)
            },
            "wasm-ls-slippage-anomaly" => Ok(EventsType::LS_Slippage_Anomaly),
            "wasm-ls-auto-close-position" => {
                Ok(EventsType::LS_Auto_Close_Position)
            },
            "wasm-reserve-cover-loss" => Ok(EventsType::Reserve_Cover_Loss),
            "wasm-lp-deposit" => Ok(EventsType::LP_deposit),
            "wasm-lp-withdraw" => Ok(EventsType::LP_Withdraw),
            "wasm-tr-profit" => Ok(EventsType::TR_Profit),
            "wasm-tr-rewards" => Ok(EventsType::TR_Rewards_Distribution),
            _ => Err(io::Error::other("Message Type not supported")),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Loan_Closing_Status {
    Repay,
    Liquidation,
    MarketClose,
    None,
}

impl fmt::Display for Loan_Closing_Status {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Loan_Closing_Status::Repay => write!(f, "repay"),
            Loan_Closing_Status::Liquidation => write!(f, "liquidation"),
            Loan_Closing_Status::MarketClose => {
                write!(f, "market-close")
            },
            Loan_Closing_Status::None => {
                write!(f, "none")
            },
        }
    }
}

impl From<Loan_Closing_Status> for String {
    fn from(value: Loan_Closing_Status) -> Self {
        match value {
            Loan_Closing_Status::Repay => String::from("repay"),
            Loan_Closing_Status::Liquidation => String::from("liquidation"),
            Loan_Closing_Status::MarketClose => String::from("market-close"),
            Loan_Closing_Status::None => String::from("none"),
        }
    }
}

impl FromStr for Loan_Closing_Status {
    type Err = io::Error;

    fn from_str(value: &str) -> Result<Loan_Closing_Status, Self::Err> {
        match value {
            "repay" => Ok(Loan_Closing_Status::Repay),
            "liquidation" => Ok(Loan_Closing_Status::Liquidation),
            "market-close" => Ok(Loan_Closing_Status::MarketClose),
            "none" => Ok(Loan_Closing_Status::None),
            _ => Err(io::Error::other("Loan_Closing_Status not supported")),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Auto_Close_Strategies {
    TakeProfit,
    StopLoss,
}

impl fmt::Display for Auto_Close_Strategies {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Auto_Close_Strategies::TakeProfit => write!(f, "take-profit"),
            Auto_Close_Strategies::StopLoss => write!(f, "stop-loss"),
        }
    }
}

impl From<Auto_Close_Strategies> for String {
    fn from(value: Auto_Close_Strategies) -> Self {
        match value {
            Auto_Close_Strategies::TakeProfit => String::from("take-profit"),
            Auto_Close_Strategies::StopLoss => String::from("stop-loss"),
        }
    }
}

impl FromStr for Auto_Close_Strategies {
    type Err = io::Error;

    fn from_str(value: &str) -> Result<Auto_Close_Strategies, Self::Err> {
        match value {
            "take-profit" => Ok(Auto_Close_Strategies::TakeProfit),
            "stop-loss" => Ok(Auto_Close_Strategies::StopLoss),
            _ => Err(io::Error::other("Auto_Close_Strategies not supported")),
        }
    }
}

#[derive(Debug)]
pub enum Status {
    Subscribed,
    Unsubscribed,
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Status::Subscribed => write!(f, "subscribed"),
            Status::Unsubscribed => write!(f, "unsubscribed"),
        }
    }
}

impl From<Status> for String {
    fn from(value: Status) -> Self {
        match value {
            Status::Subscribed => String::from("subscribed"),
            Status::Unsubscribed => String::from("unsubscribed"),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Filter_Types {
    Transfers,
    Earn,
    Staking,
    Positions,
    PositionsIds,
}

impl fmt::Display for Filter_Types {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Filter_Types::Transfers => write!(f, "transfers"),
            Filter_Types::Earn => write!(f, "earn"),
            Filter_Types::Staking => write!(f, "staking"),
            Filter_Types::Positions => write!(f, "positions"),
            Filter_Types::PositionsIds => write!(f, "positions_ids"),
        }
    }
}

impl From<Filter_Types> for String {
    fn from(value: Filter_Types) -> Self {
        match value {
            Filter_Types::Transfers => String::from("transfers"),
            Filter_Types::Earn => String::from("earn"),
            Filter_Types::Staking => String::from("staking"),
            Filter_Types::Positions => String::from("positions"),
            Filter_Types::PositionsIds => String::from("positions_ids"),
        }
    }
}

impl FromStr for Filter_Types {
    type Err = io::Error;

    fn from_str(value: &str) -> Result<Filter_Types, Self::Err> {
        match value {
            "transfers" => Ok(Filter_Types::Transfers),
            "earn" => Ok(Filter_Types::Earn),
            "staking" => Ok(Filter_Types::Staking),
            "positions" => Ok(Filter_Types::Positions),
            "positions_ids" => Ok(Filter_Types::PositionsIds),
            _ => Err(io::Error::other("Filter_Types not supported")),
        }
    }
}

impl From<Filter_Types> for Vec<String> {
    fn from(value: Filter_Types) -> Self {
        match value {
            Filter_Types::Transfers => {
                vec![
                    CosmosTypes::MsgSend.to_string(),
                    CosmosTypes::MsgTransfer.to_string(),
                    CosmosTypes::MsgRecvPacket.to_string(),
                ]
            },
            Filter_Types::Earn => {
                vec![CosmosTypes::MsgExecuteContract.to_string()]
            },
            Filter_Types::Staking => {
                vec![
                    CosmosTypes::MsgDelegate.to_string(),
                    CosmosTypes::MsgUndelegate.to_string(),
                    CosmosTypes::MsgBeginRedelegate.to_string(),
                    CosmosTypes::MsgWithdrawDelegatorReward.to_string(),
                ]
            },
            Filter_Types::Positions => {
                vec![CosmosTypes::MsgExecuteContract.to_string()]
            },
            Filter_Types::PositionsIds => {
                vec![CosmosTypes::MsgExecuteContract.to_string()]
            },
        }
    }
}

use chrono::{DateTime, Utc};
use std::future::Future;

use crate::cache::TimedCache;

// ============================================================================
// Generic Cache Helpers
// ============================================================================

/// Fetches a cached value or computes it using the provided async function.
/// Handles the common pattern of: check cache -> fetch from DB -> store in cache.
///
/// # Arguments
/// * `cache` - The TimedCache instance to use
/// * `key` - Cache key string
/// * `fetch_fn` - Async function that fetches the value if not cached
///
/// # Example
/// ```ignore
/// let data = cached_fetch(
///     &state.api_cache.revenue,
///     "revenue",
///     || async { state.database.tr_profit.get_revenue().await }
/// ).await?;
/// ```
pub async fn cached_fetch<T, F, Fut>(
    cache: &TimedCache<T>,
    key: &str,
    fetch_fn: F,
) -> Result<T, crate::error::Error>
where
    T: Clone + Send + Sync,
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<T, crate::error::Error>>,
{
    // Use get_or_fetch with stampede protection and stale-while-revalidate
    cache.get_or_fetch(key, fetch_fn).await
}

/// Time window filter parameters for historical endpoints.
/// Supports both period-based filtering (3m/6m/12m/all) and
/// timestamp-based filtering (from) for incremental syncing.
#[derive(Debug, Clone)]
pub struct TimeWindowParams {
    /// Number of months to look back (None = all time)
    pub months: Option<i32>,
    /// Only return records after this timestamp (exclusive)
    pub from: Option<DateTime<Utc>>,
}

/// Build a cache key for period-based endpoints.
/// Includes endpoint name, period, and optional from timestamp.
///
/// # Examples
/// ```ignore
/// build_cache_key("liquidations", "12m", None) // "liquidations_12m_none"
/// build_cache_key("liquidations", "3m", Some(ts)) // "liquidations_3m_1234567890"
/// ```
pub fn build_cache_key(
    endpoint: &str,
    period: &str,
    from: Option<DateTime<Utc>>,
) -> String {
    let from_key = from
        .map(|ts| ts.timestamp().to_string())
        .unwrap_or_else(|| "none".to_string());
    format!("{}_{}_{}", endpoint, period, from_key)
}

/// Build a cache key for protocol-specific period-based endpoints.
/// Includes endpoint name, protocol, period, and optional from timestamp.
///
/// # Examples
/// ```ignore
/// build_cache_key_with_protocol("borrow_apr", "OSMOSIS", "3m", None) // "borrow_apr_OSMOSIS_3m_none"
/// ```
pub fn build_cache_key_with_protocol(
    endpoint: &str,
    protocol: &str,
    period: &str,
    from: Option<DateTime<Utc>>,
) -> String {
    let from_key = from
        .map(|ts| ts.timestamp().to_string())
        .unwrap_or_else(|| "none".to_string());
    format!("{}_{}_{}_{}", endpoint, protocol, period, from_key)
}

/// Build a cache key for protocol-specific endpoints (no period).
/// Returns "endpoint_PROTOCOL" or "endpoint_total" if protocol is None.
///
/// # Examples
/// ```ignore
/// build_protocol_cache_key("borrowed", Some("OSMOSIS")) // "borrowed_OSMOSIS"
/// build_protocol_cache_key("borrowed", None) // "borrowed_total"
/// ```
pub fn build_protocol_cache_key(
    endpoint: &str,
    protocol: Option<&str>,
) -> String {
    match protocol {
        Some(p) => format!("{}_{}", endpoint, p.to_uppercase()),
        None => format!("{}_total", endpoint),
    }
}

/// Parse period query parameter to number of months for time window filtering.
/// Returns Some(months) for time-limited queries, None for "all" (no limit).
/// Default is 3 months if no period specified.
pub fn parse_period_months(
    period: &Option<String>,
) -> Result<Option<i32>, Error> {
    match period.as_deref() {
        None | Some("3m") => Ok(Some(3)),
        Some("6m") => Ok(Some(6)),
        Some("12m") => Ok(Some(12)),
        Some("all") => Ok(None),
        Some(p) => Err(Error::InvalidOption {
            option: format!("period '{}'. Valid options: 3m, 6m, 12m, all", p),
        }),
    }
}

/// Parse period and from parameters into TimeWindowParams.
/// - period: time window (3m/6m/12m/all), default 12m
/// - from: optional timestamp to filter records after (exclusive)
///
/// Both filters are applied together (AND logic).
pub fn parse_time_window(
    period: &Option<String>,
    from: Option<DateTime<Utc>>,
) -> Result<TimeWindowParams, Error> {
    let months = parse_period_months(period)?;
    Ok(TimeWindowParams { months, from })
}

/// Generate a CSV response from serializable data
pub fn to_csv_response<T: serde::Serialize>(
    data: &[T],
    filename: &str,
) -> Result<actix_web::HttpResponse, Error> {
    let mut wtr = csv::Writer::from_writer(vec![]);
    for record in data {
        wtr.serialize(record).map_err(|e| {
            Error::ServerError(format!("CSV serialization error: {}", e))
        })?;
    }
    let csv_data = wtr
        .into_inner()
        .map_err(|e| Error::ServerError(format!("CSV writer error: {}", e)))?;
    let csv_string = String::from_utf8(csv_data)?;

    Ok(actix_web::HttpResponse::Ok()
        .content_type("text/csv")
        .insert_header((
            "Content-Disposition",
            format!("attachment; filename=\"{}\"", filename),
        ))
        .body(csv_string))
}

/// Generate a streaming CSV response from serializable data.
/// This is more memory-efficient for large datasets as it streams
/// data directly to the response without loading everything into memory.
pub fn to_streaming_csv_response<T: serde::Serialize>(
    data: Vec<T>,
    filename: &str,
) -> Result<actix_web::HttpResponse, Error> {
    use actix_web::web::Bytes;

    // Serialize all data to CSV bytes
    let mut wtr = csv::Writer::from_writer(vec![]);
    for record in &data {
        wtr.serialize(record).map_err(|e| {
            Error::ServerError(format!("CSV serialization error: {}", e))
        })?;
    }
    let csv_data = wtr
        .into_inner()
        .map_err(|e| Error::ServerError(format!("CSV writer error: {}", e)))?;

    let bytes = Bytes::from(csv_data);

    Ok(actix_web::HttpResponse::Ok()
        .content_type("text/csv")
        .insert_header((
            "Content-Disposition",
            format!("attachment; filename=\"{}\"", filename),
        ))
        .body(bytes))
}
