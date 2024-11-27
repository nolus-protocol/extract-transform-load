use crate::configuration::{AppState, State};
use crate::dao::DataBase;
use crate::handler::{
    wams_reserve_cover_loss, wasm_lp_deposit, wasm_lp_withdraw, wasm_ls_close,
    wasm_ls_close_position, wasm_ls_liquidation, wasm_ls_liquidation_warning,
    wasm_ls_open, wasm_ls_repay, wasm_tr_profit, wasm_tr_rewards,
};
use crate::model::{Block, Raw_Message};
use crate::{
    error::Error,
    types::{
        Interest_values, LP_Deposit_Type, LP_Withdraw_Type, LS_Closing_Type,
        LS_Liquidation_Type, LS_Opening_Type, LS_Repayment_Type,
        TR_Profit_Type, TR_Rewards_Distribution_Type,
    },
};

use crate::types::{
    LS_Close_Position_Type, LS_Liquidation_Warning_Type,
    Reserve_Cover_Loss_Type,
};
use anyhow::Context;
use cosmos_sdk_proto::cosmos::base::abci::v1beta1::TxResponse;
use cosmos_sdk_proto::tendermint::abci::{Event, EventAttribute};
use cosmos_sdk_proto::Timestamp;

use cosmrs::{Any, Tx};
use sqlx::Transaction;
use std::{collections::HashMap, fmt, io, str::FromStr};

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

pub fn parse_tuple_string(data: String) -> Vec<String> {
    let str = &data[1..];
    let splited = str.split(",(");
    let mut items: Vec<String> = Vec::new();

    for c in splited {
        if let Some(index) = c.find(')') {
            let tuple_data = &c[0..index];
            items.push(tuple_data.to_owned());
        }
    }

    items
}

pub fn parse_wasm_ls_open(
    attributes: &Vec<EventAttribute>,
) -> Result<LS_Opening_Type, Error> {
    let ls_open = pasrse_data(attributes)?;
    let c = LS_Opening_Type {
        id: ls_open
            .get("id")
            .ok_or(Error::FieldNotExist(String::from("id")))?
            .to_owned(),
        customer: ls_open
            .get("customer")
            .ok_or(Error::FieldNotExist(String::from("customer")))?
            .to_owned(),
        currency: ls_open
            .get("currency")
            .ok_or(Error::FieldNotExist(String::from("currency")))?
            .to_owned(),
        air: ls_open
            .get("air")
            .ok_or(Error::FieldNotExist(String::from("air")))?
            .to_owned(),
        at: ls_open
            .get("at")
            .ok_or(Error::FieldNotExist(String::from("at")))?
            .to_owned(),
        loan_pool_id: ls_open
            .get("loan-pool-id")
            .ok_or(Error::FieldNotExist(String::from("loan-pool-id")))?
            .to_owned(),
        loan_amount: ls_open
            .get("loan-amount")
            .ok_or(Error::FieldNotExist(String::from("loan-amount")))?
            .to_owned(),
        loan_symbol: ls_open
            .get("loan-symbol")
            .ok_or(Error::FieldNotExist(String::from("loan-symbol")))?
            .to_owned(),
        downpayment_amount: ls_open
            .get("downpayment-amount")
            .ok_or(Error::FieldNotExist(String::from("downpayment-amount")))?
            .to_owned(),
        downpayment_symbol: ls_open
            .get("downpayment-symbol")
            .ok_or(Error::FieldNotExist(String::from("downpayment-symbol")))?
            .to_owned(),
    };

    Ok(c)
}

pub fn parse_wasm_ls_close(
    attributes: &Vec<EventAttribute>,
) -> Result<LS_Closing_Type, Error> {
    let ls_close = pasrse_data(attributes)?;
    let c = LS_Closing_Type {
        id: ls_close
            .get("id")
            .ok_or(Error::FieldNotExist(String::from("id")))?
            .to_owned(),
        at: ls_close
            .get("at")
            .ok_or(Error::FieldNotExist(String::from("at")))?
            .to_owned(),
    };

    Ok(c)
}

pub fn parse_wasm_ls_repayment(
    attributes: &Vec<EventAttribute>,
) -> Result<LS_Repayment_Type, Error> {
    let ls_repayment = pasrse_data(attributes)?;
    let items = parseInterestValues(&ls_repayment)?;
    let c = LS_Repayment_Type {
        height: ls_repayment
            .get("height")
            .ok_or(Error::FieldNotExist(String::from("height")))?
            .to_owned(),
        to: ls_repayment
            .get("to")
            .ok_or(Error::FieldNotExist(String::from("to")))?
            .to_owned(),
        payment_symbol: ls_repayment
            .get("payment-symbol")
            .ok_or(Error::FieldNotExist(String::from("payment-symbol")))?
            .to_owned(),
        payment_amount: ls_repayment
            .get("payment-amount")
            .ok_or(Error::FieldNotExist(String::from("payment-amount")))?
            .to_owned(),
        at: ls_repayment
            .get("at")
            .ok_or(Error::FieldNotExist(String::from("at")))?
            .to_owned(),
        loan_close: ls_repayment
            .get("loan-close")
            .ok_or(Error::FieldNotExist(String::from("loan-close")))?
            .to_owned(),
        prev_margin_interest: items.prev_margin_interest,
        prev_loan_interest: items.prev_loan_interest,
        curr_margin_interest: items.curr_margin_interest,
        curr_loan_interest: items.curr_loan_interest,
        principal: ls_repayment
            .get("principal")
            .ok_or(Error::FieldNotExist(String::from("principal")))?
            .to_owned(),
    };

    Ok(c)
}

pub fn parse_wasm_ls_close_position(
    attributes: &Vec<EventAttribute>,
) -> Result<Option<LS_Close_Position_Type>, Error> {
    let ls_close_position = pasrse_data(attributes)?;

    if ls_close_position.contains_key("height") {
        let items = parseInterestValues(&ls_close_position)?;
        let c = LS_Close_Position_Type {
            height: ls_close_position
                .get("height")
                .ok_or(Error::FieldNotExist(String::from("height")))?
                .to_owned(),
            to: ls_close_position
                .get("to")
                .ok_or(Error::FieldNotExist(String::from("to")))?
                .to_owned(),
            change: ls_close_position
                .get("change")
                .ok_or(Error::FieldNotExist(String::from("change")))?
                .to_owned(),
            amount_amount: ls_close_position
                .get("amount-amount")
                .ok_or(Error::FieldNotExist(String::from("amount-amount")))?
                .to_owned(),
            amount_symbol: ls_close_position
                .get("amount-symbol")
                .ok_or(Error::FieldNotExist(String::from("amount-symbol")))?
                .to_owned(),
            payment_symbol: ls_close_position
                .get("payment-symbol")
                .ok_or(Error::FieldNotExist(String::from("payment-symbol")))?
                .to_owned(),
            payment_amount: ls_close_position
                .get("payment-amount")
                .ok_or(Error::FieldNotExist(String::from("payment-amount")))?
                .to_owned(),
            at: ls_close_position
                .get("at")
                .ok_or(Error::FieldNotExist(String::from("at")))?
                .to_owned(),
            loan_close: ls_close_position
                .get("loan-close")
                .ok_or(Error::FieldNotExist(String::from("loan_close")))?
                .to_owned(),
            prev_margin_interest: items.prev_margin_interest,
            prev_loan_interest: items.prev_loan_interest,
            curr_margin_interest: items.curr_margin_interest,
            curr_loan_interest: items.curr_loan_interest,
            principal: ls_close_position
                .get("principal")
                .ok_or(Error::FieldNotExist(String::from("principal")))?
                .to_owned(),
        };
        return Ok(Some(c));
    }

    Ok(None)
}

pub fn parse_wasm_ls_liquidation(
    attributes: &Vec<EventAttribute>,
) -> Result<LS_Liquidation_Type, Error> {
    let ls_liquidation = pasrse_data(attributes)?;
    let items = parseInterestValues(&ls_liquidation)?;

    let c = LS_Liquidation_Type {
        height: ls_liquidation
            .get("height")
            .ok_or(Error::FieldNotExist(String::from("height")))?
            .to_owned(),
        to: ls_liquidation
            .get("to")
            .ok_or(Error::FieldNotExist(String::from("to")))?
            .to_owned(),
        amount_symbol: ls_liquidation
            .get("amount-symbol")
            .ok_or(Error::FieldNotExist(String::from("amount-symbol")))?
            .to_owned(),
        amount_amount: ls_liquidation
            .get("amount-amount")
            .ok_or(Error::FieldNotExist(String::from("amount-amount")))?
            .to_owned(),
        payment_symbol: ls_liquidation
            .get("payment-symbol")
            .ok_or(Error::FieldNotExist(String::from("payment-symbol")))?
            .to_owned(),
        payment_amount: ls_liquidation
            .get("payment-amount")
            .ok_or(Error::FieldNotExist(String::from("payment-amount")))?
            .to_owned(),
        at: ls_liquidation
            .get("at")
            .ok_or(Error::FieldNotExist(String::from("at")))?
            .to_owned(),
        r#type: ls_liquidation
            .get("cause")
            .ok_or(Error::FieldNotExist(String::from("cause")))?
            .to_owned(),
        loan_close: ls_liquidation
            .get("loan-close")
            .ok_or(Error::FieldNotExist(String::from("loan_close")))?
            .to_owned(),
        prev_margin_interest: items.prev_margin_interest,
        prev_loan_interest: items.prev_loan_interest,
        curr_margin_interest: items.curr_margin_interest,
        curr_loan_interest: items.curr_loan_interest,
        principal: ls_liquidation
            .get("principal")
            .ok_or(Error::FieldNotExist(String::from("principal")))?
            .to_owned(),
    };

    Ok(c)
}

pub fn parse_wasm_ls_liquidation_warning(
    attributes: &Vec<EventAttribute>,
) -> Result<LS_Liquidation_Warning_Type, Error> {
    let ls_liquidation_warning = pasrse_data(attributes)?;
    let c = LS_Liquidation_Warning_Type {
        customer: ls_liquidation_warning
            .get("customer")
            .ok_or(Error::FieldNotExist(String::from("customer")))?
            .to_owned(),
        lease: ls_liquidation_warning
            .get("lease")
            .ok_or(Error::FieldNotExist(String::from("lease")))?
            .to_owned(),
        lease_asset: ls_liquidation_warning
            .get("lease-asset")
            .ok_or(Error::FieldNotExist(String::from("lease-asset")))?
            .to_owned(),
        level: ls_liquidation_warning
            .get("level")
            .ok_or(Error::FieldNotExist(String::from("level")))?
            .to_owned(),
        ltv: ls_liquidation_warning
            .get("ltv")
            .ok_or(Error::FieldNotExist(String::from("ltv")))?
            .to_owned(),
    };

    Ok(c)
}
pub fn parse_wasm_reserve_cover_loss(
    attributes: &Vec<EventAttribute>,
) -> Result<Reserve_Cover_Loss_Type, Error> {
    let reserve_cover_loss = pasrse_data(attributes)?;
    let c = Reserve_Cover_Loss_Type {
        to: reserve_cover_loss
            .get("to")
            .ok_or(Error::FieldNotExist(String::from("to")))?
            .to_owned(),
        payment_symbol: reserve_cover_loss
            .get("payment-symbol")
            .ok_or(Error::FieldNotExist(String::from("payment_symbol")))?
            .to_owned(),
        payment_amount: reserve_cover_loss
            .get("payment-amount")
            .ok_or(Error::FieldNotExist(String::from("payment_amount")))?
            .to_owned(),
    };

    Ok(c)
}

pub fn parseInterestValues(
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
    let deposit = pasrse_data(attributes)?;

    let c = LP_Deposit_Type {
        height: deposit
            .get("height")
            .ok_or(Error::FieldNotExist(String::from("height")))?
            .to_owned(),
        from: deposit
            .get("from")
            .ok_or(Error::FieldNotExist(String::from("from")))?
            .to_owned(),
        to: deposit
            .get("to")
            .ok_or(Error::FieldNotExist(String::from("to")))?
            .to_owned(),
        at: deposit
            .get("at")
            .ok_or(Error::FieldNotExist(String::from("at")))?
            .to_owned(),
        deposit_amount: deposit
            .get("deposit-amount")
            .ok_or(Error::FieldNotExist(String::from("deposit-amount")))?
            .to_owned(),
        deposit_symbol: deposit
            .get("deposit-symbol")
            .ok_or(Error::FieldNotExist(String::from("deposit-symbol")))?
            .to_owned(),
        receipts: deposit
            .get("receipts")
            .ok_or(Error::FieldNotExist(String::from("receipts")))?
            .to_owned(),
    };

    Ok(c)
}

pub fn parse_wasm_lp_withdraw(
    attributes: &Vec<EventAttribute>,
) -> Result<LP_Withdraw_Type, Error> {
    let lp_withdraw = pasrse_data(attributes)?;

    let c = LP_Withdraw_Type {
        height: lp_withdraw
            .get("height")
            .ok_or(Error::FieldNotExist(String::from("height")))?
            .to_owned(),
        from: lp_withdraw
            .get("from")
            .ok_or(Error::FieldNotExist(String::from("from")))?
            .to_owned(),
        to: lp_withdraw
            .get("to")
            .ok_or(Error::FieldNotExist(String::from("to")))?
            .to_owned(),
        at: lp_withdraw
            .get("at")
            .ok_or(Error::FieldNotExist(String::from("at")))?
            .to_owned(),
        withdraw_amount: lp_withdraw
            .get("withdraw-amount")
            .ok_or(Error::FieldNotExist(String::from("withdraw-amount")))?
            .to_owned(),
        withdraw_symbol: lp_withdraw
            .get("withdraw-symbol")
            .ok_or(Error::FieldNotExist(String::from("withdraw-symbol")))?
            .to_owned(),
        receipts: lp_withdraw
            .get("receipts")
            .ok_or(Error::FieldNotExist(String::from("receipts")))?
            .to_owned(),
        close: lp_withdraw
            .get("close")
            .ok_or(Error::FieldNotExist(String::from("close")))?
            .to_owned(),
    };

    Ok(c)
}

pub fn parse_wasm_tr_profit(
    attributes: &Vec<EventAttribute>,
) -> Result<TR_Profit_Type, Error> {
    let tr_profit = pasrse_data(attributes)?;

    let c = TR_Profit_Type {
        height: tr_profit
            .get("height")
            .ok_or(Error::FieldNotExist(String::from("height")))?
            .to_owned(),
        at: tr_profit
            .get("at")
            .ok_or(Error::FieldNotExist(String::from("at")))?
            .to_owned(),
        profit_symbol: tr_profit
            .get("profit-amount-symbol")
            .ok_or(Error::FieldNotExist(String::from("profit-symbol")))?
            .to_owned(),
        profit_amount: tr_profit
            .get("profit-amount-amount")
            .ok_or(Error::FieldNotExist(String::from("profit-amount")))?
            .to_owned(),
    };

    Ok(c)
}

pub fn parse_wasm_tr_rewards_distribution(
    attributes: &Vec<EventAttribute>,
) -> Result<TR_Rewards_Distribution_Type, Error> {
    let tr_rewards_distribution = pasrse_data(attributes)?;

    let c = TR_Rewards_Distribution_Type {
        height: tr_rewards_distribution
            .get("height")
            .ok_or(Error::FieldNotExist(String::from("height")))?
            .to_owned(),
        to: tr_rewards_distribution
            .get("to")
            .ok_or(Error::FieldNotExist(String::from("to")))?
            .to_owned(),
        at: tr_rewards_distribution
            .get("at")
            .ok_or(Error::FieldNotExist(String::from("at")))?
            .to_owned(),
        rewards_symbol: tr_rewards_distribution
            .get("rewards-symbol")
            .ok_or(Error::FieldNotExist(String::from("rewards-symbol")))?
            .to_owned(),
        rewards_amount: tr_rewards_distribution
            .get("rewards-amount")
            .ok_or(Error::FieldNotExist(String::from("rewards-amount")))?
            .to_owned(),
    };

    Ok(c)
}

fn pasrse_data(
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
            EventsType::Reserve_Cover_Loss => {
                let reserve_cover_loss =
                    parse_wasm_reserve_cover_loss(&event.attributes)?;
                wams_reserve_cover_loss::parse_and_insert(
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
        for tx_results in txs {
            if let Some(tx_results) = tx_results {
                let hash = tx_results.txhash.to_owned();
                let tx_data =
                    tx_results.tx.context("could not find Any message")?;
                parse_raw_tx(
                    app_state.clone(),
                    tx_results.txhash,
                    tx_data,
                    height,
                    time_stamp.clone(),
                    &tx_results.events,
                    &mut tx,
                )
                .await?;
                for (index, event) in tx_results.events.iter().enumerate() {
                    parse_event(
                        app_state.clone(),
                        event,
                        index,
                        time_stamp.clone(),
                        hash.to_owned(),
                        height,
                        &mut tx,
                    )
                    .await?;
                }
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
    tx_hash: String,
    tx_data: Any,
    height: i64,
    time_stamp: Timestamp,
    tx_events: &Vec<Event>,
    tx: &mut Transaction<'_, DataBase>,
) -> Result<(), Error> {
    let c = Tx::from_bytes(&tx_data.value)?;
    for (index, msg) in c.body.messages.iter().enumerate() {
        let fee = c.auth_info.fee.clone();
        let memo = c.body.memo.to_owned();
        let msg: Result<Raw_Message, anyhow::Error> = Raw_Message::from_any(
            index.try_into()?,
            msg.clone(),
            tx_hash.to_owned(),
            height,
            time_stamp.clone(),
            fee,
            memo,
            app_state.config.events_subscribe.clone(),
            tx_events,
        );

        if let Ok(msg) = msg {
            let isExists =
                app_state.database.raw_message.isExists(&msg).await?;
            if !isExists {
                app_state.database.raw_message.insert(msg, tx).await?;
            }
        }
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
    Reserve_Cover_Loss,

    LP_deposit,
    LP_Withdraw,
    TR_Profit,
    TR_Rewards_Distribution,
}

impl fmt::Display for EventsType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            EventsType::LS_Opening => write!(f, "wasm-ls-open"),
            EventsType::LS_Closing => write!(f, "wasm-ls-close"),
            EventsType::LS_Close_Position => {
                write!(f, "wasm-ls-close-position")
            },
            EventsType::LS_Repay => write!(f, "wasm-ls-repay"),
            EventsType::LS_Liquidation => write!(f, "wasm-ls-liquidation"),
            EventsType::Reserve_Cover_Loss => {
                write!(f, "wasm-reserve-cover-loss")
            },
            EventsType::LP_deposit => write!(f, "wasm-lp-deposit"),
            EventsType::LP_Withdraw => write!(f, "wasm-lp-withdraw"),
            EventsType::TR_Profit => write!(f, "wasm-tr-profit"),
            EventsType::TR_Rewards_Distribution => write!(f, "wasm-tr-rewards"),
            EventsType::LS_Liquidation_Warning => {
                write!(f, "wasm-ls-liquidation-warning")
            },
        }
    }
}

impl From<EventsType> for String {
    fn from(value: EventsType) -> Self {
        match value {
            EventsType::LS_Opening => String::from("wasm-ls-open"),
            EventsType::LS_Closing => String::from("wasm-ls-close"),
            EventsType::LS_Close_Position => {
                String::from("wasm-ls-close-position")
            },
            EventsType::LS_Repay => String::from("wasm-ls-repay"),
            EventsType::LS_Liquidation => String::from("wasm-ls-liquidation"),
            EventsType::Reserve_Cover_Loss => {
                String::from("wasm-reserve-cover-loss")
            },

            EventsType::LP_deposit => String::from("wasm-lp-deposit"),
            EventsType::LP_Withdraw => String::from("wasm-lp-withdraw"),
            EventsType::TR_Profit => String::from("wasm-tr-profit"),
            EventsType::TR_Rewards_Distribution => {
                String::from("wasm-tr-rewards")
            },
            EventsType::LS_Liquidation_Warning => {
                String::from("wasm-ls-liquidation-warning")
            },
        }
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
            "wasm-reserve-cover-loss" => Ok(EventsType::Reserve_Cover_Loss),

            "wasm-lp-deposit" => Ok(EventsType::LP_deposit),
            "wasm-lp-withdraw" => Ok(EventsType::LP_Withdraw),
            "wasm-tr-profit" => Ok(EventsType::TR_Profit),
            "wasm-tr-rewards" => Ok(EventsType::TR_Rewards_Distribution),
            "wasm-ls-liquidation-warning" => {
                Ok(EventsType::LS_Liquidation_Warning)
            },
            _ => Err(io::Error::new(
                io::ErrorKind::Other,
                "Message Type not supported",
            )),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Loan_Closing_Status {
    Reypay,
    Liquidation,
    MarketClose,
    None,
}

impl fmt::Display for Loan_Closing_Status {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Loan_Closing_Status::Reypay => write!(f, "repay"),
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
            Loan_Closing_Status::Reypay => String::from("repay"),
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
            "repay" => Ok(Loan_Closing_Status::Reypay),
            "liquidation" => Ok(Loan_Closing_Status::Liquidation),
            "market-close" => Ok(Loan_Closing_Status::MarketClose),
            "none" => Ok(Loan_Closing_Status::None),
            _ => Err(io::Error::new(
                io::ErrorKind::Other,
                "Loan_Closing_Status not supported",
            )),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Protocol_Types {
    Long,
    Short,
}

impl fmt::Display for Protocol_Types {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Protocol_Types::Short => write!(f, "short"),
            Protocol_Types::Long => write!(f, "long"),
        }
    }
}

impl From<Protocol_Types> for String {
    fn from(value: Protocol_Types) -> Self {
        match value {
            Protocol_Types::Long => String::from("long"),
            Protocol_Types::Short => String::from("short"),
        }
    }
}

impl FromStr for Protocol_Types {
    type Err = io::Error;

    fn from_str(value: &str) -> Result<Protocol_Types, Self::Err> {
        match value {
            "long" => Ok(Protocol_Types::Long),
            "short" => Ok(Protocol_Types::Short),
            _ => Err(io::Error::new(
                io::ErrorKind::Other,
                "Protocol_Types not supported",
            )),
        }
    }
}
