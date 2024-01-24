use crate::configuration::{AppState, State};
use crate::dao::DataBase;
use crate::handler::{
    wasm_lp_deposit, wasm_lp_withdraw, wasm_ls_close, wasm_ls_close_position, wasm_ls_liquidation,
    wasm_ls_open, wasm_ls_repay, wasm_tr_profit, wasm_tr_rewards,
};
use crate::model::Block;
use crate::{
    error::Error,
    types::{
        Attributes, LP_Deposit_Type, LP_Withdraw_Type, LS_Closing_Type, LS_Liquidation_Type,
        LS_Opening_Type, LS_Repayment_Type, TR_Profit_Type, TR_Rewards_Distribution_Type,
    },
};

use crate::types::{BlockBody, EventData, LS_Close_Position_Type};
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
                parser = parser.replace(format!("${}", index).as_str(), &parsed_string);
            }
            Formatter::Number(n) => {
                parser = parser.replace(format!("${}", index).as_str(), &n.to_string());
            }
            Formatter::NumberU64(n) => {
                parser = parser.replace(format!("${}", index).as_str(), &n.to_string());
            }
            Formatter::Str(n) => {
                parser = parser.replace(format!("${}", index).as_str(), &n.to_string());
            }
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

pub fn parse_wasm_ls_open(attributes: &Vec<Attributes>) -> Result<LS_Opening_Type, Error> {
    let ls_open = pasrse_data(attributes)?;
    let c = LS_Opening_Type {
        id: ls_open
            .get("id")
            .ok_or(Error::FieldNotExist(String::from("id")))?
            .to_string(),
        customer: ls_open
            .get("customer")
            .ok_or(Error::FieldNotExist(String::from("customer")))?
            .to_string(),
        currency: ls_open
            .get("currency")
            .ok_or(Error::FieldNotExist(String::from("currency")))?
            .to_string(),
        air: ls_open
            .get("air")
            .ok_or(Error::FieldNotExist(String::from("air")))?
            .to_string(),
        at: ls_open
            .get("at")
            .ok_or(Error::FieldNotExist(String::from("at")))?
            .to_string(),
        loan_pool_id: ls_open
            .get("loan-pool-id")
            .ok_or(Error::FieldNotExist(String::from("loan-pool-id")))?
            .to_string(),
        loan_amount: ls_open
            .get("loan-amount")
            .ok_or(Error::FieldNotExist(String::from("loan-amount")))?
            .to_string(),
        downpayment_amount: ls_open
            .get("downpayment-amount")
            .ok_or(Error::FieldNotExist(String::from("downpayment-amount")))?
            .to_string(),
        downpayment_symbol: ls_open
            .get("downpayment-symbol")
            .ok_or(Error::FieldNotExist(String::from("downpayment-symbol")))?
            .to_string(),
    };

    Ok(c)
}

pub fn parse_wasm_ls_close(attributes: &Vec<Attributes>) -> Result<LS_Closing_Type, Error> {
    let ls_close = pasrse_data(attributes)?;
    let c = LS_Closing_Type {
        id: ls_close
            .get("id")
            .ok_or(Error::FieldNotExist(String::from("id")))?
            .to_string(),
        at: ls_close
            .get("at")
            .ok_or(Error::FieldNotExist(String::from("at")))?
            .to_string(),
    };

    Ok(c)
}

pub fn parse_wasm_ls_repayment(attributes: &Vec<Attributes>) -> Result<LS_Repayment_Type, Error> {
    let ls_repayment = pasrse_data(attributes)?;

    let c = LS_Repayment_Type {
        height: ls_repayment
            .get("height")
            .ok_or(Error::FieldNotExist(String::from("height")))?
            .to_string(),
        to: ls_repayment
            .get("to")
            .ok_or(Error::FieldNotExist(String::from("to")))?
            .to_string(),
        payment_symbol: ls_repayment
            .get("payment-symbol")
            .ok_or(Error::FieldNotExist(String::from("payment-symbol")))?
            .to_string(),
        payment_amount: ls_repayment
            .get("payment-amount")
            .ok_or(Error::FieldNotExist(String::from("payment-amount")))?
            .to_string(),
        at: ls_repayment
            .get("at")
            .ok_or(Error::FieldNotExist(String::from("at")))?
            .to_string(),
        loan_close: ls_repayment
            .get("loan-close")
            .ok_or(Error::FieldNotExist(String::from("loan-close")))?
            .to_string(),
        prev_margin_interest: ls_repayment
            .get("prev-margin-interest")
            .ok_or(Error::FieldNotExist(String::from("prev-margin-interest")))?
            .to_string(),
        prev_loan_interest: ls_repayment
            .get("prev-loan-interest")
            .ok_or(Error::FieldNotExist(String::from("prev-loan-interest")))?
            .to_string(),
        curr_margin_interest: ls_repayment
            .get("curr-margin-interest")
            .ok_or(Error::FieldNotExist(String::from("curr-margin-interest")))?
            .to_string(),
        curr_loan_interest: ls_repayment
            .get("curr-loan-interest")
            .ok_or(Error::FieldNotExist(String::from("curr-loan-interest")))?
            .to_string(),
        principal: ls_repayment
            .get("principal")
            .ok_or(Error::FieldNotExist(String::from("principal")))?
            .to_string(),
    };

    Ok(c)
}

pub fn parse_wasm_ls_close_position(
    attributes: &Vec<Attributes>,
) -> Result<Option<LS_Close_Position_Type>, Error> {
    let ls_close_position = pasrse_data(attributes)?;
    if ls_close_position.contains_key("height") {
        let c = LS_Close_Position_Type {
            height: ls_close_position
                .get("height")
                .ok_or(Error::FieldNotExist(String::from("height")))?
                .to_string(),
            to: ls_close_position
                .get("to")
                .ok_or(Error::FieldNotExist(String::from("to")))?
                .to_string(),
            change: ls_close_position
                .get("change")
                .ok_or(Error::FieldNotExist(String::from("change")))?
                .to_string(),
            amount_amount: ls_close_position
                .get("amount-amount")
                .ok_or(Error::FieldNotExist(String::from("amount-amount")))?
                .to_string(),
            amount_symbol: ls_close_position
                .get("amount-symbol")
                .ok_or(Error::FieldNotExist(String::from("amount-symbol")))?
                .to_string(),
            payment_symbol: ls_close_position
                .get("payment-symbol")
                .ok_or(Error::FieldNotExist(String::from("payment-symbol")))?
                .to_string(),
            payment_amount: ls_close_position
                .get("payment-amount")
                .ok_or(Error::FieldNotExist(String::from("payment-amount")))?
                .to_string(),
            at: ls_close_position
                .get("at")
                .ok_or(Error::FieldNotExist(String::from("at")))?
                .to_string(),
            loan_close: ls_close_position
                .get("loan-close")
                .ok_or(Error::FieldNotExist(String::from("loan_close")))?
                .to_string(),
            prev_margin_interest: ls_close_position
                .get("prev-margin-interest")
                .ok_or(Error::FieldNotExist(String::from("prev-margin-interest")))?
                .to_string(),
            prev_loan_interest: ls_close_position
                .get("prev-loan-interest")
                .ok_or(Error::FieldNotExist(String::from("prev-loan-interest")))?
                .to_string(),
            curr_margin_interest: ls_close_position
                .get("curr-margin-interest")
                .ok_or(Error::FieldNotExist(String::from("curr-margin-interest")))?
                .to_string(),
            curr_loan_interest: ls_close_position
                .get("curr-loan-interest")
                .ok_or(Error::FieldNotExist(String::from("curr-loan-interest")))?
                .to_string(),
            principal: ls_close_position
                .get("principal")
                .ok_or(Error::FieldNotExist(String::from("principal")))?
                .to_string(),
        };
        return Ok(Some(c));
    }

    Ok(None)
}

pub fn parse_wasm_ls_liquidation(
    attributes: &Vec<Attributes>,
) -> Result<LS_Liquidation_Type, Error> {
    let ls_liquidation = pasrse_data(attributes)?;
    let c = LS_Liquidation_Type {
        height: ls_liquidation
            .get("height")
            .ok_or(Error::FieldNotExist(String::from("height")))?
            .to_string(),
        to: ls_liquidation
            .get("to")
            .ok_or(Error::FieldNotExist(String::from("to")))?
            .to_string(),
        liquidation_symbol: ls_liquidation
            .get("amount-symbol")
            .ok_or(Error::FieldNotExist(String::from("amount-symbol")))?
            .to_string(),
        liquidation_amount: ls_liquidation
            .get("amount-amount")
            .ok_or(Error::FieldNotExist(String::from("amount-amount")))?
            .to_string(),
        at: ls_liquidation
            .get("at")
            .ok_or(Error::FieldNotExist(String::from("at")))?
            .to_string(),
        r#type: ls_liquidation
            .get("cause")
            .ok_or(Error::FieldNotExist(String::from("cause")))?
            .to_string(),
        prev_margin_interest: ls_liquidation
            .get("prev-margin-interest")
            .ok_or(Error::FieldNotExist(String::from("prev-margin-interest")))?
            .to_string(),
        prev_loan_interest: ls_liquidation
            .get("prev-loan-interest")
            .ok_or(Error::FieldNotExist(String::from("prev-loan-interest")))?
            .to_string(),
        curr_margin_interest: ls_liquidation
            .get("curr-margin-interest")
            .ok_or(Error::FieldNotExist(String::from("curr-margin-interest")))?
            .to_string(),
        curr_loan_interest: ls_liquidation
            .get("curr-loan-interest")
            .ok_or(Error::FieldNotExist(String::from("curr-loan-interest")))?
            .to_string(),
        principal: ls_liquidation
            .get("principal")
            .ok_or(Error::FieldNotExist(String::from("principal")))?
            .to_string(),
    };

    Ok(c)
}

pub fn parse_wasm_lp_deposit(attributes: &Vec<Attributes>) -> Result<LP_Deposit_Type, Error> {
    let deposit = pasrse_data(attributes)?;

    let c = LP_Deposit_Type {
        height: deposit
            .get("height")
            .ok_or(Error::FieldNotExist(String::from("height")))?
            .to_string(),
        from: deposit
            .get("from")
            .ok_or(Error::FieldNotExist(String::from("from")))?
            .to_string(),
        to: deposit
            .get("to")
            .ok_or(Error::FieldNotExist(String::from("to")))?
            .to_string(),
        at: deposit
            .get("at")
            .ok_or(Error::FieldNotExist(String::from("at")))?
            .to_string(),
        deposit_amount: deposit
            .get("deposit-amount")
            .ok_or(Error::FieldNotExist(String::from("deposit-amount")))?
            .to_string(),
        deposit_symbol: deposit
            .get("deposit-symbol")
            .ok_or(Error::FieldNotExist(String::from("deposit-symbol")))?
            .to_string(),
        receipts: deposit
            .get("receipts")
            .ok_or(Error::FieldNotExist(String::from("receipts")))?
            .to_string(),
    };

    Ok(c)
}

pub fn parse_wasm_lp_withdraw(attributes: &Vec<Attributes>) -> Result<LP_Withdraw_Type, Error> {
    let lp_withdraw = pasrse_data(attributes)?;

    let c = LP_Withdraw_Type {
        height: lp_withdraw
            .get("height")
            .ok_or(Error::FieldNotExist(String::from("height")))?
            .to_string(),
        from: lp_withdraw
            .get("from")
            .ok_or(Error::FieldNotExist(String::from("from")))?
            .to_string(),
        to: lp_withdraw
            .get("to")
            .ok_or(Error::FieldNotExist(String::from("to")))?
            .to_string(),
        at: lp_withdraw
            .get("at")
            .ok_or(Error::FieldNotExist(String::from("at")))?
            .to_string(),
        withdraw_amount: lp_withdraw
            .get("withdraw-amount")
            .ok_or(Error::FieldNotExist(String::from("withdraw-amount")))?
            .to_string(),
        withdraw_symbol: lp_withdraw
            .get("withdraw-symbol")
            .ok_or(Error::FieldNotExist(String::from("withdraw-symbol")))?
            .to_string(),
        receipts: lp_withdraw
            .get("receipts")
            .ok_or(Error::FieldNotExist(String::from("receipts")))?
            .to_string(),
        close: lp_withdraw
            .get("close")
            .ok_or(Error::FieldNotExist(String::from("close")))?
            .to_string(),
    };

    Ok(c)
}

pub fn parse_wasm_tr_profit(attributes: &Vec<Attributes>) -> Result<TR_Profit_Type, Error> {
    let tr_profit = pasrse_data(attributes)?;

    let c = TR_Profit_Type {
        height: tr_profit
            .get("height")
            .ok_or(Error::FieldNotExist(String::from("height")))?
            .to_string(),
        at: tr_profit
            .get("at")
            .ok_or(Error::FieldNotExist(String::from("at")))?
            .to_string(),
        profit_symbol: tr_profit
            .get("profit-amount-symbol")
            .ok_or(Error::FieldNotExist(String::from("profit-symbol")))?
            .to_string(),
        profit_amount: tr_profit
            .get("profit-amount-amount")
            .ok_or(Error::FieldNotExist(String::from("profit-amount")))?
            .to_string(),
    };

    Ok(c)
}

pub fn parse_wasm_tr_rewards_distribution(
    attributes: &Vec<Attributes>,
) -> Result<TR_Rewards_Distribution_Type, Error> {
    let tr_rewards_distribution = pasrse_data(attributes)?;

    let c = TR_Rewards_Distribution_Type {
        height: tr_rewards_distribution
            .get("height")
            .ok_or(Error::FieldNotExist(String::from("height")))?
            .to_string(),
        to: tr_rewards_distribution
            .get("to")
            .ok_or(Error::FieldNotExist(String::from("to")))?
            .to_string(),
        at: tr_rewards_distribution
            .get("at")
            .ok_or(Error::FieldNotExist(String::from("at")))?
            .to_string(),
        rewards_symbol: tr_rewards_distribution
            .get("rewards-symbol")
            .ok_or(Error::FieldNotExist(String::from("rewards-symbol")))?
            .to_string(),
        rewards_amount: tr_rewards_distribution
            .get("rewards-amount")
            .ok_or(Error::FieldNotExist(String::from("rewards-amount")))?
            .to_string(),
    };

    Ok(c)
}

fn pasrse_data(attributes: &Vec<Attributes>) -> Result<HashMap<String, String>, Error> {
    let mut data: HashMap<String, String> = HashMap::new();
    for attribute in attributes {
        let value = attribute.value.to_owned().unwrap_or(String::from(""));
        let key = attribute.key.to_owned();
        data.insert(key, value);
    }

    Ok(data)
}

pub async fn parse_event(
    app_state: AppState<State>,
    event: &EventData,
    index: usize,
    tx: &mut Transaction<'_, DataBase>,
) -> Result<(), Error> {
    if let Ok(t) = EventsType::from_str(&event.r#type) {
        match t {
            EventsType::LS_Opening => {
                let wasm_ls_opening = parse_wasm_ls_open(&event.attributes)?;
                wasm_ls_open::parse_and_insert(&app_state, wasm_ls_opening, tx).await?;
            }
            EventsType::LS_Closing => {
                let wasm_ls_closing = parse_wasm_ls_close(&event.attributes)?;
                wasm_ls_close::parse_and_insert(&app_state, wasm_ls_closing, tx).await?;
            }
            EventsType::LS_Close_Position => {
                let wasm_ls_close_position = parse_wasm_ls_close_position(&event.attributes)?;
                if let Some(item) = wasm_ls_close_position {
                    wasm_ls_close_position::parse_and_insert(&app_state, item, tx).await?;
                }
            }
            EventsType::LS_Repay => {
                let wasm_ls_repay = parse_wasm_ls_repayment(&event.attributes)?;
                wasm_ls_repay::parse_and_insert(&app_state, wasm_ls_repay, tx).await?;
            }
            EventsType::LS_Liquidation => {
                let wasm_ls_liquidation = parse_wasm_ls_liquidation(&event.attributes)?;
                wasm_ls_liquidation::parse_and_insert(&app_state, wasm_ls_liquidation, tx).await?;
            }
            EventsType::LP_deposit => {
                let wasm_lp_deposit = parse_wasm_lp_deposit(&event.attributes)?;
                wasm_lp_deposit::parse_and_insert(&app_state, wasm_lp_deposit, tx).await?;
            }
            EventsType::LP_Withdraw => {
                let wasm_lp_withdraw = parse_wasm_lp_withdraw(&event.attributes)?;
                wasm_lp_withdraw::parse_and_insert(&app_state, wasm_lp_withdraw, tx).await?;
            }
            EventsType::TR_Profit => {
                let wasm_tr_profit = parse_wasm_tr_profit(&event.attributes)?;
                wasm_tr_profit::parse_and_insert(&app_state, wasm_tr_profit, tx).await?;
            }
            EventsType::TR_Rewards_Distribution => {
                let wasm_tr_rewards_distribution =
                    parse_wasm_tr_rewards_distribution(&event.attributes)?;
                wasm_tr_rewards::parse_and_insert(&app_state, wasm_tr_rewards_distribution, index, tx)
                    .await?;
            }
        }
    }
    Ok(())
}

pub async fn insert_block(app_state: AppState<State>, data: BlockBody) -> Result<bool, Error> {
    let height = data.result.height.parse::<i64>()?;
    let block = app_state.database.block.get_one(height).await?;

    if block.is_none() {
        let mut tx = app_state.database.pool.begin().await?;
        if let Some(items) = data.result.txs_results {
            for tx_results in items {
                if let Some(events) = tx_results.events {
                    for (index, event) in events.iter().enumerate() {
                        parse_event(app_state.clone(), event, index, &mut tx).await?;
                    }
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

#[derive(Debug)]
pub enum MessageType {
    NewEvent,
}

impl fmt::Display for MessageType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            MessageType::NewEvent => write!(f, "tendermint/event/NewBlock"),
        }
    }
}

impl From<MessageType> for String {
    fn from(value: MessageType) -> Self {
        match value {
            MessageType::NewEvent => String::from("tendermint/event/NewBlock"),
        }
    }
}

impl FromStr for MessageType {
    type Err = io::Error;

    fn from_str(value: &str) -> Result<MessageType, Self::Err> {
        match value {
            "tendermint/event/NewBlock" => Ok(MessageType::NewEvent),
            _ => Err(io::Error::new(
                io::ErrorKind::Other,
                "Message Type not supported",
            )),
        }
    }
}

#[derive(Debug)]
pub enum EventsType {
    LS_Opening,
    LS_Closing,
    LS_Close_Position,
    LS_Repay,
    LS_Liquidation,
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
            EventsType::LS_Close_Position => write!(f, "wasm-ls-close-position"),
            EventsType::LS_Repay => write!(f, "wasm-ls-repay"),
            EventsType::LS_Liquidation => write!(f, "wasm-ls-liquidation"),
            EventsType::LP_deposit => write!(f, "wasm-lp-deposit"),
            EventsType::LP_Withdraw => write!(f, "wasm-lp-withdraw"),
            EventsType::TR_Profit => write!(f, "wasm-tr-profit"),
            EventsType::TR_Rewards_Distribution => write!(f, "wasm-tr-rewards"),
        }
    }
}

impl From<EventsType> for String {
    fn from(value: EventsType) -> Self {
        match value {
            EventsType::LS_Opening => String::from("wasm-ls-open"),
            EventsType::LS_Closing => String::from("wasm-ls-close"),
            EventsType::LS_Close_Position => String::from("wasm-ls-close-position"),
            EventsType::LS_Repay => String::from("wasm-ls-repay"),
            EventsType::LS_Liquidation => String::from("wasm-ls-liquidation"),
            EventsType::LP_deposit => String::from("wasm-lp-deposit"),
            EventsType::LP_Withdraw => String::from("wasm-lp-withdraw"),
            EventsType::TR_Profit => String::from("wasm-tr-profit"),
            EventsType::TR_Rewards_Distribution => String::from("wasm-tr-rewards"),
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
            "wasm-lp-deposit" => Ok(EventsType::LP_deposit),
            "wasm-lp-withdraw" => Ok(EventsType::LP_Withdraw),
            "wasm-tr-profit" => Ok(EventsType::TR_Profit),
            "wasm-tr-rewards" => Ok(EventsType::TR_Rewards_Distribution),
            _ => Err(io::Error::new(
                io::ErrorKind::Other,
                "Message Type not supported",
            )),
        }
    }
}
