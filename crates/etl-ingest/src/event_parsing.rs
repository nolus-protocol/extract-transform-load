use std::collections::HashMap;

use cosmrs::proto::tendermint::abci::EventAttribute;

use etl_core::{
    error::Error,
    types::{
        Interest_values, LP_Deposit_Type, LP_Withdraw_Type,
        LS_Auto_Close_Position_Type, LS_Close_Position_Type, LS_Closing_Type,
        LS_Liquidation_Type, LS_Liquidation_Warning_Type, LS_Opening_Type,
        LS_Repayment_Type, LS_Slippage_Anomaly_Type, Reserve_Cover_Loss_Type,
        TR_Profit_Type, TR_Rewards_Distribution_Type,
    },
};

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
