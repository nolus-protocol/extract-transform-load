use anyhow::Context;
use bigdecimal::BigDecimal;
use chrono::DateTime;
use sqlx::Transaction;
use std::str::FromStr;

use crate::{
    configuration::{AppState, State},
    dao::DataBase,
    error::Error,
    helpers::Loan_Closing_Status,
    model::{LS_Close_Position, LS_Loan_Closing},
    types::LS_Close_Position_Type,
};

use super::ls_loan_closing as ls_loan_closing_handler;

pub async fn parse_and_insert(
    app_state: &AppState<State>,
    item: LS_Close_Position_Type,
    tx_hash: String,
    transaction: &mut Transaction<'_, DataBase>,
) -> Result<(), Error> {
    let sec: i64 = item.at.parse()?;
    let at_sec = sec / 1_000_000_000;
    let at = DateTime::from_timestamp(at_sec, 0).ok_or_else(|| {
        Error::DecodeDateTimeError(format!(
            "Wasm_LS_Close_Position date parse {}",
            at_sec
        ))
    })?;

    let lease = app_state
        .database
        .ls_opening
        .get(item.to.to_owned())
        .await?;

    let protocol = match lease {
        Some(lease) => {
            app_state.get_protocol_by_pool_id(&lease.LS_loan_pool_id)
        },
        None => None,
    };

    let f1 = app_state.in_stabe_by_date(
        &item.payment_symbol,
        &item.payment_amount,
        protocol.to_owned(),
        &at,
    );

    let f2 = app_state.in_stabe_by_date(
        &item.amount_symbol,
        &item.amount_amount,
        protocol.to_owned(),
        &at,
    );

    let (LS_payment_amnt_stable, LS_amnt_stable) = tokio::try_join!(f1, f2)?;

    let ls_close_position = LS_Close_Position {
        Tx_Hash: Some(tx_hash),
        LS_position_height: item.height.parse()?,
        LS_position_idx: None,
        LS_contract_id: item.to.to_owned(),
        LS_change: BigDecimal::from_str(&item.change)?,
        LS_amount_amount: BigDecimal::from_str(&item.amount_amount)?,
        LS_amount_symbol: item.amount_symbol.to_owned(),
        LS_payment_amnt_stable,
        LS_timestamp: at,
        LS_loan_close: item.loan_close.parse()?,
        LS_prev_margin_stable: BigDecimal::from_str(
            &item.prev_margin_interest,
        )?,
        LS_prev_interest_stable: BigDecimal::from_str(
            &item.prev_loan_interest,
        )?,
        LS_current_margin_stable: BigDecimal::from_str(
            &item.curr_margin_interest,
        )?,
        LS_current_interest_stable: BigDecimal::from_str(
            &item.curr_loan_interest,
        )?,
        LS_principal_stable: BigDecimal::from_str(&item.principal)?,
        LS_amnt_stable: Some(LS_amnt_stable),
        LS_payment_amnt: Some(BigDecimal::from_str(&item.payment_amount)?),
        LS_payment_symbol: Some(item.payment_symbol.to_owned()),
    };

    if ls_close_position.LS_loan_close {
        let currency = app_state
            .config
            .hash_map_currencies
            .get(&item.payment_symbol)
            .context(format!(
                "could not get currency {}",
                &item.payment_symbol
            ))?;
        let power_value =
            BigDecimal::from(u64::pow(10, currency.1.try_into()?));

        let amount = &ls_close_position.LS_payment_amnt_stable / &power_value;
        let loan = app_state
            .database
            .ls_loan_closing
            .get_lease_amount(ls_close_position.LS_contract_id.to_owned())
            .await?;

        let rest = (loan - amount) * &power_value;

        let ls_loan_closing = LS_Loan_Closing {
            LS_contract_id: ls_close_position.LS_contract_id.to_owned(),
            LS_symbol: ls_close_position.LS_amount_symbol.to_owned(),
            LS_amnt_stable: rest,
            LS_timestamp: ls_close_position.LS_timestamp.to_owned(),
            Type: String::from(Loan_Closing_Status::MarketClose),
        };

        ls_loan_closing_handler::parse_and_insert(
            app_state,
            ls_loan_closing,
            transaction,
        )
        .await?;
    }

    let isExists = app_state
        .database
        .ls_close_position
        .isExists(&ls_close_position)
        .await?;

    if !isExists {
        app_state
            .database
            .ls_close_position
            .insert(ls_close_position, transaction)
            .await?;
    }

    Ok(())
}
