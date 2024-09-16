use bigdecimal::BigDecimal;
use chrono::DateTime;
use sqlx::Transaction;
use std::str::FromStr;

use super::ls_loan_closing as ls_loan_closing_handler;
use crate::{
    configuration::{AppState, State},
    dao::DataBase,
    error::Error,
    helpers::Loan_Closing_Status,
    model::{LS_Liquidation, LS_Loan_Closing},
    types::LS_Liquidation_Type,
};

pub async fn parse_and_insert(
    app_state: &AppState<State>,
    item: LS_Liquidation_Type,
    tx_hash: String,
    transaction: &mut Transaction<'_, DataBase>,
) -> Result<(), Error> {
    let sec: i64 = item.at.parse()?;
    let at_sec = sec / 1_000_000_000;
    let at = DateTime::from_timestamp(at_sec, 0).ok_or_else(|| {
        Error::DecodeDateTimeError(format!(
            "Wasm_LS_Liquidation date parse {}",
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

    let loan_close = item.loan_close.parse()?;
    let f1 = app_state.in_stabe_by_date(
        &item.amount_symbol,
        &item.amount_amount,
        protocol.to_owned(),
        &at,
    );

    let f2 = app_state.in_stabe_by_date(
        &item.payment_symbol,
        &item.payment_amount,
        protocol.to_owned(),
        &at,
    );

    let (LS_amnt_stable, LS_payment_amnt_stable) = tokio::try_join!(f1, f2)?;

    let ls_liquidation = LS_Liquidation {
        Tx_Hash: Some(tx_hash),
        LS_liquidation_height: item.height.parse()?,
        LS_liquidation_idx: None,
        LS_contract_id: item.to,
        LS_symbol: item.amount_symbol.to_owned(),
        LS_amnt_stable,

        LS_amnt: Some(BigDecimal::from_str(&item.amount_amount)?),
        LS_payment_symbol: Some(item.payment_symbol.to_owned()),
        LS_payment_amnt: Some(BigDecimal::from_str(&item.payment_amount)?),
        LS_payment_amnt_stable: Some(LS_payment_amnt_stable),
        LS_timestamp: at,
        LS_transaction_type: item.r#type,
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
        LS_loan_close: Some(loan_close),
    };

    if loan_close {
        let ls_loan_closing = LS_Loan_Closing {
            LS_contract_id: ls_liquidation.LS_contract_id.to_owned(),
            LS_symbol: ls_liquidation.LS_symbol.to_owned(),
            LS_amnt: BigDecimal::from_str(&item.amount_amount)?,
            LS_amnt_stable: ls_liquidation.LS_amnt_stable.to_owned(),
            LS_timestamp: ls_liquidation.LS_timestamp.to_owned(),
            Type: String::from(Loan_Closing_Status::Liquidation),
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
        .ls_liquidation
        .isExists(&ls_liquidation)
        .await?;

    if !isExists {
        app_state
            .database
            .ls_liquidation
            .insert(ls_liquidation, transaction)
            .await?;
    }

    Ok(())
}
