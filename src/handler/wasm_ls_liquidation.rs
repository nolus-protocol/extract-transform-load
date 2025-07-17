use std::str::FromStr as _;

use bigdecimal::BigDecimal;
use chrono::DateTime;
use sqlx::Transaction;

use crate::{
    configuration::{AppState, State},
    dao::DataBase,
    error::Error,
    handler::send_push::send,
    helpers::Loan_Closing_Status,
    model::{LS_Liquidation, LS_Liquidation_Type as LS_Liquidation_Data},
    types::{LS_Liquidation_Type, PushData, PUSH_TYPES},
};

use super::ls_loan_closing as ls_loan_closing_handler;

pub async fn parse_and_insert(
    app_state: &AppState<State>,
    item: LS_Liquidation_Type,
    tx_hash: String,
    block: i64,
    transaction: &mut Transaction<'_, DataBase>,
) -> Result<(), Error> {
    let contract = item.to.to_owned();
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
    let amount = BigDecimal::from_str(&item.amount_amount)?;
    let ls_liquidation = LS_Liquidation {
        Tx_Hash: tx_hash,
        LS_liquidation_height: item.height.parse()?,
        LS_liquidation_idx: None,
        LS_contract_id: item.to.to_owned(),
        LS_amnt_symbol: item.amount_symbol.to_owned(),
        LS_amnt_stable,

        LS_amnt: amount.to_owned(),
        LS_payment_symbol: item.payment_symbol.to_owned(),
        LS_payment_amnt: BigDecimal::from_str(&item.payment_amount)?,
        LS_payment_amnt_stable,
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
        LS_loan_close: loan_close,
    };

    let status = ls_liquidation.LS_transaction_type.to_owned();
    let isExists = app_state
        .database
        .ls_liquidation
        .isExists(&ls_liquidation)
        .await?;

    if !isExists {
        app_state
            .database
            .ls_liquidation
            .insert(&ls_liquidation, transaction)
            .await?;
    }

    if loan_close {
        ls_loan_closing_handler::parse_and_insert(
            app_state,
            item.to.to_owned(),
            Loan_Closing_Status::Liquidation,
            at.to_owned(),
            block,
            transaction,
        )
        .await?;
    }

    let push_data = match LS_Liquidation_Data::from(status.as_str()) {
        LS_Liquidation_Data::OverdueInterest => PushData {
            r#type: PUSH_TYPES::PartiallyLiquidated.to_string(),
            body: format!(r#"{{"position": "{}"}}"#, contract),
        },
        LS_Liquidation_Data::HighLiability => PushData {
            r#type: PUSH_TYPES::FullyLiquidated.to_string(),
            body: format!(r#"{{"position": "{}"}}"#, contract),
        },
        LS_Liquidation_Data::Unsupported => PushData {
            r#type: PUSH_TYPES::Unsupported.to_string(),
            body: format!(r#"{{}}"#,),
        },
    };

    send(app_state.clone(), contract, push_data).await?;

    Ok(())
}
