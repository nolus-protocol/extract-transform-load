use std::str::FromStr as _;

use bigdecimal::BigDecimal;
use sqlx::Transaction;

use crate::{
    configuration::{AppState, State},
    dao::DataBase,
    error::Error,
    handler::parse_event_timestamp,
    helpers::Loan_Closing_Status,
    model::LS_Repayment,
    types::LS_Repayment_Type,
};

use super::ls_loan_closing as ls_loan_closing_handler;

pub async fn parse_and_insert(
    app_state: &AppState<State>,
    item: LS_Repayment_Type,
    tx_hash: String,
    block: i64,
    transaction: &mut Transaction<'_, DataBase>,
) -> Result<(), Error> {
    let at = parse_event_timestamp(&item.at)?;
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

    let loan_close: bool = item.loan_close.parse()?;

    let ls_repay = LS_Repayment {
        Tx_Hash: tx_hash,
        LS_repayment_height: item.height.parse()?,
        LS_repayment_idx: None,
        LS_contract_id: item.to.to_owned(),
        LS_payment_symbol: item.payment_symbol.to_owned(),
        LS_payment_amnt: BigDecimal::from_str(&item.payment_amount)?,
        LS_payment_amnt_stable: app_state
            .in_stabe_by_date(
                &item.payment_symbol,
                &item.payment_amount,
                protocol.to_owned(),
                &at,
            )
            .await?,
        LS_timestamp: at,
        LS_loan_close: loan_close,
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
    };

    let isExists = app_state.database.ls_repayment.isExists(&ls_repay).await?;

    if !isExists {
        app_state
            .database
            .ls_repayment
            .insert(ls_repay, transaction)
            .await?;
    }

    if loan_close {
        ls_loan_closing_handler::parse_and_insert(
            app_state,
            item.to.to_owned(),
            Loan_Closing_Status::Reypay,
            at.to_owned(),
            block,
            None,
            transaction,
        )
        .await?;
    }

    Ok(())
}
