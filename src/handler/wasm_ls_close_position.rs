use std::str::FromStr as _;

use bigdecimal::BigDecimal;
use sqlx::Transaction;

use crate::{
    configuration::{AppState, State},
    dao::DataBase,
    error::Error,
    handler::parse_event_timestamp,
    helpers::Loan_Closing_Status,
    model::LS_Close_Position,
    types::{AmountTicker, LS_Close_Position_Type},
};

use super::ls_loan_closing as ls_loan_closing_handler;

pub async fn parse_and_insert(
    app_state: &AppState<State>,
    item: LS_Close_Position_Type,
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
    let loan_close: bool = item.loan_close.parse()?;
    let amount = BigDecimal::from_str(&item.amount_amount)?;
    let ls_close_position = LS_Close_Position {
        Tx_Hash: tx_hash,
        LS_position_height: item.height.parse()?,
        LS_position_idx: None,
        LS_contract_id: item.to.to_owned(),
        LS_change: BigDecimal::from_str(&item.change)?,
        LS_amnt: amount.to_owned(),
        LS_amnt_symbol: item.amount_symbol.to_owned(),
        LS_payment_amnt_stable,
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
        LS_amnt_stable,
        LS_payment_amnt: BigDecimal::from_str(&item.payment_amount)?,
        LS_payment_symbol: item.payment_symbol.to_owned(),
    };

    let isExists = app_state
        .database
        .ls_close_position
        .isExists(&ls_close_position)
        .await?;

    if !isExists {
        app_state
            .database
            .ls_close_position
            .insert(&ls_close_position, transaction)
            .await?;
    }

    if loan_close {
        ls_loan_closing_handler::parse_and_insert(
            app_state,
            item.to.to_owned(),
            Loan_Closing_Status::MarketClose,
            at.to_owned(),
            block,
            Some(AmountTicker {
                amount: item.change.to_owned(),
                ticker: item.payment_symbol.to_owned(),
            }),
            transaction,
        )
        .await?;
    }

    Ok(())
}
