use bigdecimal::BigDecimal;
use chrono::{DateTime, NaiveDateTime, Utc};
use sqlx::Transaction;
use std::str::FromStr;

use crate::{
    configuration::{AppState, State},
    dao::DataBase,
    error::Error,
    model::LS_Close_Position,
    types::LS_Close_Position_Type,
};

pub async fn parse_and_insert(
    app_state: &AppState<State>,
    item: LS_Close_Position_Type,
    transaction: &mut Transaction<'_, DataBase>,
) -> Result<(), Error> {
    let sec: i64 = item.at.parse()?;
    let at_sec = sec / 1_000_000_000;
    let time = NaiveDateTime::from_timestamp_opt(at_sec, 0).ok_or_else(|| {
        Error::DecodeDateTimeError(format!("Wasm_LS_Close_Position date parse {}", at_sec))
    })?;

    let lease = app_state
        .database
        .ls_opening
        .get(item.to.to_owned())
        .await?;

    let protocol = match lease {
        Some(lease) => app_state.get_protocol_by_pool_id(&lease.LS_loan_pool_id),
        None => None
    };

    let at = DateTime::<Utc>::from_utc(time, Utc);
    let ls_close_position = LS_Close_Position {
        LS_position_height: item.height.parse()?,
        LS_position_idx: None,
        LS_contract_id: item.to,
        LS_symbol: item.payment_symbol.to_owned(),
        LS_change: BigDecimal::from_str(&item.change)?,
        LS_amount_amount: BigDecimal::from_str(&item.amount_amount)?,
        LS_amount_symbol: item.amount_symbol,
        LS_amnt_stable: app_state
            .in_stabe_by_date(&item.payment_symbol, &item.payment_amount, protocol, &at)
            .await?,
        LS_timestamp: at,
        LS_loan_close: item.loan_close.parse()?,
        LS_prev_margin_stable: BigDecimal::from_str(&item.prev_margin_interest)?,
        LS_prev_interest_stable: BigDecimal::from_str(&item.prev_loan_interest)?,
        LS_current_margin_stable: BigDecimal::from_str(&item.curr_margin_interest)?,
        LS_current_interest_stable: BigDecimal::from_str(&item.curr_loan_interest)?,
        LS_principal_stable: BigDecimal::from_str(&item.principal)?,
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
            .insert(ls_close_position, transaction)
            .await?;
    }

    Ok(())
}
