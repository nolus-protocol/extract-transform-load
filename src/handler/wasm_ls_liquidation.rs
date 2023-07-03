use bigdecimal::BigDecimal;
use chrono::{DateTime, NaiveDateTime, Utc};
use sqlx::Transaction;
use std::str::FromStr;

use crate::{
    configuration::{AppState, State},
    error::Error,
    model::LS_Liquidation,
    types::LS_Liquidation_Type, dao::DataBase,
};

pub async fn parse_and_insert(
    app_state: &AppState<State>,
    item: LS_Liquidation_Type,
    transaction: &mut Transaction<'_, DataBase>,
) -> Result<(), Error> {
    let sec: i64 = item.at.parse()?;
    let at_sec = sec / 1_000_000_000;
    let at = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(at_sec, 0), Utc);
    let ls_liquidation = LS_Liquidation {
        LS_liquidation_height: item.height.parse()?,
        LS_liquidation_idx: None,
        LS_contract_id: item.to,
        LS_symbol: item.liquidation_symbol.to_owned(),
        LS_amnt_stable: app_state
            .in_stabe(&item.liquidation_symbol, &item.liquidation_amount)
            .await?,
        LS_timestamp: at,
        LS_transaction_type: item.r#type,
        LS_prev_margin_stable: BigDecimal::from_str(&item.prev_margin_interest)?,
        LS_prev_interest_stable: BigDecimal::from_str(&item.prev_loan_interest)?,
        LS_current_margin_stable: BigDecimal::from_str(&item.curr_margin_interest)?,
        LS_current_interest_stable: BigDecimal::from_str(&item.curr_loan_interest)?,
        LS_principal_stable: BigDecimal::from_str(&item.principal)?,
    };

    app_state
        .database
        .ls_liquidation
        .insert(ls_liquidation, transaction)
        .await?;

    Ok(())
}
