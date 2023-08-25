use bigdecimal::BigDecimal;
use chrono::{DateTime, NaiveDateTime, Utc};
use sqlx::Transaction;
use std::str::FromStr;

use crate::{
    configuration::{AppState, State},
    error::Error,
    model::LP_Deposit,
    types::LP_Deposit_Type, dao::DataBase,
};

pub async fn parse_and_insert(
    app_state: &AppState<State>,
    item: LP_Deposit_Type,
    transaction: &mut Transaction<'_, DataBase>,
) -> Result<(), Error> {
    let sec: i64 = item.at.parse()?;
    let at_sec = sec / 1_000_000_000;
    let time = NaiveDateTime::from_timestamp_opt(at_sec, 0).ok_or_else(|| Error::DecodeDateTimeError(format!(
        "Wasm_LP_deposit date parse {}",
        at_sec
    )))?;
    let at = DateTime::<Utc>::from_utc(time, Utc);

    let lp_deposit = LP_Deposit {
        LP_deposit_idx: None,
        LP_deposit_height: item.height.parse()?,
        LP_address_id: item.from.to_owned(),
        LP_timestamp: at,
        LP_Pool_id: item.to.to_owned(),
        LP_amnt_stable: app_state
            .in_stabe(&item.deposit_symbol, &item.deposit_amount)
            .await?,
        LP_amnt_asset: BigDecimal::from_str(&item.deposit_amount)?,
        LP_amnt_receipts: BigDecimal::from_str(&item.receipts)?,
    };

    app_state
        .database
        .lp_deposit
        .insert(lp_deposit, transaction)
        .await?;

    Ok(())
}
