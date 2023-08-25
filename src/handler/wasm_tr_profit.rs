use bigdecimal::BigDecimal;
use chrono::{DateTime, NaiveDateTime, Utc};
use sqlx::Transaction;
use std::str::FromStr;

use crate::{
    configuration::{AppState, State},
    error::Error,
    model::TR_Profit,
    types::TR_Profit_Type, dao::DataBase,
};

pub async fn parse_and_insert(
    app_state: &AppState<State>,
    item: TR_Profit_Type,
    transaction: &mut Transaction<'_, DataBase>,
) -> Result<(), Error> {
    let sec: i64 = item.at.parse()?;
    let at_sec = sec / 1_000_000_000;
    let time = NaiveDateTime::from_timestamp_opt(at_sec, 0).ok_or_else(|| Error::DecodeDateTimeError(format!(
        "Wasm_TR_profit date parse {}",
        at_sec
    )))?;
    let at = DateTime::<Utc>::from_utc(time, Utc);

    let tr_profit = TR_Profit {
        TR_Profit_height: item.height.parse()?,
        TR_Profit_idx: None,
        TR_Profit_timestamp: at,
        TR_Profit_amnt_stable: app_state
            .in_stabe(&item.profit_symbol, &item.profit_amount)
            .await?,
        TR_Profit_amnt_nls: BigDecimal::from_str(&item.profit_amount)?,
    };

    app_state
        .database
        .tr_profit
        .insert(tr_profit, transaction)
        .await?;

    Ok(())
}
