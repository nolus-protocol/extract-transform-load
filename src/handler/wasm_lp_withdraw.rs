use bigdecimal::BigDecimal;
use chrono::{DateTime, NaiveDateTime, Utc};
use sqlx::Transaction;
use std::str::FromStr;

use crate::{
    configuration::{AppState, State},
    error::Error,
    model::LP_Withdraw,
    types::LP_Withdraw_Type, dao::DataBase,
};

pub async fn parse_and_insert(
    app_state: &AppState<State>,
    item: LP_Withdraw_Type,
    transaction: &mut Transaction<'_, DataBase>,
) -> Result<(), Error> {
    let sec: i64 = item.at.parse()?;
    let at_sec = sec / 1_000_000_000;
    let at = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(at_sec, 0), Utc);

    let lp_withdraw = LP_Withdraw {
        LP_withdraw_height: item.height.parse()?,
        LP_withdraw_idx: None,
        LP_address_id: item.to.to_owned(),
        LP_timestamp: at,
        LP_Pool_id: item.from,
        LP_amnt_stable: app_state
            .in_stabe(&item.withdraw_symbol, &item.withdraw_amount)
            .await?,
        LP_amnt_asset: BigDecimal::from_str(&item.withdraw_amount)?,
        LP_amnt_receipts: BigDecimal::from_str(&item.receipts)?,
        LP_deposit_close: item.close.parse()?,
    };

    app_state
        .database
        .lp_withdraw
        .insert(lp_withdraw, transaction)
        .await?;

    Ok(())
}
