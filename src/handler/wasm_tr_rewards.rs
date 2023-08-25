use bigdecimal::BigDecimal;
use chrono::{DateTime, NaiveDateTime, Utc};
use sqlx::Transaction;
use std::str::FromStr;

use crate::{
    configuration::{AppState, State},
    dao::DataBase,
    error::Error,
    model::TR_Rewards_Distribution,
    types::TR_Rewards_Distribution_Type,
};

pub async fn parse_and_insert(
    app_state: &AppState<State>,
    item: TR_Rewards_Distribution_Type,
    transaction: &mut Transaction<'_, DataBase>,
) -> Result<(), Error> {
    let sec: i64 = item.at.parse()?;
    let at_sec = sec / 1_000_000_000;

    let time = NaiveDateTime::from_timestamp_opt(at_sec, 0).ok_or_else(|| {
        Error::DecodeDateTimeError(format!("Wasm_TR_rewards date parse {}", at_sec))
    })?;
    let at = DateTime::<Utc>::from_utc(time, Utc);

    let tr_reward_distribution = TR_Rewards_Distribution {
        TR_Rewards_height: item.height.parse()?,
        TR_Rewards_idx: None,
        TR_Rewards_Pool_id: item.to.to_owned(),
        TR_Rewards_timestamp: at,
        TR_Rewards_amnt_stable: app_state
            .in_stabe(&item.rewards_symbol, &item.rewards_amount)
            .await?,
        TR_Rewards_amnt_nls: BigDecimal::from_str(&item.rewards_amount)?,
    };

    app_state
        .database
        .tr_rewards_distribution
        .insert(tr_reward_distribution, transaction)
        .await?;

    Ok(())
}
