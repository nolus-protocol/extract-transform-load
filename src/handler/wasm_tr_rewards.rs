use bigdecimal::BigDecimal;
use chrono::DateTime;
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
    index: usize,
    tx_hash: String,
    transaction: &mut Transaction<'_, DataBase>,
) -> Result<(), Error> {
    let sec: i64 = item.at.parse()?;
    let at_sec = sec / 1_000_000_000;

    let at = DateTime::from_timestamp(at_sec, 0).ok_or_else(|| {
        Error::DecodeDateTimeError(format!(
            "Wasm_TR_rewards date parse {}",
            at_sec
        ))
    })?;
    let protocol = app_state.get_protocol_by_pool_id(&item.to);

    let tr_rewards_distribution = TR_Rewards_Distribution {
        Tx_Hash: tx_hash,
        TR_Rewards_height: item.height.parse()?,
        TR_Rewards_idx: None,
        TR_Rewards_Pool_id: item.to.to_owned(),
        TR_Rewards_timestamp: at,
        TR_Rewards_amnt_stable: app_state
            .in_stabe_by_date(
                &item.rewards_symbol,
                &item.rewards_amount,
                protocol,
                &at,
            )
            .await?,
        TR_Rewards_amnt_nls: BigDecimal::from_str(&item.rewards_amount)?,
        Event_Block_Index: index.try_into()?,
    };

    let isExists = app_state
        .database
        .tr_rewards_distribution
        .isExists(&tr_rewards_distribution)
        .await?;

    if !isExists {
        app_state
            .database
            .tr_rewards_distribution
            .insert(tr_rewards_distribution, transaction)
            .await?;
    }

    Ok(())
}
