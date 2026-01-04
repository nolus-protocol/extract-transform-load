use std::str::FromStr as _;

use bigdecimal::BigDecimal;
use sqlx::Transaction;

use crate::{
    configuration::{AppState, State},
    dao::DataBase,
    error::Error,
    handler::parse_event_timestamp,
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
    let at = parse_event_timestamp(&item.at)?;
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
