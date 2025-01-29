use anyhow::Context as _;
use chrono::DateTime;
use cosmrs::proto::Timestamp;
use sqlx::Transaction;

use crate::{
    configuration::{AppState, State},
    dao::DataBase,
    error::Error,
    model::LS_Liquidation_Warning,
    types::LS_Liquidation_Warning_Type,
};

pub async fn parse_and_insert(
    app_state: &AppState<State>,
    item: LS_Liquidation_Warning_Type,
    time_stamp: Timestamp,
    tx_hash: String,
    transaction: &mut Transaction<'_, DataBase>,
) -> Result<(), Error> {
    let seconds = time_stamp.seconds.try_into()?;
    let nanos = time_stamp.nanos.try_into()?;
    let time_stamp = DateTime::from_timestamp(seconds, nanos)
        .context("Could not parse time stamp")?;
    let level = item.level.parse()?;
    let ltv = item.ltv.parse()?;

    let ls_liquidation_warning = LS_Liquidation_Warning {
        Tx_Hash: Some(tx_hash),
        LS_contract_id: item.lease,
        LS_address_id: item.customer,
        LS_asset_symbol: item.lease_asset,
        LS_level: level,
        LS_ltv: ltv,
        LS_timestamp: time_stamp,
    };

    let isExists = app_state
        .database
        .ls_liquidation_warning
        .isExists(&ls_liquidation_warning)
        .await?;

    if !isExists {
        app_state
            .database
            .ls_liquidation_warning
            .insert(ls_liquidation_warning, transaction)
            .await?;
    }

    Ok(())
}
