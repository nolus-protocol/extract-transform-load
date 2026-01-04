use anyhow::Context as _;
use chrono::DateTime;
use cosmrs::proto::Timestamp;
use sqlx::Transaction;

use crate::{
    configuration::{AppState, State},
    dao::DataBase,
    error::Error,
    model::LS_Slippage_Anomaly,
    types::LS_Slippage_Anomaly_Type,
};

pub async fn parse_and_insert(
    app_state: &AppState<State>,
    item: LS_Slippage_Anomaly_Type,
    time_stamp: Timestamp,
    tx_hash: String,
    transaction: &mut Transaction<'_, DataBase>,
) -> Result<(), Error> {
    let seconds = time_stamp.seconds.try_into()?;
    let nanos = time_stamp.nanos.try_into()?;
    let time_stamp = DateTime::from_timestamp(seconds, nanos)
        .context("Could not parse time stamp")?;
    let max_slipagge = item.max_slippage.parse()?;

    let ls_slippage_anomaly = LS_Slippage_Anomaly {
        Tx_Hash: Some(tx_hash),
        LS_contract_id: item.lease,
        LS_address_id: item.customer,
        LS_asset_symbol: item.lease_asset,
        LS_max_slipagge: max_slipagge,
        LS_timestamp: time_stamp,
    };

    app_state
        .database
        .ls_slippage_anomaly
        .insert_if_not_exists(ls_slippage_anomaly, transaction)
        .await?;

    Ok(())
}
