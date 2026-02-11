use std::str::FromStr as _;

use anyhow::Context as _;
use bigdecimal::BigDecimal;
use chrono::DateTime;
use cosmrs::proto::Timestamp;
use sqlx::Transaction;

use etl_core::{
    configuration::{AppState, State},
    dao::DataBase,
    error::Error,
    model::Reserve_Cover_Loss,
    types::Reserve_Cover_Loss_Type,
};

pub async fn parse_and_insert(
    app_state: &AppState<State>,
    item: Reserve_Cover_Loss_Type,
    index: usize,
    time_stamp: Timestamp,
    tx_hash: String,
    transaction: &mut Transaction<'_, DataBase>,
) -> Result<(), Error> {
    let seconds = time_stamp.seconds;
    let nanos = time_stamp.nanos.try_into()?;
    let time_stamp = DateTime::from_timestamp(seconds, nanos)
        .context("Could not parse time stamp")?;

    let reserve_cover_loss = Reserve_Cover_Loss {
        Tx_Hash: tx_hash,
        LS_contract_id: item.to,
        LS_symbol: item.payment_symbol,
        LS_amnt: BigDecimal::from_str(&item.payment_amount)?,
        LS_timestamp: time_stamp,
        Event_Block_Index: index.try_into()?,
    };

    app_state
        .database
        .reserve_cover_loss
        .insert_if_not_exists(reserve_cover_loss, transaction)
        .await?;

    Ok(())
}
