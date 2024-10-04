use anyhow::{Context, Result};
use bigdecimal::BigDecimal;
use chrono::DateTime;
use cosmos_sdk_proto::Timestamp;
use sqlx::Transaction;
use std::str::FromStr;

use crate::{
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
    let seconds = time_stamp.seconds.try_into()?;
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

    let isExists = app_state
        .database
        .reserve_cover_loss
        .isExists(&reserve_cover_loss)
        .await?;

    if !isExists {
        app_state
            .database
            .reserve_cover_loss
            .insert(reserve_cover_loss, transaction)
            .await?;
    }

    Ok(())
}
