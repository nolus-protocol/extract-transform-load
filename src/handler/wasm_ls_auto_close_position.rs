use anyhow::Context as _;
use chrono::DateTime;
use cosmrs::proto::Timestamp;
use sqlx::Transaction;

use crate::{
    configuration::{AppState, State},
    dao::DataBase,
    error::Error,
    helpers::Auto_Close_Strategies,
    model::LS_Auto_Close_Position,
    types::LS_Auto_Close_Position_Type,
};

pub async fn parse_and_insert(
    app_state: &AppState<State>,
    item: LS_Auto_Close_Position_Type,
    time_stamp: Timestamp,
    tx_hash: String,
    transaction: &mut Transaction<'_, DataBase>,
) -> Result<(), Error> {
    let seconds = time_stamp.seconds.try_into()?;
    let nanos = time_stamp.nanos.try_into()?;
    let time_stamp = DateTime::from_timestamp(seconds, nanos)
        .context("Could not parse time stamp")?;

    let data = if let Some(i) = item.stop_loss_ltv {
        Ok((i.parse()?, Auto_Close_Strategies::StopLoss))
    } else if let Some(i) = item.take_profit_ltv {
        Ok((i.parse()?, Auto_Close_Strategies::TakeProfit))
    } else {
        Err(Error::AutoClosePosition())
    };

    let (amout, strategy) = data?;

    let ls_auto_close_position = LS_Auto_Close_Position {
        Tx_Hash: tx_hash,
        LS_contract_id: item.to,
        LS_timestamp: time_stamp,
        LS_Close_Strategy: strategy.to_string(),
        LS_Close_Strategy_Ltv: amout,
    };

    let isExists = app_state
        .database
        .ls_auto_close_position
        .isExists(&ls_auto_close_position)
        .await?;

    if !isExists {
        app_state
            .database
            .ls_auto_close_position
            .insert(ls_auto_close_position, transaction)
            .await?;
    }

    Ok(())
}
