use bigdecimal::BigDecimal;
use chrono::DateTime;
use sqlx::Transaction;
use std::str::FromStr;

use crate::{
    configuration::{AppState, State},
    dao::DataBase,
    error::Error,
    model::TR_Profit,
    types::TR_Profit_Type,
};

pub async fn parse_and_insert(
    app_state: &AppState<State>,
    item: TR_Profit_Type,
    tx_hash: String,
    transaction: &mut Transaction<'_, DataBase>,
) -> Result<(), Error> {
    let sec: i64 = item.at.parse()?;
    let at_sec = sec / 1_000_000_000;
    let at = DateTime::from_timestamp(at_sec, 0).ok_or_else(|| {
        Error::DecodeDateTimeError(format!(
            "Wasm_TR_profit date parse {}",
            at_sec
        ))
    })?;
    let protocol = &app_state.config.initial_protocol;

    let tr_profit = TR_Profit {
        Tx_Hash: tx_hash,
        TR_Profit_height: item.height.parse()?,
        TR_Profit_idx: None,
        TR_Profit_timestamp: at,
        TR_Profit_amnt_stable: app_state
            .in_stabe_by_date(
                &item.profit_symbol,
                &item.profit_amount,
                Some(protocol.to_owned()),
                &at,
            )
            .await?,
        TR_Profit_amnt_nls: BigDecimal::from_str(&item.profit_amount)?,
    };

    let isExists = app_state.database.tr_profit.isExists(&tr_profit).await?;

    if !isExists {
        app_state
            .database
            .tr_profit
            .insert(tr_profit, transaction)
            .await?;
    }

    Ok(())
}
