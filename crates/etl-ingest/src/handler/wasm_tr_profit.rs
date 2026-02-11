use std::str::FromStr as _;

use bigdecimal::BigDecimal;
use sqlx::Transaction;

use super::parse_event_timestamp;

use etl_core::{
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
    let at = parse_event_timestamp(&item.at)?;
    // Use the first available protocol for treasury operations
    let protocol = app_state.get_default_protocol();

    let tr_profit = TR_Profit {
        Tx_Hash: tx_hash,
        TR_Profit_height: item.height.parse()?,
        TR_Profit_idx: None,
        TR_Profit_timestamp: at,
        TR_Profit_amnt_stable: app_state
            .in_stable_by_date(
                &item.profit_symbol,
                &item.profit_amount,
                protocol.clone(),
                &at,
            )
            .await?,
        TR_Profit_amnt_nls: BigDecimal::from_str(&item.profit_amount)?,
    };

    app_state
        .database
        .tr_profit
        .insert_if_not_exists(tr_profit, transaction)
        .await?;

    Ok(())
}
