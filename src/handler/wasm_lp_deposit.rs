use std::str::FromStr as _;

use bigdecimal::BigDecimal;
use sqlx::Transaction;

use crate::{
    configuration::{AppState, State},
    dao::DataBase,
    error::Error,
    handler::parse_event_timestamp,
    model::LP_Deposit,
    types::LP_Deposit_Type,
};

pub async fn parse_and_insert(
    app_state: &AppState<State>,
    item: LP_Deposit_Type,
    tx_hash: String,
    transaction: &mut Transaction<'_, DataBase>,
) -> Result<(), Error> {
    let at = parse_event_timestamp(&item.at)?;
    let protocol = app_state.get_protocol_by_pool_id(&item.to);

    let lp_deposit = LP_Deposit {
        Tx_Hash: tx_hash,
        LP_deposit_idx: None,
        LP_deposit_height: item.height.parse()?,
        LP_address_id: item.from.to_owned(),
        LP_timestamp: at,
        LP_Pool_id: item.to.to_owned(),
        LP_amnt_stable: app_state
            .in_stable_by_date(
                &item.deposit_symbol,
                &item.deposit_amount,
                protocol,
                &at,
            )
            .await?,
        LP_amnt_asset: BigDecimal::from_str(&item.deposit_amount)?,
        LP_amnt_receipts: BigDecimal::from_str(&item.receipts)?,
    };
    let isExists = app_state.database.lp_deposit.isExists(&lp_deposit).await?;

    if !isExists {
        app_state
            .database
            .lp_deposit
            .insert(lp_deposit, transaction)
            .await?;
    }

    Ok(())
}
