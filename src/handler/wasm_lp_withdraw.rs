use std::str::FromStr as _;

use bigdecimal::BigDecimal;
use sqlx::Transaction;

use crate::{
    configuration::{AppState, State},
    dao::DataBase,
    error::Error,
    handler::parse_event_timestamp,
    model::LP_Withdraw,
    types::LP_Withdraw_Type,
};

pub async fn parse_and_insert(
    app_state: &AppState<State>,
    item: LP_Withdraw_Type,
    tx_hash: String,
    transaction: &mut Transaction<'_, DataBase>,
) -> Result<(), Error> {
    let at = parse_event_timestamp(&item.at)?;
    let protocol = app_state.get_protocol_by_pool_id(&item.from);
    let lp_withdraw = LP_Withdraw {
        Tx_Hash: tx_hash,
        LP_withdraw_height: item.height.parse()?,
        LP_withdraw_idx: None,
        LP_address_id: item.to.to_owned(),
        LP_timestamp: at,
        LP_Pool_id: item.from,
        LP_amnt_stable: app_state
            .in_stable_by_date(
                &item.withdraw_symbol,
                &item.withdraw_amount,
                protocol,
                &at,
            )
            .await?,
        LP_amnt_asset: BigDecimal::from_str(&item.withdraw_amount)?,
        LP_amnt_receipts: BigDecimal::from_str(&item.receipts)?,
        LP_deposit_close: item.close.parse()?,
    };
    app_state
        .database
        .lp_withdraw
        .insert_if_not_exists(lp_withdraw, transaction)
        .await?;

    Ok(())
}
