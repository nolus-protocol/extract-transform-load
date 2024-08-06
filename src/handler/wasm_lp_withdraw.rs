use bigdecimal::BigDecimal;
use chrono::DateTime;
use sqlx::Transaction;
use std::str::FromStr;

use crate::{
    configuration::{AppState, State},
    dao::DataBase,
    error::Error,
    model::LP_Withdraw,
    types::LP_Withdraw_Type,
};

pub async fn parse_and_insert(
    app_state: &AppState<State>,
    item: LP_Withdraw_Type,
    tx_hash: String,
    transaction: &mut Transaction<'_, DataBase>,
) -> Result<(), Error> {
    let sec: i64 = item.at.parse()?;
    let at_sec = sec / 1_000_000_000;
    let at = DateTime::from_timestamp(at_sec, 0).ok_or_else(|| {
        Error::DecodeDateTimeError(format!(
            "Wasm_LP_withdraw date parse {}",
            at_sec
        ))
    })?;
    let protocol = app_state.get_protocol_by_pool_id(&item.from);
    let lp_withdraw = LP_Withdraw {
        Tx_Hash: Some(tx_hash),
        LP_withdraw_height: item.height.parse()?,
        LP_withdraw_idx: None,
        LP_address_id: item.to.to_owned(),
        LP_timestamp: at,
        LP_Pool_id: item.from,
        LP_amnt_stable: app_state
            .in_stabe_by_date(
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
    let isExists = app_state
        .database
        .lp_withdraw
        .isExists(&lp_withdraw)
        .await?;

    if !isExists {
        app_state
            .database
            .lp_withdraw
            .insert(lp_withdraw, transaction)
            .await?;
    }

    Ok(())
}
