use bigdecimal::BigDecimal;
use chrono::{DateTime, NaiveDateTime, Utc};
use sqlx::Transaction;
use std::str::FromStr;

use crate::{
    configuration::{AppState, State},
    dao::DataBase,
    error::Error,
    model::LS_Opening,
    types::LS_Opening_Type,
};

pub async fn parse_and_insert(
    app_state: &AppState<State>,
    item: LS_Opening_Type,
    transaction: &mut Transaction<'_, DataBase>,
) -> Result<(), Error> {
    let f1 = app_state.database.mp_asset.get_price(&item.currency);
    let f2 = app_state
        .database
        .mp_asset
        .get_price(&item.downpayment_symbol);

    let (loan_price, downpayment_price) = tokio::try_join!(f1, f2)?;
    let air: i16 = item.air.parse()?;

    let at_sec: i64 = item.at.parse()?;
    let time = NaiveDateTime::from_timestamp_opt(at_sec, 0).ok_or_else(|| Error::DecodeDateTimeError(format!(
        "Wasm_LS_Open date parse {}",
        at_sec
    )))?;
    let at = DateTime::<Utc>::from_utc(time, Utc);

    let (l_price,) = loan_price;
    let (d_price,) = downpayment_price;

    let ls_opening = LS_Opening {
        LS_contract_id: item.id,
        LS_address_id: item.customer,
        LS_asset_symbol: item.currency,
        LS_interest: air,
        LS_timestamp: at,
        LS_loan_pool_id: item.loan_pool_id.to_owned(),
        LS_loan_amnt_stable: app_state.in_stabe_calc(&l_price, &item.loan_amount)?,
        LS_loan_amnt_asset: BigDecimal::from_str(item.loan_amount.as_str())?,
        LS_cltr_symbol: item.downpayment_symbol.to_owned(),
        LS_cltr_amnt_stable: app_state.in_stabe_calc(&d_price, &item.downpayment_amount)?,
        LS_cltr_amnt_asset: BigDecimal::from_str(item.downpayment_amount.as_str())?,
        LS_native_amnt_stable: BigDecimal::from(0),
        LS_native_amnt_nolus: BigDecimal::from(0),
    };

    app_state
        .database
        .ls_opening
        .insert(ls_opening, transaction)
        .await?;

    Ok(())
}
