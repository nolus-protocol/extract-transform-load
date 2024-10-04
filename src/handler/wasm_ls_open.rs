use bigdecimal::BigDecimal;
use chrono::DateTime;
use futures::TryFutureExt;
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
    tx_hash: String,
    height: i64,
    transaction: &mut Transaction<'_, DataBase>,
) -> Result<(), Error> {
    let sec: i64 = item.at.parse()?;
    let at_sec = sec / 1_000_000_000;

    let at = DateTime::from_timestamp(at_sec, 0).ok_or_else(|| {
        Error::DecodeDateTimeError(format!(
            "Wasm_LS_Open date parse {}",
            at_sec
        ))
    })?;

    let protocol = app_state.get_protocol_by_pool_id(&item.loan_pool_id);
    let lpn_currency = app_state.get_currency_by_pool_id(&item.loan_pool_id)?;

    let f1 = app_state
        .database
        .mp_asset
        .get_price_by_date(&item.loan_symbol, protocol.to_owned(), &at)
        .map_err(Error::from);

    let f2 = app_state
        .database
        .mp_asset
        .get_price_by_date(&item.downpayment_symbol, protocol.to_owned(), &at)
        .map_err(Error::from);

    let f3 = app_state
        .grpc
        .get_lease_state_by_block(item.id.to_owned(), height);

    let f4 = app_state
        .database
        .mp_asset
        .get_price_by_date(&lpn_currency.0, protocol.to_owned(), &at)
        .map_err(Error::from);

    let f5 = app_state
        .database
        .mp_asset
        .get_price_by_date(&item.currency, protocol.to_owned(), &at)
        .map_err(Error::from);

    let (
        loan_price,
        downpayment_price,
        lease_state,
        lpn_price,
        lease_currency_price,
    ) = tokio::try_join!(f1, f2, f3, f4, f5)?;
    let air: i16 = item.air.parse()?;

    let LS_loan_amnt = match lease_state.opened {
        Some(item) => item.amount.amount,
        None => String::from("0"),
    };

    let LS_loan_amnt = BigDecimal::from_str(&LS_loan_amnt)?;

    let (l_price,) = loan_price;
    let (d_price,) = downpayment_price;
    let (lpn_price,) = lpn_price;
    let (lease_currency_price,) = lease_currency_price;

    let LS_loan_amnt_stable =
        app_state.in_stabe_calc(&l_price, &item.loan_amount)?;
    let LS_lpn_loan_amnt = &LS_loan_amnt * lease_currency_price / lpn_price;

    let ls_opening = LS_Opening {
        Tx_Hash: tx_hash,
        LS_contract_id: item.id,
        LS_address_id: item.customer,
        LS_asset_symbol: item.currency,
        LS_loan_amnt,
        LS_interest: air,
        LS_timestamp: at,
        LS_loan_pool_id: item.loan_pool_id.to_owned(),
        LS_loan_amnt_stable,
        LS_loan_amnt_asset: BigDecimal::from_str(item.loan_amount.as_str())?,
        LS_cltr_symbol: item.downpayment_symbol.to_owned(),
        LS_cltr_amnt_stable: app_state
            .in_stabe_calc(&d_price, &item.downpayment_amount)?,
        LS_cltr_amnt_asset: BigDecimal::from_str(
            item.downpayment_amount.as_str(),
        )?,
        LS_native_amnt_stable: BigDecimal::from(0),
        LS_native_amnt_nolus: BigDecimal::from(0),
        LS_lpn_loan_amnt,
    };
    let isExists = app_state.database.ls_opening.isExists(&ls_opening).await?;

    if !isExists {
        app_state
            .database
            .ls_opening
            .insert(ls_opening, transaction)
            .await?;
    }

    Ok(())
}
