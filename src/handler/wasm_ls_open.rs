use std::str::FromStr as _;

use bigdecimal::BigDecimal;
use futures::TryFutureExt as _;
use sqlx::Transaction;

use crate::{
    configuration::{AppState, State},
    dao::DataBase,
    error::Error,
    handler::parse_event_timestamp,
    model::LS_Opening,
    types::LS_Opening_Type,
};

/// Calculate liquidation price at open
/// For Long: liquidation_price = (loan / 0.9) / (down_payment + loan) * price
/// For Short: liquidation_price = (down_payment + loan) / (total_position_lpn / 0.9)
fn calculate_liquidation_price(
    position_type: &str,
    down_payment_stable: &BigDecimal,
    loan_stable: &BigDecimal,
    opening_price: &BigDecimal,
    total_position_lpn: &BigDecimal,
) -> Option<BigDecimal> {
    let ltv_factor = BigDecimal::from_str("0.9").ok()?;
    let total_collateral = down_payment_stable + loan_stable;

    if total_collateral == BigDecimal::from(0) || *total_position_lpn == BigDecimal::from(0) {
        return None;
    }

    match position_type {
        "Long" => {
            // (loan / 0.9) / (down_payment + loan) * price
            let debt_at_liquidation = loan_stable / &ltv_factor;
            Some(&debt_at_liquidation / &total_collateral * opening_price)
        }
        "Short" => {
            // (down_payment + loan) / (total_position_lpn / 0.9)
            let position_at_liquidation = total_position_lpn / &ltv_factor;
            Some(&total_collateral / &position_at_liquidation)
        }
        _ => None,
    }
}

pub async fn parse_and_insert(
    app_state: &AppState<State>,
    item: LS_Opening_Type,
    tx_hash: String,
    height: i64,
    transaction: &mut Transaction<'_, DataBase>,
) -> Result<(), Error> {
    let at = parse_event_timestamp(&item.at)?;

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
    let LS_lpn_loan_amnt = &LS_loan_amnt * &lease_currency_price / &lpn_price;

    // Fetch pool config for pre-computed columns
    let pool_config = app_state
        .database
        .pool_config
        .get_by_pool_id(&item.loan_pool_id)
        .await
        .ok()
        .flatten();

    // Extract pre-computed values from pool_config
    let (position_type, lpn_symbol, lpn_decimals) = match &pool_config {
        Some(pc) => (
            Some(pc.position_type.clone()),
            Some(pc.lpn_symbol.clone()),
            Some(pc.lpn_decimals),
        ),
        None => (None, None, None),
    };

    // Calculate down payment in stable
    let down_payment_stable = app_state
        .in_stabe_calc(&d_price, &item.downpayment_amount)?;

    // Opening price is the leased currency price
    let opening_price = Some(lease_currency_price.clone());

    // Calculate liquidation price at open
    let liquidation_price_at_open = position_type.as_ref().and_then(|pt| {
        calculate_liquidation_price(
            pt,
            &down_payment_stable,
            &LS_loan_amnt_stable,
            &lease_currency_price,
            &LS_lpn_loan_amnt,
        )
    });

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
        LS_cltr_amnt_stable: down_payment_stable,
        LS_cltr_amnt_asset: BigDecimal::from_str(
            item.downpayment_amount.as_str(),
        )?,
        LS_native_amnt_stable: BigDecimal::from(0),
        LS_native_amnt_nolus: BigDecimal::from(0),
        LS_lpn_loan_amnt,
        // Pre-computed columns
        LS_position_type: position_type,
        LS_lpn_symbol: lpn_symbol,
        LS_lpn_decimals: lpn_decimals,
        LS_opening_price: opening_price,
        LS_liquidation_price_at_open: liquidation_price_at_open,
    };

    app_state
        .database
        .ls_opening
        .insert_if_not_exists(ls_opening, transaction)
        .await?;

    Ok(())
}
