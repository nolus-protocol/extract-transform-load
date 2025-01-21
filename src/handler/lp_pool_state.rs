use std::{str::FromStr, sync::Arc};

use bigdecimal::num_bigint::BigInt;
use bigdecimal::{BigDecimal, FromPrimitive};
use chrono::{DateTime, Utc};

use crate::{
    configuration::State,
    error::Error,
    model::{LP_Pool, LP_Pool_State},
    try_join_with_capacity,
};

pub async fn parse_and_insert(
    app_state: Arc<State>,
    timestsamp: DateTime<Utc>,
) -> Result<(), Error> {
    app_state
        .database
        .lp_pool_state
        .insert_many(&try_join_with_capacity(
            app_state
                .database
                .lp_pool
                .get_all()
                .await?
                .into_iter()
                .map(|item| proceed(app_state.clone(), item, timestsamp)),
            app_state.config.max_tasks,
        ))
        .await?;

    Ok(())
}

async fn proceed(
    state: Arc<State>,
    item: LP_Pool<String>,
    timestsamp: DateTime<Utc>,
) -> Result<Option<LP_Pool_State>, Error> {
    let (lp_pool_state, lp_pool_config_state) = tokio::try_join!(
        state.grpc.get_lpp_balance_state(item.LP_Pool_id.to_owned()),
        state.grpc.get_lpp_config_state(item.LP_Pool_id.to_owned())
    )?;

    let min_utilization_threshold = if let Some(c) =
        BigDecimal::from_u128(lp_pool_config_state.min_utilization)
    {
        c
    } else {
        return Err(Error::ServerError(String::from(
            "can not parse LP_Pool_Config_State_Type",
        )));
    };

    let balance = lp_pool_state.balance.amount.parse::<u128>()?;
    let total_principal_due =
        lp_pool_state.total_principal_due.amount.parse::<u128>()?;
    let total_interest_due =
        lp_pool_state.total_interest_due.amount.parse::<u128>()?;
    let total_value_locked_asset = BigDecimal::new(
        BigInt::from(balance) + total_principal_due + total_interest_due,
        0,
    );
    let pool_id = item.LP_Pool_id;

    let total_borrowed_asset =
        BigDecimal::from_str(&lp_pool_state.total_principal_due.amount)?;

    let total_value_locked_stable = state
        .in_stabe_by_pool_id(&pool_id, &total_value_locked_asset)
        .await?;

    let total_borrowed_stable = state
        .in_stabe_by_pool_id(&pool_id, &total_borrowed_asset)
        .await?;

    let lp_pool_state = LP_Pool_State {
        LP_Pool_id: pool_id,
        LP_Pool_timestamp: timestsamp,
        LP_Pool_total_value_locked_stable: total_value_locked_stable,
        LP_Pool_total_value_locked_asset: total_value_locked_asset.clone(),
        LP_Pool_total_issued_receipts: BigDecimal::from_str(
            &lp_pool_state.balance_nlpn.amount,
        )?,
        LP_Pool_total_borrowed_stable: total_borrowed_stable,
        LP_Pool_total_borrowed_asset: total_borrowed_asset,
        LP_Pool_min_utilization_threshold: min_utilization_threshold,
        LP_Pool_total_yield_stable: BigDecimal::from_str("0")?,
        LP_Pool_total_yield_asset: BigDecimal::from_str("0")?,
    };

    Ok(Some(lp_pool_state))
}
