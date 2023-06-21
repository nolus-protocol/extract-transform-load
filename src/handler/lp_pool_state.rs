use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use std::str::FromStr;
use tokio::task::JoinHandle;

use crate::{
    configuration::{AppState, State},
    error::Error,
    model::{LP_Pool, LP_Pool_State}
};

pub async fn parse_and_insert(
    app_state: AppState<State>,
    timestsamp: DateTime<Utc>,
) -> Result<(), Error> {
    let items = app_state.database.lp_pool.get_all().await?;
    let mut data = vec![];
    let mut tasks = vec![];

    for item in items {
        tasks.push(proceed(&app_state, item, timestsamp));
    }

    let results = futures::future::join_all(tasks).await;

    for task in results {
        let item = task?;
        if let Some(ls_state) = item {
            data.push(ls_state);
        }
    }

    app_state
        .database
        .lp_pool_state
        .insert_many(&data)
        .await?;

    Ok(())
}

async fn proceed(
    state: &AppState<State>,
    item: LP_Pool,
    timestsamp: DateTime<Utc>,
) -> Result<Option<LP_Pool_State>, Error> {
    let data = state
        .query_api
        .lpp_balance_state(item.LP_Pool_id.to_string())
        .await?;

    if let Some(lp_pool_state) = data {
        let balance = lp_pool_state.balance.amount.parse::<u128>()?;
        let total_principal_due = lp_pool_state.total_principal_due.amount.parse::<u128>()?;
        let total_interest_due = lp_pool_state.total_interest_due.amount.parse::<u128>()?;
        let total_value_locked_asset =
            (balance + total_principal_due + total_interest_due).to_string();
        let pool_id = item.LP_Pool_id;

        let lp_pool_state = LP_Pool_State {
            LP_Pool_id: pool_id.to_string(),
            LP_Pool_timestamp: timestsamp,
            LP_Pool_total_value_locked_stable: state
                .in_stabe_by_pool_id(&pool_id, &total_value_locked_asset)
                .await?,
            LP_Pool_total_value_locked_asset: BigDecimal::from_str(&total_value_locked_asset)?,
            LP_Pool_total_issued_receipts: BigDecimal::from_str(
                &lp_pool_state.balance_nlpn.amount,
            )?,
            LP_Pool_total_borrowed_stable: state
                .in_stabe_by_pool_id(&pool_id, &lp_pool_state.total_principal_due.amount)
                .await?,
            LP_Pool_total_borrowed_asset: BigDecimal::from_str(
                &lp_pool_state.total_principal_due.amount,
            )?,
            LP_Pool_total_yield_stable: BigDecimal::from_str("0")?,
            LP_Pool_total_yield_asset: BigDecimal::from_str("0")?,
        };
        return Ok(Some(lp_pool_state));
    }


    Ok(None)
}

pub fn start_task(
    app_state: AppState<State>,
    timestsamp: DateTime<Utc>,
) -> JoinHandle<Result<(), Error>> {
    tokio::spawn(async move { parse_and_insert(app_state, timestsamp).await })
}
