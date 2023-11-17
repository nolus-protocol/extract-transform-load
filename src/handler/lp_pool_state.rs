use bigdecimal::{BigDecimal, FromPrimitive};
use chrono::{DateTime, Utc};
use std::str::FromStr;
use tokio::task::{JoinHandle, JoinSet};

use crate::{
    configuration::{AppState, State},
    error::Error,
    model::{LP_Pool, LP_Pool_State},
};

pub async fn parse_and_insert(
    app_state: AppState<State>,
    timestsamp: DateTime<Utc>,
) -> Result<(), Error> {
    let items = app_state.database.lp_pool.get_all().await?;
    let mut data = vec![];
    let mut tasks = vec![];
    let max_tasks = app_state.config.max_tasks;

    for item in items {
        tasks.push(proceed(app_state.clone(), item, timestsamp));
    }

    while !tasks.is_empty() {
        let mut st = JoinSet::new();
        let range = if tasks.len() > max_tasks {
            max_tasks
        } else {
            tasks.len()
        };

        for _t in 0..range {
            if let Some(item) = tasks.pop() {
                st.spawn(item);
            }
        }

        while let Some(item) = st.join_next().await {
            let d = item??;
            if let Some(record) = d {
                data.push(record);
            }
        }
    }

    app_state.database.lp_pool_state.insert_many(&data).await?;

    Ok(())
}

async fn proceed(
    state: AppState<State>,
    item: LP_Pool,
    timestsamp: DateTime<Utc>,
) -> Result<Option<LP_Pool_State>, Error> {
    let (data, config) = tokio::try_join!(
        state
            .query_api
            .lpp_balance_state(item.LP_Pool_id.to_string()),
        state
            .query_api
            .lpp_config_state(item.LP_Pool_id.to_string())
    )?;

    let lp_pool_state = if let Some(d) = data {
        d
    } else {
        return Err(Error::ServerError(String::from(
            "can not parse LP_Pool_State_Type",
        )));
    };

    let lp_pool_config_state = if let Some(c) = config {
        c
    } else {
        return Err(Error::ServerError(String::from(
            "can not parse LP_Pool_Config_State_Type",
        )));
    };

    let min_utilization_threshold =
        if let Some(c) = BigDecimal::from_u128(lp_pool_config_state.min_utilization) {
            c
        } else {
            return Err(Error::ServerError(String::from(
                "can not parse LP_Pool_Config_State_Type",
            )));
        };

    let balance = lp_pool_state.balance.amount.parse::<u128>()?;
    let total_principal_due = lp_pool_state.total_principal_due.amount.parse::<u128>()?;
    let total_interest_due = lp_pool_state.total_interest_due.amount.parse::<u128>()?;
    let total_value_locked_asset = (balance + total_principal_due + total_interest_due).to_string();
    let pool_id = item.LP_Pool_id;

    let lp_pool_state = LP_Pool_State {
        LP_Pool_id: pool_id.to_string(),
        LP_Pool_timestamp: timestsamp,
        LP_Pool_total_value_locked_stable: state
            .in_stabe_by_pool_id(&pool_id, &total_value_locked_asset)
            .await?,
        LP_Pool_total_value_locked_asset: BigDecimal::from_str(&total_value_locked_asset)?,
        LP_Pool_total_issued_receipts: BigDecimal::from_str(&lp_pool_state.balance_nlpn.amount)?,
        LP_Pool_total_borrowed_stable: state
            .in_stabe_by_pool_id(&pool_id, &lp_pool_state.total_principal_due.amount)
            .await?,
        LP_Pool_total_borrowed_asset: BigDecimal::from_str(
            &lp_pool_state.total_principal_due.amount,
        )?,
        LP_Pool_min_utilization_threshold: min_utilization_threshold,
        LP_Pool_total_yield_stable: BigDecimal::from_str("0")?,
        LP_Pool_total_yield_asset: BigDecimal::from_str("0")?,
    };

    Ok(Some(lp_pool_state))
}

pub fn start_task(
    app_state: AppState<State>,
    timestsamp: DateTime<Utc>,
) -> JoinHandle<Result<(), Error>> {
    tokio::spawn(async move { parse_and_insert(app_state, timestsamp).await })
}
