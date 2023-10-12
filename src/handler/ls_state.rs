use chrono::{DateTime, Utc};
use tokio::{
    join,
    task::{JoinHandle, JoinSet},
};

use crate::{
    configuration::{AppState, State},
    error::Error,
    model::{LS_Opening, LS_State},
};

pub async fn parse_and_insert(
    app_state: AppState<State>,
    timestsamp: DateTime<Utc>,
) -> Result<(), Error> {
    let items = app_state.database.ls_state.get_active_states().await?;
    let mut tasks = vec![];
    let mut data = vec![];
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

    app_state.database.ls_state.insert_many(&data).await?;

    Ok(())
}

async fn proceed(
    state: AppState<State>,
    item: LS_Opening,
    timestsamp: DateTime<Utc>,
) -> Result<Option<LS_State>, Error> {
    let contract = item.LS_contract_id.to_string();
    let query_data = state.query_api.lease_state(contract).await?;

    if let Some(data) = query_data {
        if let Some(status) = data.opened {
            let pool_currency = state.get_currency_by_pool_id(&item.LS_loan_pool_id)?;

            let (price, pool_currency_price) = join!(
                state.database.mp_asset.get_price(&item.LS_asset_symbol),
                state.database.mp_asset.get_price(&pool_currency.2),
            );

            let (price,) = price?;
            let (pool_currency_price,) = pool_currency_price?;

            let ls_state = LS_State {
                LS_contract_id: item.LS_contract_id,
                LS_timestamp: timestsamp,
                LS_amnt_stable: state.in_stabe_calc(&price, &status.amount.amount)?,
                LS_prev_margin_stable: state
                    .in_stabe_calc(&pool_currency_price, &status.previous_margin_due.amount)?,
                LS_prev_interest_stable: state
                    .in_stabe_calc(&pool_currency_price, &status.previous_interest_due.amount)?,
                LS_current_margin_stable: state
                    .in_stabe_calc(&pool_currency_price, &status.current_margin_due.amount)?,
                LS_current_interest_stable: state
                    .in_stabe_calc(&pool_currency_price, &status.current_interest_due.amount)?,
                LS_principal_stable: state
                    .in_stabe_calc(&pool_currency_price, &status.principal_due.amount)?,
            };

            return Ok(Some(ls_state));
        }
    }

    Ok(None)
}

pub fn start_task(
    app_state: AppState<State>,
    timestsamp: DateTime<Utc>,
) -> JoinHandle<Result<(), Error>> {
    tokio::spawn(async move { parse_and_insert(app_state, timestsamp).await })
}
