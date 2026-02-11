use std::str::FromStr as _;

use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use tokio::{
    join,
    task::{JoinHandle, JoinSet},
};

use etl_core::{
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
    let contract = item.LS_contract_id.to_owned();
    let data = state.grpc.get_lease_state(contract.to_owned()).await?;

    let Some(status) = data.into_opened() else {
        return Ok(None);
    };

    let pool_currency = state.get_currency_by_pool_id(&item.LS_loan_pool_id)?;
    let protocol = state.get_protocol_by_pool_id(&item.LS_loan_pool_id);

    let (price, pool_currency_price) = join!(
        state
            .database
            .mp_asset
            .get_price(&status.amount.ticker, protocol.to_owned()),
        state
            .database
            .mp_asset
            .get_price(&pool_currency.0, protocol.to_owned()),
    );

    let (price,) = price?;
    let (pool_currency_price,) = pool_currency_price?;

    let overdue_margin_stable = state
        .in_stable_calc(&pool_currency_price, &status.overdue_margin.amount)?;
    let overdue_interest_stable = state.in_stable_calc(
        &pool_currency_price,
        &status.overdue_interest.amount,
    )?;
    let due_margin_stable = state
        .in_stable_calc(&pool_currency_price, &status.due_margin.amount)?;
    let due_interest_stable = state
        .in_stable_calc(&pool_currency_price, &status.due_interest.amount)?;

    let prev_margin_asset =
        BigDecimal::from_str(&status.overdue_margin.amount)?;
    let prev_interest_asset =
        BigDecimal::from_str(&status.overdue_interest.amount)?;
    let current_margin_asset = BigDecimal::from_str(&status.due_margin.amount)?;
    let current_interest_asset =
        BigDecimal::from_str(&status.due_interest.amount)?;
    let principal_asset = BigDecimal::from_str(&status.principal_due.amount)?;

    let lpn_loan_amnt = BigDecimal::from_str(&status.amount.amount)? * &price
        / &pool_currency_price;

    let ls_state = LS_State {
        LS_contract_id: item.LS_contract_id,
        LS_timestamp: timestsamp,
        LS_amnt_stable: state.in_stable_calc(&price, &status.amount.amount)?,
        LS_amnt: BigDecimal::from_str(&status.amount.amount)?,
        LS_prev_margin_stable: overdue_margin_stable,
        LS_prev_interest_stable: overdue_interest_stable,
        LS_current_margin_stable: due_margin_stable,
        LS_current_interest_stable: due_interest_stable,
        LS_principal_stable: state.in_stable_calc(
            &pool_currency_price,
            &status.principal_due.amount,
        )?,
        LS_lpn_loan_amnt: lpn_loan_amnt,
        LS_prev_margin_asset: prev_margin_asset,
        LS_prev_interest_asset: prev_interest_asset,
        LS_current_margin_asset: current_margin_asset,
        LS_current_interest_asset: current_interest_asset,
        LS_principal_asset: principal_asset,
    };

    Ok(Some(ls_state))
}

pub fn start_task(
    app_state: AppState<State>,
    timestsamp: DateTime<Utc>,
) -> JoinHandle<Result<(), Error>> {
    tokio::spawn(async move { parse_and_insert(app_state, timestsamp).await })
}
