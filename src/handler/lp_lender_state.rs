use std::str::FromStr as _;

use anyhow::Context;
use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use tokio::task::{JoinHandle, JoinSet};

use crate::{
    configuration::{AppState, State},
    error::Error,
    model::LP_Lender_State,
};

pub async fn parse_and_insert(
    app_state: AppState<State>,
    timestsamp: DateTime<Utc>,
) -> Result<(), Error> {
    let items = app_state
        .database
        .lp_lender_state
        .get_active_states()
        .await?;
    let mut data: Vec<LP_Lender_State> = Vec::new();
    let mut tasks = vec![];
    let max_tasks = app_state.config.max_tasks;

    for item in items {
        let c = app_state
            .config
            .hash_map_lp_pools
            .get(&item.1)
            .context("protocol not found")?;
        if c.3 {
            tasks.push(proceed(app_state.clone(), item, timestsamp));
        }
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
            data.push(d);
        }
    }

    app_state
        .database
        .lp_lender_state
        .insert_many(&data)
        .await?;

    Ok(())
}

async fn proceed(
    state: AppState<State>,
    item: (String, String),
    timestsamp: DateTime<Utc>,
) -> Result<LP_Lender_State, Error> {
    let (LP_address_id, LP_Pool_id) = item;
    let (balance_task, lpp_price) = tokio::join!(
        state
            .grpc
            .get_balance_state(LP_Pool_id.to_owned(), LP_address_id.to_owned()),
        state.grpc.get_lpp_price(LP_Pool_id.to_owned())
    );

    let balance = balance_task?;
    let price = lpp_price?;

    let lpp_price = {
        let amount = BigDecimal::from_str(&price.amount.amount)?;
        let quote_amount = BigDecimal::from_str(&price.amount_quote.amount)?;

        &quote_amount / &amount
    };

    let lpp_balance = BigDecimal::from_str(&balance.amount)?;
    let value = lpp_balance * lpp_price;
    let amnt_stable = value.to_string();
    let amnt_stable =
        state.in_stabe_by_pool_id(&LP_Pool_id, &amnt_stable).await?;

    let lp_lender_state = LP_Lender_State {
        LP_Lender_id: LP_address_id.to_owned(),
        LP_Pool_id,
        LP_timestamp: timestsamp,
        LP_Lender_stable: amnt_stable,
        LP_Lender_asset: value,
        LP_Lender_receipts: BigDecimal::from_str(&balance.amount)?,
    };

    Ok(lp_lender_state)
}

pub fn start_task(
    app_state: AppState<State>,
    timestsamp: DateTime<Utc>,
) -> JoinHandle<Result<(), Error>> {
    tokio::spawn(async move { parse_and_insert(app_state, timestsamp).await })
}
