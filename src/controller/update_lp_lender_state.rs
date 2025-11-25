use actix_web::{get, web, Responder};
use anyhow::Context;
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};
use tokio::task::JoinSet;

use crate::{
    configuration::{AppState, State},
    error::Error,
    model::LP_Lender_State,
};

#[get("/update/lp-lender-state")]
async fn index(
    state: web::Data<AppState<State>>,
    data: web::Query<Query>,
) -> Result<impl Responder, Error> {
    let auth = data.auth.to_owned().context("Auth is required")?;

    if auth != state.config.auth {
        return Ok(web::Json(Response { result: false }));
    };

    let data = state.database.lp_lender_state.get_all().await?;
    let mut tasks = vec![];
    let max_tasks = state.config.max_tasks;

    for lease in data {
        let s = state.get_ref().clone();
        tasks.push(proceed(s, lease));
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
            item??;
        }
    }

    return Ok(web::Json(Response { result: true }));
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub result: bool,
}

#[derive(Debug, Deserialize)]
pub struct Query {
    auth: Option<String>,
}

async fn proceed(
    state: AppState<State>,
    lp_lender_state: LP_Lender_State,
) -> Result<(), Error> {
    let ls_pool_state = state
        .database
        .lp_pool_state
        .get_by_date(
            lp_lender_state.LP_Pool_id.to_owned(),
            &lp_lender_state.LP_timestamp,
        )
        .await?;

    if lp_lender_state.LP_Lender_asset > BigDecimal::from(0) {
        let price = ls_pool_state.LP_Pool_total_value_locked_asset
            / ls_pool_state.LP_Pool_total_issued_receipts;
        let LP_Lender_asset = price * &lp_lender_state.LP_Lender_receipts;

        let lp_lender_price = &lp_lender_state.LP_Lender_stable
            / &lp_lender_state.LP_Lender_asset;
        let LP_Lender_stable = lp_lender_price * &LP_Lender_asset;
        let LP_Lender_receipts = lp_lender_state.LP_Lender_receipts.to_owned();

        let lp_lender_state = LP_Lender_State {
            LP_Lender_asset,
            LP_Lender_stable,
            LP_Lender_id: lp_lender_state.LP_Lender_id,
            LP_Pool_id: lp_lender_state.LP_Pool_id,
            LP_timestamp: lp_lender_state.LP_timestamp,
            LP_Lender_receipts,
        };

        state
            .database
            .lp_lender_state
            .update(lp_lender_state)
            .await?;
    }

    Ok(())
}
