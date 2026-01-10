use actix_web::{get, web, Responder};
use anyhow::Context;
use serde::{Deserialize, Serialize};
use tokio::task::JoinSet;

use crate::{
    configuration::{AppState, State},
    error::Error,
    model::LS_Loan_Collect,
};

#[get("/update/loan-collect")]
async fn index(
    state: web::Data<AppState<State>>,
    data: web::Query<Query>,
) -> Result<impl Responder, Error> {
    let auth = data.auth.to_owned().context("Auth is required")?;

    if auth != state.config.auth {
        return Ok(web::Json(Response { result: false }));
    };

    let data = state.database.ls_loan_collect.get_all().await?;
    let mut tasks = vec![];
    let max_tasks = state.config.max_tasks;

    for loan_collect in data {
        let s = state.get_ref().clone();
        tasks.push(proceed(s, loan_collect));
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
    ls_loan_collect: LS_Loan_Collect,
) -> Result<(), Error> {
    let ls_loan_closing = state
        .database
        .ls_loan_closing
        .get(ls_loan_collect.LS_contract_id.to_owned())
        .await?;

    let ls_opening = state
        .database
        .ls_opening
        .get(ls_loan_closing.LS_contract_id.to_owned())
        .await?
        .context(format!(
            "lease contract not found {}",
            &ls_loan_closing.LS_contract_id
        ))?;

    let protocol = match state
        .get_protocol_by_pool_id(&ls_opening.LS_loan_pool_id)
        .context(format!(
            "protocol not found {}",
            &ls_opening.LS_loan_pool_id
        )) {
        Ok(p) => Some(p),
        Err(_) => None,
    };

    let amount = state
        .in_stable_by_date(
            &ls_loan_collect.LS_symbol,
            &ls_loan_collect.LS_amount.to_string(),
            protocol,
            &ls_loan_closing.LS_timestamp,
        )
        .await?;

    state
        .database
        .ls_loan_collect
        .update_stable_amount(ls_loan_collect, amount)
        .await?;

    Ok(())
}
