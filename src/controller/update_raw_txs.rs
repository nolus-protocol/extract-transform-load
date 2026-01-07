use actix_web::{get, web, Responder};
use anyhow::Context;
use serde::{Deserialize, Serialize};
use tokio::task::JoinSet;

use crate::{
    configuration::{AppState, State},
    error::Error,
    model::Raw_Message,
};

#[get("/update/raw-txs")]
async fn index(
    state: web::Data<AppState<State>>,
    data: web::Query<Query>,
) -> Result<impl Responder, Error> {
    let auth = data.auth.to_owned().context("Auth is required")?;

    if auth != state.config.auth {
        return Ok(web::Json(Response { result: false }));
    };

    let data = state.database.raw_message.get_all().await?;
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

    Ok(web::Json(Response { result: true }))
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
    raw_message: Raw_Message,
) -> Result<(), Error> {
    let tx = state
        .grpc
        .get_tx(raw_message.tx_hash.to_owned(), raw_message.block)
        .await?
        .context(format!("missing transaction {}", &raw_message.tx_hash))?;

    let mut msg = raw_message;
    msg.code = Some(tx.code.try_into()?);

    state.database.raw_message.update(msg).await?;

    Ok(())
}
