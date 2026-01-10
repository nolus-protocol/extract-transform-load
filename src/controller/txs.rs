use std::{collections::HashSet, str::FromStr};

use actix_web::{get, web, HttpResponse};
use anyhow::Context;
use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

use crate::{
    configuration::{AppState, State},
    error::Error,
    helpers::Filter_Types,
};

#[utoipa::path(
    get,
    path = "/api/txs",
    tag = "Wallet Analytics",
    params(Query),
    responses(
        (status = 200, description = "Returns a paginated transaction history for a specific wallet address.", body = Vec<TxResponse>)
    )
)]
#[get("/txs")]
async fn index(
    state: web::Data<AppState<State>>,
    data: web::Query<Query>,
) -> Result<HttpResponse, Error> {
    let skip = data.skip.unwrap_or(0);
    let mut limit = data.limit.unwrap_or(10);

    let filters: Vec<String> = data
        .filter
        .as_deref()
        .unwrap_or("")
        .split(',')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();
    let mut to: Vec<String> = vec![];

    let mut set: HashSet<String> = HashSet::new();
    let mut combine = false;
    let filters: HashSet<String> = HashSet::from_iter(filters);
    let filters: Vec<String> = filters.into_iter().collect();

    if filters.len() > 10 {
        return Ok(HttpResponse::BadRequest().body("max filter length 10"));
    }

    if limit > 100 {
        limit = 100;
    }

    let address = data.address.to_lowercase().to_owned();

    for filter in &filters {
        let item = Filter_Types::from_str(filter)?;
        match item {
            Filter_Types::Earn => {
                for c in &state.config.lp_pools {
                    to.push(c.0.clone());
                }
            },
            Filter_Types::Positions => {
                let ids = state
                    .database
                    .ls_opening
                    .get_addresses(address.to_owned())
                    .await?;
                let ids: Vec<String> =
                    ids.iter().map(|item| item.0.clone()).collect();
                to.extend(ids);
            },
            Filter_Types::PositionsIds => {
                let ids: Vec<String> = data
                    .to
                    .clone()
                    .context("no contracts")?
                    .split(',')
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_string())
                    .collect();
                to.extend(ids);
            },
            _ => {},
        }
        let data: Vec<String> = item.into();
        set.extend(data);
    }

    let filters: Vec<String> = set.into_iter().collect();

    if filters.len() > 1 {
        combine = true;
    }

    let data = state
        .database
        .raw_message
        .get(address.to_owned(), skip, limit, filters, to, combine)
        .await?;

    Ok(HttpResponse::Ok().json(data))
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct Query {
    /// Number of records to skip (default: 0)
    skip: Option<i64>,
    /// Maximum number of records to return (default: 10, max: 100)
    limit: Option<i64>,
    /// Comma-separated filter types
    filter: Option<String>,
    /// Target contract addresses (comma-separated)
    to: Option<String>,
    /// Wallet address
    address: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct TxResponse {
    /// Transaction date
    pub date: DateTime<Utc>,
    /// Transaction hash
    pub tx_hash: String,
    /// Transaction type
    #[serde(rename = "type")]
    pub tx_type: String,
    /// Transaction amount
    #[schema(value_type = f64)]
    pub amount: BigDecimal,
}
