use actix_web::{get, web, HttpResponse};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};

use crate::{
    configuration::{AppState, State},
    error::Error,
    helpers::to_csv_response,
};

const CACHE_KEY: &str = "historically_liquidated";

#[derive(Debug, Deserialize)]
pub struct Query {
    format: Option<String>,
}

#[get("/historically-liquidated")]
async fn index(
    state: web::Data<AppState<State>>,
    query: web::Query<Query>,
) -> Result<HttpResponse, Error> {
    // Try cache first
    if let Some(cached) = state.api_cache.historically_liquidated.get(CACHE_KEY).await {
        let liquidated: Vec<HistoricallyLiquidated> = cached
            .into_iter()
            .map(|l| HistoricallyLiquidated {
                contract_id: l.contract_id,
                asset: l.asset,
                loan: l.loan,
                total_liquidated: l.total_liquidated,
            })
            .collect();
        return match query.format.as_deref() {
            Some("csv") => to_csv_response(&liquidated, "historically-liquidated.csv"),
            _ => Ok(HttpResponse::Ok().json(liquidated)),
        };
    }

    // Cache miss - query DB
    let data = state
        .database
        .ls_liquidation
        .get_historically_liquidated()
        .await?;

    // Store in cache
    state.api_cache.historically_liquidated.set(CACHE_KEY, data.clone()).await;

    let liquidated: Vec<HistoricallyLiquidated> = data
        .into_iter()
        .map(|l| HistoricallyLiquidated {
            contract_id: l.contract_id,
            asset: l.asset,
            loan: l.loan,
            total_liquidated: l.total_liquidated,
        })
        .collect();

    match query.format.as_deref() {
        Some("csv") => to_csv_response(&liquidated, "historically-liquidated.csv"),
        _ => Ok(HttpResponse::Ok().json(liquidated)),
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HistoricallyLiquidated {
    pub contract_id: String,
    pub asset: String,
    pub loan: BigDecimal,
    pub total_liquidated: Option<BigDecimal>,
}
