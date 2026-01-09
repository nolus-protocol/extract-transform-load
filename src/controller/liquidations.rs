use actix_web::{get, web, HttpResponse};
use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    configuration::{AppState, State},
    error::Error,
    helpers::{parse_period_months, to_csv_response, to_streaming_csv_response},
};

#[derive(Debug, Deserialize)]
pub struct Query {
    format: Option<String>,
    period: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Liquidation {
    pub timestamp: DateTime<Utc>,
    pub ticker: String,
    pub contract_id: String,
    pub user: Option<String>,
    pub transaction_type: Option<String>,
    pub liquidation_amount: BigDecimal,
    pub closed_loan: bool,
    pub down_payment: BigDecimal,
    pub loan: BigDecimal,
    pub liquidation_price: Option<BigDecimal>,
}

impl From<crate::dao::postgre::ls_liquidation::LiquidationData> for Liquidation {
    fn from(l: crate::dao::postgre::ls_liquidation::LiquidationData) -> Self {
        Self {
            timestamp: l.timestamp,
            ticker: l.ticker,
            contract_id: l.contract_id,
            user: l.user,
            transaction_type: l.transaction_type,
            liquidation_amount: l.liquidation_amount,
            closed_loan: l.closed_loan,
            down_payment: l.down_payment,
            loan: l.loan,
            liquidation_price: l.liquidation_price,
        }
    }
}

#[get("/liquidations")]
async fn index(
    state: web::Data<AppState<State>>,
    query: web::Query<Query>,
) -> Result<HttpResponse, Error> {
    let months = parse_period_months(&query.period)?;
    let period_str = query.period.as_deref().unwrap_or("12m");
    let cache_key = format!("liquidations_{}", period_str);

    if let Some(cached) = state.api_cache.liquidations.get(&cache_key).await {
        let data: Vec<Liquidation> = cached.into_iter().map(Into::into).collect();
        return match query.format.as_deref() {
            Some("csv") => to_csv_response(&data, "liquidations.csv"),
            _ => Ok(HttpResponse::Ok().json(data)),
        };
    }

    let data = state
        .database
        .ls_liquidation
        .get_liquidations_with_window(months)
        .await?;

    state.api_cache.liquidations.set(&cache_key, data.clone()).await;

    let response: Vec<Liquidation> = data.into_iter().map(Into::into).collect();

    match query.format.as_deref() {
        Some("csv") => to_csv_response(&response, "liquidations.csv"),
        _ => Ok(HttpResponse::Ok().json(response)),
    }
}

#[get("/liquidations/export")]
pub async fn export(state: web::Data<AppState<State>>) -> Result<HttpResponse, Error> {
    const CACHE_KEY: &str = "liquidations_all";

    if let Some(cached) = state.api_cache.liquidations.get(CACHE_KEY).await {
        let data: Vec<Liquidation> = cached.into_iter().map(Into::into).collect();
        return to_streaming_csv_response(data, "liquidations.csv");
    }

    let data = state.database.ls_liquidation.get_all_liquidations().await?;
    state.api_cache.liquidations.set(CACHE_KEY, data.clone()).await;

    let response: Vec<Liquidation> = data.into_iter().map(Into::into).collect();
    to_streaming_csv_response(response, "liquidations.csv")
}
