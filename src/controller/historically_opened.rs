use actix_web::{get, web, HttpResponse};
use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    configuration::{AppState, State},
    error::Error,
    helpers::to_csv_response,
};

const DEFAULT_LIMIT: i64 = 100;
const MAX_LIMIT: i64 = 1000;

#[derive(Debug, Deserialize)]
pub struct Query {
    skip: Option<i64>,
    limit: Option<i64>,
    format: Option<String>,
}

#[get("/historically-opened")]
async fn index(
    state: web::Data<AppState<State>>,
    query: web::Query<Query>,
) -> Result<HttpResponse, Error> {
    let skip = query.skip.unwrap_or(0);
    let limit = query.limit.unwrap_or(DEFAULT_LIMIT).min(MAX_LIMIT);
    let cache_key = format!("historically_opened_{}_{}", skip, limit);

    // Try cache first
    if let Some(cached) = state.api_cache.historically_opened.get(&cache_key).await {
        let opened: Vec<HistoricallyOpened> = cached
            .into_iter()
            .map(|o| HistoricallyOpened {
                contract_id: o.contract_id,
                user: o.user,
                leased_asset: o.leased_asset,
                opening_date: o.opening_date,
                position_type: o.position_type,
                down_payment_amount: o.down_payment_amount,
                down_payment_asset: o.down_payment_asset,
                loan: o.loan,
                total_position_amount_lpn: o.total_position_amount_lpn,
                price: o.price,
                open: o.open,
                liquidation_price: o.liquidation_price,
            })
            .collect();
        return match query.format.as_deref() {
            Some("csv") => to_csv_response(&opened, "historically-opened.csv"),
            _ => Ok(HttpResponse::Ok().json(opened)),
        };
    }

    // Cache miss - query DB
    let data = state.database.ls_opening.get_historically_opened(skip, limit).await?;

    // Store in cache
    state.api_cache.historically_opened.set(&cache_key, data.clone()).await;

    let opened: Vec<HistoricallyOpened> = data
        .into_iter()
        .map(|o| HistoricallyOpened {
            contract_id: o.contract_id,
            user: o.user,
            leased_asset: o.leased_asset,
            opening_date: o.opening_date,
            position_type: o.position_type,
            down_payment_amount: o.down_payment_amount,
            down_payment_asset: o.down_payment_asset,
            loan: o.loan,
            total_position_amount_lpn: o.total_position_amount_lpn,
            price: o.price,
            open: o.open,
            liquidation_price: o.liquidation_price,
        })
        .collect();

    match query.format.as_deref() {
        Some("csv") => to_csv_response(&opened, "historically-opened.csv"),
        _ => Ok(HttpResponse::Ok().json(opened)),
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HistoricallyOpened {
    pub contract_id: String,
    pub user: String,
    pub leased_asset: String,
    pub opening_date: DateTime<Utc>,
    pub position_type: String,
    pub down_payment_amount: BigDecimal,
    pub down_payment_asset: String,
    pub loan: BigDecimal,
    pub total_position_amount_lpn: BigDecimal,
    pub price: Option<BigDecimal>,
    pub open: bool,
    pub liquidation_price: Option<BigDecimal>,
}
