use actix_web::{get, web, Responder};

use crate::{
    configuration::{AppState, State},
    error::Error,
    model::RevenueSeriesPoint,
};

const CACHE_KEY: &str = "revenue_series";

#[get("/revenue-series")]
async fn index(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    if let Some(cached) = state.api_cache.revenue_series.get(CACHE_KEY).await {
        return Ok(web::Json(cached));
    }

    let data = state.database.tr_profit.get_revenue_series().await?;
    let series: Vec<RevenueSeriesPoint> = data
        .into_iter()
        .map(|(time, daily, cumulative)| RevenueSeriesPoint {
            time,
            daily,
            cumulative,
        })
        .collect();

    state.api_cache.revenue_series.set(CACHE_KEY, series.clone()).await;

    Ok(web::Json(series))
}
