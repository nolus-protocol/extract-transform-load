use actix_web::{get, web, Responder};

use crate::{
    configuration::{AppState, State},
    error::Error,
    model::DailyPositionsPoint,
};

const CACHE_KEY: &str = "daily_positions";

#[get("/daily-positions")]
async fn index(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    if let Some(cached) = state.api_cache.daily_positions.get(CACHE_KEY).await {
        return Ok(web::Json(cached));
    }

    let data = state.database.ls_opening.get_daily_opened_closed().await?;
    let series: Vec<DailyPositionsPoint> = data
        .into_iter()
        .map(|(date, closed, opened)| DailyPositionsPoint {
            date,
            closed_loans: closed,
            opened_loans: opened,
        })
        .collect();

    state.api_cache.daily_positions.set(CACHE_KEY, series.clone()).await;

    Ok(web::Json(series))
}
