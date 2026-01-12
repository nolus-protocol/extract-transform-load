use actix_web::{get, web, Responder};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};

use crate::{
    configuration::{AppState, State},
    error::Error,
    helpers::cached_fetch,
};

const CACHE_KEY: &str = "revenue";

#[get("/revenue")]
async fn index(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    let data = cached_fetch(&state.api_cache.revenue, CACHE_KEY, || async {
        state.database.tr_profit.get_revenue().await
    })
    .await?;

    Ok(web::Json(Response { revenue: data }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub revenue: BigDecimal,
}
