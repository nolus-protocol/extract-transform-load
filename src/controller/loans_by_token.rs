use actix_web::{get, web, Responder};

use crate::{
    configuration::{AppState, State},
    error::Error,
    model::TokenLoan,
};

const CACHE_KEY: &str = "loans_by_token";

#[get("/loans-by-token")]
async fn index(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    if let Some(cached) = state.api_cache.loans_by_token.get(CACHE_KEY).await {
        return Ok(web::Json(cached));
    }

    let data = state.database.ls_state.get_loans_by_token().await?;
    let loans: Vec<TokenLoan> = data
        .into_iter()
        .map(|l| TokenLoan {
            symbol: l.symbol,
            value: l.value,
        })
        .collect();

    state.api_cache.loans_by_token.set(CACHE_KEY, loans.clone()).await;

    Ok(web::Json(loans))
}
