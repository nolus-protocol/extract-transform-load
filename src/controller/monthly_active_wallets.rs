use actix_web::{get, web, Responder};

use crate::{
    configuration::{AppState, State},
    error::Error,
    model::MonthlyActiveWallet,
};

const CACHE_KEY: &str = "monthly_active_wallets";

#[get("/monthly-active-wallets")]
async fn index(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    if let Some(cached) = state.api_cache.monthly_active_wallets.get(CACHE_KEY).await {
        return Ok(web::Json(cached));
    }

    let data = state
        .database
        .ls_opening
        .get_monthly_active_wallets()
        .await?;
    let wallets: Vec<MonthlyActiveWallet> = data
        .into_iter()
        .map(|w| MonthlyActiveWallet {
            month: w.month,
            unique_addresses: w.unique_addresses,
        })
        .collect();

    state.api_cache.monthly_active_wallets.set(CACHE_KEY, wallets.clone()).await;

    Ok(web::Json(wallets))
}
