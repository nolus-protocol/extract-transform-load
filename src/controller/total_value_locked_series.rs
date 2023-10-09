use crate::{
    configuration::{AppState, State},
    error::Error,
    model::TVL_Serie,
};
use actix_web::{get, web, Responder, Result};

#[get("/total-value-locked-series")]
async fn index(state: web::Data<AppState<State>>) -> Result<impl Responder, Error> {
    let total_value_locked_series: Vec<TVL_Serie> = if let Ok(item) = state.cache.lock() {
        match &item.total_value_locked_series {
            Some(data) => {
                data.to_vec()
            },
            None => vec![],
        }
    } else {
        vec![]
    };

    Ok(web::Json(total_value_locked_series))
}
