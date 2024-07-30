use crate::{
    configuration::{AppState, State},
    error::Error,
};
use actix_web::{get, web, HttpRequest, Responder, Result};
use bigdecimal::ToPrimitive;
use serde_json::{json, Map};

#[get("/max_lp_ratio/{lpp_address}")]
async fn index(
    state: web::Data<AppState<State>>,
    req: HttpRequest,
) -> Result<impl Responder, Error> {
    let lpp_address = req.match_info().get("lpp_address");
    let mut items = Map::new();

    match lpp_address {
        Some(data) => {
            let data = state
                .database
                .lp_pool_state
                .get_max_ls_interest_7d(data.to_owned())
                .await?;
            for item in data {
                let value = json!(item.ratio.to_f64().unwrap_or(0.0));
                items.insert(item.date.to_string(), value);
            }
            Ok(web::Json(items))
        },
        None => Ok(web::Json(items)),
    }
}
