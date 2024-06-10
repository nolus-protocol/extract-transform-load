use crate::{
    configuration::{AppState, State},
    error::Error,
};
use actix_web::{get, web, HttpRequest, Responder, Result};
use serde_json::Map;

#[get("/max_ls_interest_7d/{lpp_address}")]
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
                .ls_opening
                .get_max_ls_interest_7d(data.to_owned())
                .await?;
            for item in data {
                items.insert(item.date.to_string(), item.max_interest.into());
            }
            Ok(web::Json(items))
        }
        None => Ok(web::Json(items)),
    }
}
