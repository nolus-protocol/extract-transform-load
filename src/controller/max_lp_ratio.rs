use std::collections::BTreeMap;

use actix_web::{get, web, Responder};
use bigdecimal::ToPrimitive as _;

use crate::{
    configuration::{AppState, State},
    error::Error,
};

#[get("/max_lp_ratio/{lpp_address}")]
async fn index(
    state: web::Data<AppState<State>>,
    lpp_address: web::Path<String>,
) -> Result<impl Responder, Error> {
    state
        .database
        .lp_pool_state
        .get_max_ls_interest_7d(lpp_address.into_inner())
        .await
        .map(|max_lp_ratios| {
            web::Json(
                max_lp_ratios
                    .into_iter()
                    .map(|max_lp_ratio| {
                        (
                            max_lp_ratio.date.to_string(),
                            max_lp_ratio.ratio.to_f64().unwrap_or(0.0),
                        )
                    })
                    .collect::<BTreeMap<_, _>>(),
            )
        })
        .map_err(From::from)
}
