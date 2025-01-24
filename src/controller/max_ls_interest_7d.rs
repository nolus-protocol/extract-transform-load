use std::collections::BTreeMap;

use actix_web::{get, web, Responder};

use crate::{
    configuration::{AppState, State},
    error::Error,
};

#[get("/max_ls_interest_7d/{lpp_address}")]
async fn index(
    state: web::Data<AppState<State>>,
    lpp_address: web::Path<String>,
) -> Result<impl Responder, Error> {
    state
        .database
        .ls_opening
        .get_max_ls_interest_7d(lpp_address.into_inner())
        .await
        .map(|max_interests| {
            web::Json(
                max_interests
                    .into_iter()
                    .map(|max_interest| {
                        (
                            max_interest.date.to_string(),
                            max_interest.max_interest,
                        )
                    })
                    .collect::<BTreeMap<_, _>>(),
            )
        })
        .map_err(From::from)
}
