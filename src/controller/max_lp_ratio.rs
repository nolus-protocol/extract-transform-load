use std::collections::BTreeMap;

use actix_web::{
    get,
    web::{Data, Json, Path},
    Responder,
};

use crate::{configuration::State, error::Error};

#[get("/max_lp_ratio/{lpp_address}")]
async fn index(
    state: Data<State>,
    lpp_address: Path<String>,
) -> Result<impl Responder, Error> {
    state
        .database
        .lp_pool_state
        .get_max_ls_interest_7d(&lpp_address)
        .await
        .map(|ratios| {
            Json(
                ratios
                    .into_iter()
                    .map(|ratio| (ratio.date, ratio.ratio))
                    .collect::<BTreeMap<_, _>>(),
            )
        })
        .map_err(From::from)
}
