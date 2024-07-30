use crate::{
    configuration::{AppState, State},
    error::Error,
};
use actix_web::{get, web, Responder, Result};
use bigdecimal::BigDecimal;
use serde::Deserialize;

#[get("/borrow-apr")]
async fn index(
    state: web::Data<AppState<State>>,
    data: web::Query<Query>,
) -> Result<impl Responder, Error> {
    let skip = data.skip.unwrap_or(0);
    let mut limit = data.limit.unwrap_or(32);

    if limit > 100 {
        limit = 100;
    }

    if let Some(protocolKey) = &data.protocol {
        let protocolKey = protocolKey.to_uppercase();
        let admin = state.protocols.get(&protocolKey);

        if let Some(protocol) = admin {
            let data = state
                .database
                .ls_opening
                .get_borrow_apr(protocol.contracts.lpp.to_owned(), skip, limit)
                .await?;
            let items: Vec<BigDecimal> =
                data.iter().map(|item| item.APR.to_owned()).collect();
            return Ok(web::Json(items));
        }
    }

    let items = vec![];
    Ok(web::Json(items))
}

#[derive(Debug, Deserialize)]
pub struct Query {
    skip: Option<i64>,
    limit: Option<i64>,
    protocol: Option<String>,
}
