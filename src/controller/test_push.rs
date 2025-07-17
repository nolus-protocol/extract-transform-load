use std::str::FromStr;

use crate::{
    configuration::{AppState, State},
    error::Error,
    handler::send_push::send,
    types::{PushData, PUSH_TYPES},
};
use actix_web::{get, web, HttpResponse, Result};
use anyhow::Context;
use serde::{Deserialize, Serialize};

#[get("/test-push")]
pub async fn index(
    state: web::Data<AppState<State>>,
    data: web::Query<Query>,
) -> Result<HttpResponse, Error> {
    let auth = data.auth.to_owned().context("Auth is required")?;

    if auth != state.config.auth {
        return Ok(HttpResponse::Ok().json(Response { data: false }));
    };

    let push_type = PUSH_TYPES::from_str(&data.r#type)?;

    let push_data = match push_type {
        PUSH_TYPES::Funding => funding(),
        PUSH_TYPES::FundingRecommended => funding_recomented(),
        PUSH_TYPES::FundNow => fund_now(),
        PUSH_TYPES::PartiallyLiquidated => partially_liquidated(),
        PUSH_TYPES::FullyLiquidated => fully_liquidated(),
        PUSH_TYPES::Unsupported => unsupported(),
    };

    send(state.as_ref().clone(), data.address.to_owned(), push_data).await?;
    Ok(HttpResponse::Ok().json(Response {
        data: true,
        version: String::from("1.0.0"),
    }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub data: bool,
    pub version: String,
}

#[derive(Debug, Deserialize)]
pub struct Query {
    auth: Option<String>,
    r#type: String,
    address: String,
}

pub fn funding() -> PushData {
    PushData {
        r#type: PUSH_TYPES::Funding.to_string(),
        body: format!(
            r#"{{"level": {}, "ltv": {}, "position": "nolus1tgjl63gyrpwx6323vgeehcenwj8myvdurzfd0pskrgyrl2pqp98sx527j4"}}"#,
            1, 850
        ),
    }
}

pub fn funding_recomented() -> PushData {
    PushData {
        r#type: PUSH_TYPES::FundingRecommended.to_string(),
        body: format!(
            r#"{{"level": {}, "ltv": {}, "position": "nolus1tgjl63gyrpwx6323vgeehcenwj8myvdurzfd0pskrgyrl2pqp98sx527j4"}}"#,
            2, 865
        ),
    }
}

pub fn fund_now() -> PushData {
    PushData {
        r#type: PUSH_TYPES::FundNow.to_string(),
        body: format!(
            r#"{{"level": {}, "ltv": {}, "position": "nolus1tgjl63gyrpwx6323vgeehcenwj8myvdurzfd0pskrgyrl2pqp98sx527j4"}}"#,
            3, 865
        ),
    }
}

pub fn partially_liquidated() -> PushData {
    PushData {
        r#type: PUSH_TYPES::PartiallyLiquidated.to_string(),
        body: format!(
            r#"{{"position": "nolus1tgjl63gyrpwx6323vgeehcenwj8myvdurzfd0pskrgyrl2pqp98sx527j4"}}"#,
        ),
    }
}

pub fn fully_liquidated() -> PushData {
    PushData {
        r#type: PUSH_TYPES::FullyLiquidated.to_string(),
        body: format!(
            r#"{{"position": "nolus1tgjl63gyrpwx6323vgeehcenwj8myvdurzfd0pskrgyrl2pqp98sx527j4"}}"#
        ),
    }
}

pub fn unsupported() -> PushData {
    PushData {
        r#type: PUSH_TYPES::Unsupported.to_string(),
        body: format!(r#"{{}}"#),
    }
}
