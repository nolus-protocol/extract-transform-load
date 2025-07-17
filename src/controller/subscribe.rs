use crate::{
    configuration::{AppState, State},
    error::Error,
    helpers::Status,
    model, types,
};
use actix_web::{get, post, web, HttpRequest, HttpResponse, Result};
use chrono::DateTime;
use serde::{Deserialize, Serialize};

#[post("/subscribe")]
pub async fn post_index(
    state: web::Data<AppState<State>>,
    subscription: web::Json<types::Subscription>,
    req: HttpRequest,
) -> Result<HttpResponse, Error> {
    let user_agent = if let Some(item) = req.headers().get("user-agent") {
        Some(item.to_str()?.to_string())
    } else {
        None
    };

    let ip = if let Some(item) = req.peer_addr() {
        Some(item.ip().to_string())
    } else {
        None
    };

    let expiration = if let Some(ms) = subscription.data.expiration_time {
        let sec = ms / 1000;

        let at = DateTime::from_timestamp(sec, 0).ok_or_else(|| {
            Error::DecodeDateTimeError(format!(
                "Subscription date parse {}",
                sec
            ))
        })?;

        Some(at)
    } else {
        None
    };

    let data = model::Subscription {
        active: None,
        address: subscription.address.to_owned(),
        endpoint: subscription.data.endpoint.to_owned(),
        auth: subscription.data.keys.auth.to_owned(),
        p256dh: subscription.data.keys.p256dh.to_owned(),
        ip,
        user_agent,
        expiration,
    };

    let (_, item) = tokio::try_join!(
        state
            .database
            .subscription
            .deactivate_by_auth_and_ne_address(
                subscription.address.to_owned(),
                subscription.data.keys.auth.to_owned(),
            ),
        state.database.subscription.get_one(
            subscription.address.to_owned(),
            subscription.data.keys.auth.to_owned(),
        )
    )?;

    if let Some(sub) = item {
        let active = !sub.active.unwrap_or(false);
        state
            .database
            .subscription
            .update(
                active,
                subscription.address.to_owned(),
                subscription.data.keys.auth.to_owned(),
            )
            .await?;

        let b = if active {
            String::from(Status::Subscribed)
        } else {
            String::from(Status::Unsubscribed)
        };

        return Ok(HttpResponse::Ok().body(b));
    }

    state.database.subscription.insert(data).await?;

    Ok(HttpResponse::Ok().body(String::from(Status::Subscribed)))
}

#[get("/subscribe")]
pub async fn get_index(
    state: web::Data<AppState<State>>,
    data: web::Query<Query>,
) -> Result<HttpResponse, Error> {
    let result = state
        .database
        .subscription
        .isExists(data.address.to_owned(), data.auth.to_owned())
        .await?;

    Ok(HttpResponse::Ok().json(Response { result }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Query {
    address: String,
    auth: String,
    active: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub result: bool,
}
