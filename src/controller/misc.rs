//! Miscellaneous API endpoints
//!
//! Utility endpoints for prices, blocks, transactions, stats, subscriptions, and notifications.

use std::{collections::HashSet, str::FromStr};

use actix_web::{get, post, web, HttpRequest, HttpResponse, Responder};
use anyhow::Context;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    configuration::{AppState, State},
    error::Error,
    handler::send_push::send,
    helpers::{Filter_Types, Status},
    model, types,
    types::{Bucket_Type, PushData, PUSH_TYPES},
};

// =============================================================================
// Prices
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct PricesQuery {
    interval: i64,
    protocol: String,
    key: String,
}

#[get("/prices")]
pub async fn prices(
    state: web::Data<AppState<State>>,
    query: web::Query<PricesQuery>,
) -> Result<impl Responder, Error> {
    let mut interval = query.interval;

    if interval > 100 {
        interval = 100;
    }

    let group = get_interval_group(interval);
    let date = Utc::now() - Duration::days(interval);

    let data = state
        .database
        .mp_asset
        .get_prices(query.key.to_owned(), query.protocol.to_owned(), date, group)
        .await?;
    let mut prices = vec![];

    for (date, price) in data.into_iter() {
        let ms = date.timestamp_millis();
        let str_price = price.to_string();
        let p = f64::from_str(&str_price)
            .context("could not parse big decimal to float")?;
        prices.push((ms, p));
    }

    Ok(web::Json(prices))
}

fn get_interval_group(interval: i64) -> i32 {
    if interval <= 7 {
        return 1;
    } else if interval > 7 && interval < 30 {
        return 5;
    }

    60
}

// =============================================================================
// Blocks
// =============================================================================

#[get("/blocks")]
pub async fn blocks(
    state: web::Data<AppState<State>>,
) -> Result<impl Responder, Error> {
    let data = state.database.block.count().await?;
    Ok(web::Json(data))
}

// =============================================================================
// Transactions
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct TxsQuery {
    skip: Option<i64>,
    limit: Option<i64>,
    filter: Option<String>,
    to: Option<String>,
    address: String,
}

#[get("/txs")]
pub async fn txs(
    state: web::Data<AppState<State>>,
    query: web::Query<TxsQuery>,
) -> Result<HttpResponse, Error> {
    let skip = query.skip.unwrap_or(0);
    let mut limit = query.limit.unwrap_or(10);

    let filters: Vec<String> = query
        .filter
        .as_deref()
        .unwrap_or("")
        .split(',')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();
    let mut to: Vec<String> = vec![];

    let mut set: HashSet<String> = HashSet::new();
    let mut combine = false;
    let filters: HashSet<String> = HashSet::from_iter(filters);
    let filters: Vec<String> = filters.into_iter().collect();

    if filters.len() > 10 {
        return Ok(HttpResponse::BadRequest().body("max filter length 10"));
    }

    if limit > 100 {
        limit = 100;
    }

    let address = query.address.to_lowercase().to_owned();

    for filter in &filters {
        let item = Filter_Types::from_str(filter)?;
        match item {
            Filter_Types::Earn => {
                // Get all active pool IDs dynamically
                to.extend(state.get_active_pool_ids());
            },
            Filter_Types::Positions => {
                let ids = state
                    .database
                    .ls_opening
                    .get_addresses(address.to_owned())
                    .await?;
                let ids: Vec<String> =
                    ids.iter().map(|item| item.0.clone()).collect();
                to.extend(ids);
            },
            Filter_Types::PositionsIds => {
                let ids: Vec<String> = query
                    .to
                    .clone()
                    .context("no contracts")?
                    .split(',')
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_string())
                    .collect();
                to.extend(ids);
            },
            _ => {},
        }
        let data: Vec<String> = item.into();
        set.extend(data);
    }

    let filters: Vec<String> = set.into_iter().collect();

    if filters.len() > 1 {
        combine = true;
    }

    let data = state
        .database
        .raw_message
        .get(address.to_owned(), skip, limit, filters, to, combine)
        .await?;

    Ok(HttpResponse::Ok().json(data))
}

// =============================================================================
// History Stats
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct HistoryStatsQuery {
    address: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct HistoryStatsResponse {
    pub pnl: f64,
    pub tx_volume: f64,
    pub win_rate: f64,
    pub bucket: Vec<Bucket_Type>,
}

#[get("/history-stats")]
pub async fn history_stats(
    state: web::Data<AppState<State>>,
    query: web::Query<HistoryStatsQuery>,
) -> Result<impl Responder, Error> {
    let address = query.address.to_lowercase().to_owned();
    let (pnl, tx_volume, win_rate, bucket) = tokio::try_join!(
        state
            .database
            .ls_loan_closing
            .get_realized_pnl(address.to_owned()),
        state.database.raw_message.get_tx_volume(address.to_owned()),
        state.database.raw_message.get_win_rate(address.to_owned()),
        state.database.raw_message.get_buckets(address.to_owned())
    )?;

    Ok(web::Json(HistoryStatsResponse {
        pnl,
        tx_volume,
        win_rate,
        bucket,
    }))
}

// =============================================================================
// Version
// =============================================================================

#[derive(Debug, Serialize, Deserialize)]
pub struct VersionResponse<'a> {
    pub version: Option<&'a str>,
}

#[get("/version")]
pub async fn version() -> Result<impl Responder, Error> {
    const VERSION: Option<&str> = option_env!("CARGO_PKG_VERSION");

    Ok(web::Json(VersionResponse { version: VERSION }))
}

// =============================================================================
// Subscribe
// =============================================================================

#[derive(Debug, Serialize, Deserialize)]
pub struct SubscribeQuery {
    address: String,
    auth: String,
    active: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SubscribeResponse {
    pub result: bool,
}

#[post("/subscribe")]
pub async fn subscribe_post(
    state: web::Data<AppState<State>>,
    subscription: web::Json<types::Subscription>,
    req: HttpRequest,
) -> Result<HttpResponse, Error> {
    let user_agent = if let Some(item) = req.headers().get("user-agent") {
        Some(item.to_str()?.to_string())
    } else {
        None
    };

    let ip = req.peer_addr().map(|item| item.ip().to_string());

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
pub async fn subscribe_get(
    state: web::Data<AppState<State>>,
    query: web::Query<SubscribeQuery>,
) -> Result<HttpResponse, Error> {
    let result = state
        .database
        .subscription
        .isExists(query.address.to_owned(), query.auth.to_owned())
        .await?;

    Ok(HttpResponse::Ok().json(SubscribeResponse { result }))
}

// =============================================================================
// Test Push
// =============================================================================

#[derive(Debug, Serialize, Deserialize)]
pub struct TestPushResponse {
    pub data: bool,
}

#[derive(Debug, Deserialize)]
pub struct TestPushQuery {
    auth: Option<String>,
    r#type: String,
    address: String,
}

#[get("/test-push")]
pub async fn test_push(
    state: web::Data<AppState<State>>,
    query: web::Query<TestPushQuery>,
) -> Result<HttpResponse, Error> {
    let auth = query.auth.to_owned().context("Auth is required")?;

    if auth != state.config.auth {
        return Ok(HttpResponse::Ok().json(TestPushResponse { data: false }));
    };

    let push_type = PUSH_TYPES::from_str(&query.r#type)?;

    let push_data = match push_type {
        PUSH_TYPES::Funding => push_funding(),
        PUSH_TYPES::FundingRecommended => push_funding_recommended(),
        PUSH_TYPES::FundNow => push_fund_now(),
        PUSH_TYPES::PartiallyLiquidated => push_partially_liquidated(),
        PUSH_TYPES::FullyLiquidated => push_fully_liquidated(),
        PUSH_TYPES::Unsupported => push_unsupported(),
    };

    send(state.as_ref().clone(), query.address.to_owned(), push_data).await?;
    Ok(HttpResponse::Ok().json(TestPushResponse { data: true }))
}

fn push_funding() -> PushData {
    PushData {
        r#type: PUSH_TYPES::Funding.to_string(),
        body: format!(
            r#"{{"level": {}, "ltv": {}, "position": "nolus1tgjl63gyrpwx6323vgeehcenwj8myvdurzfd0pskrgyrl2pqp98sx527j4"}}"#,
            1, 850
        ),
    }
}

fn push_funding_recommended() -> PushData {
    PushData {
        r#type: PUSH_TYPES::FundingRecommended.to_string(),
        body: format!(
            r#"{{"level": {}, "ltv": {}, "position": "nolus1tgjl63gyrpwx6323vgeehcenwj8myvdurzfd0pskrgyrl2pqp98sx527j4"}}"#,
            2, 865
        ),
    }
}

fn push_fund_now() -> PushData {
    PushData {
        r#type: PUSH_TYPES::FundNow.to_string(),
        body: format!(
            r#"{{"level": {}, "ltv": {}, "position": "nolus1tgjl63gyrpwx6323vgeehcenwj8myvdurzfd0pskrgyrl2pqp98sx527j4"}}"#,
            3, 865
        ),
    }
}

fn push_partially_liquidated() -> PushData {
    PushData {
        r#type: PUSH_TYPES::PartiallyLiquidated.to_string(),
        body: r#"{"position": "nolus1tgjl63gyrpwx6323vgeehcenwj8myvdurzfd0pskrgyrl2pqp98sx527j4"}"#.to_string(),
    }
}

fn push_fully_liquidated() -> PushData {
    PushData {
        r#type: PUSH_TYPES::FullyLiquidated.to_string(),
        body: r#"{"position": "nolus1tgjl63gyrpwx6323vgeehcenwj8myvdurzfd0pskrgyrl2pqp98sx527j4"}"#.to_string(),
    }
}

fn push_unsupported() -> PushData {
    PushData {
        r#type: PUSH_TYPES::Unsupported.to_string(),
        body: r#"{}"#.to_string(),
    }
}
