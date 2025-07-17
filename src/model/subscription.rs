use chrono::{DateTime, Utc};
use sqlx::FromRow;

#[derive(Debug, FromRow)]
pub struct Subscription {
    pub active: Option<bool>,
    pub address: String,
    pub p256dh: String,
    pub auth: String,
    pub endpoint: String,
    pub expiration: Option<DateTime<Utc>>,
    pub ip: Option<String>,
    pub user_agent: Option<String>,
}
