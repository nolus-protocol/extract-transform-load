use chrono::{DateTime, Utc};
use sqlx::FromRow;

#[derive(Debug, FromRow)]
pub struct LS_Closing {
    pub LS_contract_id: String,
    pub LS_timestamp: DateTime<Utc>,
}
