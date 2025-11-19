use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};
use sqlx::prelude::FromRow;

#[derive(Debug, FromRow, Deserialize, Serialize)]
pub struct Bucket_Type {
    pub bucket: String,
    pub positions: i64,
    pub share_percent: BigDecimal,
}
