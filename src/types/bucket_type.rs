use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};
use sqlx::prelude::FromRow;
use utoipa::ToSchema;

#[derive(Debug, FromRow, Deserialize, Serialize, ToSchema)]
pub struct Bucket_Type {
    pub bucket: String,
    pub positions: i64,
    #[schema(value_type = String)]
    pub share_percent: BigDecimal,
}
