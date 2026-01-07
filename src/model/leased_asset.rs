use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

#[derive(Debug, Clone, FromRow, Deserialize, Serialize)]
pub struct Leased_Asset {
    #[sqlx(rename = "Loan")]
    pub loan: BigDecimal,
    #[sqlx(rename = "Asset")]
    pub asset: String,
}
