use bigdecimal::BigDecimal;
use sqlx::FromRow;
use serde::{Serialize, Deserialize};

#[derive(Debug, FromRow, Deserialize, Serialize)]
pub struct Leased_Asset {
    #[sqlx(rename = "Loan")]
    pub loan: BigDecimal,
    #[sqlx(rename = "Asset")]
    pub asset: String
}
