use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

#[derive(Debug, FromRow, Deserialize, Serialize)]
pub struct Utilization_Level {
    #[sqlx(rename = "Utilization_Level")]
    pub utilization_level: BigDecimal,
}
