use bigdecimal::BigDecimal;
use sqlx::FromRow;
use serde::{Serialize, Deserialize};

#[derive(Debug, FromRow, Deserialize, Serialize)]
pub struct Utilization_Level {
    #[sqlx(rename = "Utilization_Level")]
    pub utilization_level: BigDecimal,
}
