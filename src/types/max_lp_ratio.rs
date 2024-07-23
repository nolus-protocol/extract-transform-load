use bigdecimal::BigDecimal;
use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use sqlx::prelude::FromRow;

#[derive(Debug, FromRow, Deserialize, Serialize)]
pub struct Max_LP_Ratio {
    pub date: NaiveDate,
    pub ratio: BigDecimal,
}
