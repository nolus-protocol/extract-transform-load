use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use sqlx::prelude::FromRow;

#[derive(Debug, FromRow, Deserialize, Serialize)]
pub struct LS_Max_Interest {
    pub date: NaiveDate,
    pub max_interest: i16,
}
