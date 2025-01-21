use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use sqlx::prelude::FromRow;

use crate::custom_uint::UInt15;

#[derive(Debug, FromRow, Deserialize, Serialize)]
pub struct LS_Max_Interest {
    pub date: NaiveDate,
    pub max_interest: UInt15,
}
