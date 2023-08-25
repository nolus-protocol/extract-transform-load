use bigdecimal::BigDecimal;
use sqlx::FromRow;
use serde::{Serialize, Deserialize};

#[derive(Debug, FromRow, Deserialize, Serialize)]
pub struct Borrow_APR {
    pub APR: BigDecimal,
}
