use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

#[derive(Debug, Clone, FromRow, Deserialize, Serialize)]
#[repr(transparent)]
pub struct Borrow_APR {
    pub APR: BigDecimal,
}
