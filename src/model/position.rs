use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

#[derive(Debug, Clone, FromRow, Deserialize, Serialize)]
pub struct Position {
    #[sqlx(rename = "Date")]
    pub date: String,
    #[sqlx(rename = "Type")]
    pub position_type: String,
    #[sqlx(rename = "Symbol")]
    pub symbol: String,
    #[sqlx(rename = "Contract ID")]
    pub contract_id: String,
    #[sqlx(rename = "User")]
    pub user: String,
    #[sqlx(rename = "Loan")]
    pub loan: BigDecimal,
    #[sqlx(rename = "Down Payment")]
    pub down_payment: BigDecimal,
    #[sqlx(rename = "Lease Value")]
    pub lease_value: BigDecimal,
    #[sqlx(rename = "PnL")]
    pub pnl: Option<BigDecimal>,
    #[sqlx(rename = "PnL %")]
    pub pnl_percent: Option<BigDecimal>,
    #[sqlx(rename = "Current Price")]
    pub current_price: Option<BigDecimal>,
    #[sqlx(rename = "Liquidation Price")]
    pub liquidation_price: Option<BigDecimal>,
}
