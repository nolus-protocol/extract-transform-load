use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{types::BigDecimal, FromRow};

#[derive(Debug, FromRow, Deserialize, Serialize)]
pub struct LS_Realized_Pnl_Data {
    #[serde(rename = "Position ID")]
    #[sqlx(rename = "Position ID")]
    pub Position_Id: String,
    #[serde(rename = "Sent Amount")]
    #[sqlx(rename = "Sent Amount")]
    pub Sent_Amount: BigDecimal,
    #[serde(rename = "Sent Currency")]
    #[sqlx(rename = "Sent Currency")]
    pub Sent_Currency: String,
    #[serde(rename = "Received Amount")]
    #[sqlx(rename = "Received Amount")]
    pub Received_Amount: BigDecimal,
    #[serde(rename = "Received Currency")]
    #[sqlx(rename = "Received Currency")]
    pub Received_Currency: String,
    #[serde(rename = "Fee Amount")]
    #[sqlx(rename = "Fee Amount")]
    pub Fee_Amount: BigDecimal,
    #[serde(rename = "Fee Currency")]
    #[sqlx(rename = "Fee Currency")]
    pub Fee_Currency: String,
    pub Label: String,
    pub Description: String,
    pub TxHash: String,
    pub Date: DateTime<Utc>,
}
