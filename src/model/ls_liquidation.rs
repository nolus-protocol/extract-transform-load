use std::{fmt, io, str::FromStr};

use chrono::{DateTime, Utc};
use sqlx::{types::BigDecimal, FromRow};

#[derive(Debug, FromRow)]
pub struct LS_Liquidation {
    pub LS_liquidation_height: i64,
    pub LS_liquidation_idx: Option<i32>,
    pub LS_contract_id: String,
    pub LS_symbol: String,
    pub LS_amnt_stable: BigDecimal,
    pub LS_timestamp: DateTime<Utc>,
    pub LS_transaction_type: String,
    pub LS_prev_margin_stable: BigDecimal,
    pub LS_prev_interest_stable: BigDecimal,
    pub LS_current_margin_stable: BigDecimal,
    pub LS_current_interest_stable: BigDecimal,
    pub LS_principal_stable: BigDecimal,
}

#[derive(sqlx::Type, Debug)]
pub enum LS_transactions {
    Interest_Overdue_Liquidation,
    Liability_Exceeded_Liquidation,
}

impl fmt::Display for LS_transactions {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            LS_transactions::Interest_Overdue_Liquidation => write!(f, "0"),
            LS_transactions::Liability_Exceeded_Liquidation => write!(f, "1"),
        }
    }
}

impl From<LS_transactions> for String {
    fn from(value: LS_transactions) -> Self {
        match value {
            LS_transactions::Interest_Overdue_Liquidation => String::from("0"),
            LS_transactions::Liability_Exceeded_Liquidation => {
                String::from("1")
            },
        }
    }
}

impl FromStr for LS_transactions {
    type Err = io::Error;

    fn from_str(value: &str) -> Result<LS_transactions, Self::Err> {
        match value {
            "0" => Ok(LS_transactions::Interest_Overdue_Liquidation),
            "1" => Ok(LS_transactions::Liability_Exceeded_Liquidation),
            _ => Err(io::Error::new(
                io::ErrorKind::Other,
                "LS_transactions not supported",
            )),
        }
    }
}
