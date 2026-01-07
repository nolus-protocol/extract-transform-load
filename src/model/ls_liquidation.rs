use std::{fmt, io, str::FromStr};

use chrono::{DateTime, Utc};
use sqlx::{types::BigDecimal, FromRow};

#[derive(Debug, FromRow)]
pub struct LS_Liquidation {
    pub Tx_Hash: String,
    pub LS_liquidation_height: i64,
    pub LS_liquidation_idx: Option<i32>,
    pub LS_contract_id: String,
    pub LS_amnt_symbol: String,
    pub LS_amnt_stable: BigDecimal,

    pub LS_amnt: BigDecimal,
    pub LS_payment_symbol: String,
    pub LS_payment_amnt: BigDecimal,
    pub LS_payment_amnt_stable: BigDecimal,

    pub LS_timestamp: DateTime<Utc>,
    pub LS_transaction_type: String,
    pub LS_prev_margin_stable: BigDecimal,
    pub LS_prev_interest_stable: BigDecimal,
    pub LS_current_margin_stable: BigDecimal,
    pub LS_current_interest_stable: BigDecimal,
    pub LS_principal_stable: BigDecimal,
    pub LS_loan_close: bool,
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
            _ => Err(io::Error::other(
                "LS_transactions not supported",
            )),
        }
    }
}

pub enum LS_Liquidation_Type {
    OverdueInterest,
    HighLiability,
    Unsupported,
}

impl fmt::Display for LS_Liquidation_Type {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            LS_Liquidation_Type::OverdueInterest => {
                write!(f, "overdue interest")
            },
            LS_Liquidation_Type::HighLiability => {
                write!(f, "high liability")
            },
            LS_Liquidation_Type::Unsupported => {
                write!(f, "unsupported")
            },
        }
    }
}

impl From<LS_Liquidation_Type> for String {
    fn from(value: LS_Liquidation_Type) -> Self {
        match value {
            LS_Liquidation_Type::OverdueInterest => {
                String::from("overdue interest")
            },
            LS_Liquidation_Type::HighLiability => {
                String::from("high liability")
            },
            LS_Liquidation_Type::Unsupported => String::from("unsupported"),
        }
    }
}

impl From<&str> for LS_Liquidation_Type {
    fn from(value: &str) -> LS_Liquidation_Type {
        match value {
            "overdue interest" => LS_Liquidation_Type::OverdueInterest,
            "high liability" => LS_Liquidation_Type::HighLiability,
            _ => LS_Liquidation_Type::Unsupported,
        }
    }
}
