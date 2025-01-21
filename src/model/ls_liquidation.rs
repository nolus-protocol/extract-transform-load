use chrono::{DateTime, Utc};
use sqlx::{types::BigDecimal, Database, Decode, FromRow};

use crate::custom_uint::{UInt31, UInt63};

#[derive(Debug, FromRow)]
pub struct LS_Liquidation {
    pub Tx_Hash: String,
    pub LS_liquidation_height: UInt63,
    pub LS_liquidation_idx: Option<UInt31>,
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

// #[derive(Debug)]
// pub enum LS_transactions {
//     Interest_Overdue_Liquidation,
//     Liability_Exceeded_Liquidation,
// }
//
// impl LS_transactions {
//     const INTEREST_OVERDUE_LIQUIDATION: &'static str = "0";
//
//     const LIABILITY_EXCEEDED_LIQUIDATION: &'static str = "1";
//
//     const fn as_str(&self) -> &'static str {
//         match self {
//             Self::Interest_Overdue_Liquidation => {
//                 Self::INTEREST_OVERDUE_LIQUIDATION
//             },
//             Self::Liability_Exceeded_Liquidation => {
//                 Self::LIABILITY_EXCEEDED_LIQUIDATION
//             },
//         }
//     }
// }
//
// impl fmt::Display for LS_transactions {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         f.write_str(match self {
//             Self::Interest_Overdue_Liquidation => {
//                 Self::INTEREST_OVERDUE_LIQUIDATION
//             },
//             Self::Liability_Exceeded_Liquidation => {
//                 Self::LIABILITY_EXCEEDED_LIQUIDATION
//             },
//         })
//     }
// }
//
// impl<DB> Type<DB> for LS_transactions
// where
//     DB: Database,
//     str: Type<DB>,
// {
//     fn type_info() -> DB::TypeInfo {
//         str::type_info()
//     }
// }
//
// impl<'q, DB> Encode<'q, DB> for LS_transactions
// where
//     DB: Database,
//     &'q str: Encode<'q, DB>,
// {
//     fn encode_by_ref(
//         &self,
//         buf: &mut <DB as Database>::ArgumentBuffer<'q>,
//     ) -> Result<IsNull, BoxDynError> {
//         self.as_str().encode_by_ref(buf)
//     }
// }
//
// impl<'r, DB> Decode<'r, DB> for LS_transactions
// where
//     DB: Database,
//     Cow<'r, str>: Decode<'r, DB>,
// {
//     fn decode(
//         value: <DB as Database>::ValueRef<'r>,
//     ) -> Result<Self, BoxDynError> {
//         Cow::decode(value).map(Self::from)
//     }
// }
//
// impl FromStr for LS_transactions {
//     type Err = UnknownTransactionType;
//
//     fn from_str(s: &str) -> Result<Self, Self::Err> {
//         match s {
//             Self::INTEREST_OVERDUE_LIQUIDATION => {
//                 Ok(Self::Interest_Overdue_Liquidation)
//             },
//             Self::LIABILITY_EXCEEDED_LIQUIDATION => {
//                 Ok(Self::Liability_Exceeded_Liquidation)
//             },
//             _ => Err(UnknownTransactionType),
//         }
//     }
// }
//
// #[derive(Debug, Error)]
// #[error("Unknown action type")]
// struct UnknownTransactionType;
