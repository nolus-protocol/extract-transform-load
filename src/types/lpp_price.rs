use serde::Deserialize;
use super::Amount;

#[derive(Debug, Deserialize)]
pub struct LPP_Price {
    pub amount: Amount,
    pub amount_quote: Amount
}