use super::Amount;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct LPP_Price {
    pub amount: Amount,
    pub amount_quote: Amount,
}
