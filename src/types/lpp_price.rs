use serde::Deserialize;
use super::AmountSymbol;

#[derive(Debug, Deserialize)]
pub struct LPP_Price {
    pub amount: AmountSymbol,
    pub amount_quote: AmountSymbol
}