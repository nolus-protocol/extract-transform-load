use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Reserve_Cover_Loss_Type {
    pub to: String,
    #[serde(alias = "payment-symbol")]
    pub payment_symbol: String,
    #[serde(alias = "payment-amount")]
    pub payment_amount: String,
}
