use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct LP_Lender_State_Type {
    pub balance: String,
    pub price: String,
}
