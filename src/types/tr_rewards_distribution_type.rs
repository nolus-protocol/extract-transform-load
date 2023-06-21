use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct TR_Rewards_Distribution_Type {
    pub height: String,
    pub to: String,
    pub at: String,
    #[serde(alias = "rewards-symbol")]
    pub rewards_symbol: String,
    #[serde(alias = "rewards-amount")]
    pub rewards_amount: String,
}
