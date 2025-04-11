use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct LS_Auto_Close_Position_Type {
    pub to: String,
    #[serde(alias = "take-profit-ltv")]
    pub take_profit_ltv: Option<String>,
    #[serde(alias = "stop-loss-ltv")]
    pub stop_loss_ltv: Option<String>,
}
