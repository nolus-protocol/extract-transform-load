use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct LS_Slippage_Anomaly_Type {
    pub customer: String,
    pub lease: String,
    #[serde(alias = "lease-asset")]
    pub lease_asset: String,
    pub max_slippage: String,
}
