use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct LS_Liquidation_Warning_Type {
    pub customer: String,
    pub lease: String,
    #[serde(alias = "lease-asset")]
    pub lease_asset: String,
    pub level: String,
    pub ltv: String,
}
