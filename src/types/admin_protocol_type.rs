use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct AdminProtocolType {
    pub network: String,
    pub contracts: ProtocolContracts,
}

#[derive(Debug, Deserialize)]
pub struct AdminProtocolExtendType {
    pub network: String,
    pub protocol: String,
    pub contracts: ProtocolContracts,
}

#[derive(Debug, Deserialize)]
pub struct ProtocolContracts {
    pub leaser: String,
    pub lpp: String,
    pub oracle: String,
    pub profit: String,
}
