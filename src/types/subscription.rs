use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Subscription {
    pub address: String,
    pub data: Data,
}

#[derive(Debug, Deserialize)]
pub struct Data {
    pub endpoint: String,
    #[serde(alias = "expirationTime")]
    pub expiration_time: Option<i64>,
    pub keys: Keys,
}

#[derive(Debug, Deserialize)]
pub struct Keys {
    pub p256dh: String,
    pub auth: String,
}
