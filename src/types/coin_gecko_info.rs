use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct CoinGeckoInfo {
    pub id: String,
}
