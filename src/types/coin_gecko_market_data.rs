use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct CoinGeckoMarketData {
    pub prices: Vec<MarketData>,
    pub market_caps: Vec<MarketData>,
    pub total_volumes: Vec<MarketData>,
}

#[derive(Deserialize, Debug)]
pub struct MarketData(pub i64, pub f64);
