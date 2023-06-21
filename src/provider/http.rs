use reqwest::get;

use crate::{
    configuration::Config,
    error::Error,
    types::{AbciBody, CoinGeckoInfo, CoinGeckoMarketData, CoinGeckoPrice},
};

#[derive(Debug)]
pub struct HTTP {
    pub config: Config,
}

impl HTTP {
    pub fn new(config: Config) -> Self {
        HTTP { config }
    }

    pub async fn get_coingecko_info(&self, chain: String, contract_id: String) -> Result<CoinGeckoInfo, Error> {
        let url = self.config.get_coingecko_info_url(chain, contract_id);
        let json = get(url).await?.json::<CoinGeckoInfo>().await?;
        Ok(json)
    }

    pub async fn get_coingecko_prices(&self, ids: &Vec<String>) -> Result<CoinGeckoPrice, Error> {
        let url = self.config.get_coingecko_prices_url(ids);
        let json = get(url).await?.json::<CoinGeckoPrice>().await?;
        Ok(json)
    }

    pub async fn get_coingecko_market_data_range(
        &self,
        id: String,
        from: i64,
        to: i64,
    ) -> Result<CoinGeckoMarketData, Error> {
        let url = self
            .config
            .get_coingecko_market_data_range_url(id, from, to);
        let json = get(url).await?.json::<CoinGeckoMarketData>().await?;
        Ok(json)
    }

    pub async fn get_latest_block(&self) -> Result<i64, Error> {
        let url = self.config.get_abci_info_url();
        let json = get(url).await?.json::<AbciBody>().await?;
        let height: i64 = json.result.response.last_block_height.parse()?;

        Ok(height)
    }
}
