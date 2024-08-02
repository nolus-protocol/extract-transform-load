use reqwest::Client;
use std::time::Duration;

use crate::{
    configuration::Config,
    error::{self, Error},
    types::{CoinGeckoInfo, CoinGeckoMarketData, CoinGeckoPrice},
};

#[derive(Debug)]
pub struct HTTP {
    pub config: Config,
    pub http: Client,
}

impl HTTP {
    pub fn new(config: Config) -> Result<HTTP, Error> {
        let http = match Client::builder()
            .timeout(Duration::from_secs(config.timeout))
            .build()
        {
            Ok(c) => c,
            Err(e) => {
                return Err(error::Error::REQWEST(e));
            },
        };

        Ok(HTTP { config, http })
    }

    pub async fn get_coingecko_info(
        &self,
        coinGeckoId: String,
    ) -> Result<CoinGeckoInfo, Error> {
        let url = self.config.get_coingecko_info_url(coinGeckoId);
        let json = self
            .http
            .get(url)
            .send()
            .await?
            .json::<CoinGeckoInfo>()
            .await?;
        Ok(json)
    }

    pub async fn get_coingecko_prices(
        &self,
        ids: &[String],
    ) -> Result<CoinGeckoPrice, Error> {
        let url = self.config.get_coingecko_prices_url(ids);
        let json = self
            .http
            .get(url)
            .send()
            .await?
            .json::<CoinGeckoPrice>()
            .await?;
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

        let json = self
            .http
            .get(url)
            .send()
            .await?
            .json::<CoinGeckoMarketData>()
            .await?;
        Ok(json)
    }
}
