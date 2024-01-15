use crate::dao::get_path;
use crate::error::Error;
use crate::helpers::{formatter, parse_tuple_string, Formatter};
use crate::model::{LP_Pool, MP_Asset_Mapping, TVL_Serie};
use crate::provider::{DatabasePool, QueryApi, HTTP};
use crate::types::Currency;
use bigdecimal::BigDecimal;
use std::collections::HashMap;
use std::ops::Deref;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::{env, fs};
use urlencoding::encode;

#[derive(Debug)]
pub struct AppState<T>(Arc<T>);

impl<T> AppState<T> {
    pub fn new(state: T) -> AppState<T> {
        AppState(Arc::new(state))
    }
}

impl<T> Clone for AppState<T> {
    fn clone(&self) -> AppState<T> {
        AppState(Arc::clone(&self.0))
    }
}

impl<T> Deref for AppState<T> {
    type Target = Arc<T>;

    fn deref(&self) -> &Arc<T> {
        &self.0
    }
}

#[derive(Debug)]
pub struct Cache {
    pub total_value_locked: Option<BigDecimal>,
    pub total_value_locked_series: Option<Vec<TVL_Serie>>,
    pub r#yield: Option<BigDecimal>,
}

#[derive(Debug)]
pub struct State {
    pub config: Config,
    pub database: DatabasePool,
    pub http: HTTP,
    pub query_api: QueryApi,
    pub cache: Mutex<Cache>,
}

impl State {
    pub async fn new(
        config: Config,
        database: DatabasePool,
        http: HTTP,
        query_api: QueryApi,
    ) -> Result<State, Error> {
        Self::init_migrations(&database).await?;
        Self::init_pools(&config.lp_pools, &database).await?;
        Self::init_mp_asset_mapping(&database, &http, &config.supported_currencies).await?;

        Ok(Self {
            config,
            database,
            http,
            query_api,
            cache: Mutex::new(Cache {
                total_value_locked: None,
                total_value_locked_series: None,
                r#yield: None,
            }),
        })
    }

    async fn init_migrations(database: &DatabasePool) -> Result<(), Error> {
        let files = vec![
            "lp_pool.sql",
            "action_history.sql",
            "block.sql",
            "lp_deposit.sql",
            "lp_lender_state.sql",
            "lp_pool_state.sql",
            "lp_withdraw.sql",
            "ls_closing.sql",
            "ls_liquidation.sql",
            "ls_opening.sql",
            "ls_repayment.sql",
            "ls_state.sql",
            "mp_asset_state.sql",
            "mp_asset.sql",
            "mp_asset_mapping.sql",
            "mp_yield.sql",
            "pl_state.sql",
            "tr_profit.sql",
            "tr_rewards_distribution.sql",
            "tr_state.sql",
        ];

        let dir = env!("CARGO_MANIFEST_DIR");

        for file in files {
            let data = get_path(dir, file)?;
            sqlx::query(data.as_str()).execute(&database.pool).await?;
        }

        Ok(())
    }

    async fn init_pools(
        pools: &Vec<(String, String)>,
        database: &DatabasePool,
    ) -> Result<(), Error> {
        for (id, symbol) in pools {
            let pool = LP_Pool {
                LP_Pool_id: id.to_string(),
                LP_symbol: symbol.to_string(),
            };
            database.lp_pool.insert(pool).await?;
        }
        Ok(())
    }

    async fn init_mp_asset_mapping(
        database: &DatabasePool,
        http: &HTTP,
        supported_currencies: &Vec<Currency>,
    ) -> Result<(), Error> {
        for Currency(coinGeckoId, _address, symbol, _deicmal) in supported_currencies {
            let mp_asset_mapping = &database.mp_asset_mapping;
            let c = mp_asset_mapping.get_one(symbol.to_owned()).await?;
            if c.is_none() {
                let data = http.get_coingecko_info(coinGeckoId.to_owned()).await?;
                let item = MP_Asset_Mapping {
                    MP_asset_symbol: symbol.to_owned(),
                    MP_asset_symbol_coingecko: data.id.to_owned(),
                };
                mp_asset_mapping.insert(item).await?;
            }
        }
        Ok(())
    }

    pub async fn in_stabe(&self, currency_symbol: &str, value: &str) -> Result<BigDecimal, Error> {
        let currency = self.get_currency(currency_symbol)?;
        let Currency(_, _, symbol, _) = currency;
        let (stabe_price,) = self.database.mp_asset.get_price(symbol).await?;
        let val = self.in_stabe_calc(&stabe_price, value)?;

        Ok(val)
    }

    pub async fn in_stabe_by_pool_id(
        &self,
        pool_id: &str,
        value: &str,
    ) -> Result<BigDecimal, Error> {
        let currency = self.get_currency_by_pool_id(pool_id)?;
        let Currency(_, _, symbol, _) = currency;

        let (stabe_price,) = self.database.mp_asset.get_price(symbol).await?;
        let val = self.in_stabe_calc(&stabe_price, value)?;

        Ok(val)
    }

    pub fn in_stabe_calc(
        &self,
        stable_price: &BigDecimal,
        value: &str,
    ) -> Result<BigDecimal, Error> {
        let val = BigDecimal::from_str(value)?;
        let val = val * stable_price;
        Ok(val)
    }

    pub fn get_currency(&self, currency_symbol: &str) -> Result<&Currency, Error> {
        let currency = match self.config.hash_map_currencies.get(currency_symbol) {
            Some(c) => c,
            None => {
                return Err(Error::NotSupportedCurrency(format!(
                    "Currency {} not found",
                    currency_symbol
                )));
            }
        };

        Ok(currency)
    }

    pub fn get_currency_by_pool_id(&self, pool_id: &str) -> Result<&Currency, Error> {
        let currency = match self.config.hash_map_pool_currency.get(pool_id) {
            Some(c) => c,
            None => {
                return Err(Error::NotSupportedCurrency(format!(
                    "Pool with id {} not found",
                    pool_id
                )));
            }
        };

        Ok(currency)
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    pub host: String,
    pub websocket_host: String,
    pub database_url: String,
    pub new_block_event: String,
    pub abci_info_url: String,
    pub abci_query_url: String,
    pub block_results_event: String,
    pub sync_threads: i16,
    pub coingecko_info_url: String,
    pub coingecko_prices_url: String,
    pub coingecko_market_data_range_url: String,
    pub stable_currency: String,
    pub aggregation_interval: u8,
    pub mp_asset_interval: u8,
    pub cache_state_interval: u16,
    pub timeout: u64,
    pub supported_currencies: Vec<Currency>,
    pub lp_pools: Vec<(String, String)>,
    pub native_currency: String,
    pub hash_map_currencies: HashMap<String, Currency>,
    pub hash_map_pool_currency: HashMap<String, Currency>,
    pub treasury_contract: String,
    pub server_host: String,
    pub port: u16,
    pub allowed_origins: Vec<String>,
    pub static_dir: String,
    pub max_tasks: usize,
    pub admin_contract: String,
    pub ignore_protocols: Vec<String>,
}

impl Config {
    pub fn new_block_event(&self, id: u64) -> String {
        let event = &self.new_block_event;
        formatter(event.to_string(), &[Formatter::NumberU64(id)])
    }

    pub fn block_results_event(&self, height: i64, id: u64) -> String {
        let event = self.block_results_event.to_string();
        formatter(
            event,
            &[
                Formatter::ParsedStr(height.to_string()),
                Formatter::NumberU64(id),
            ],
        )
    }

    pub fn get_abci_info_url(&self) -> String {
        let url = &self.abci_info_url;
        let host_url = &self.host;
        formatter(url.to_string(), &[Formatter::Str(host_url.to_string())])
    }

    pub fn get_abci_query_url(&self) -> String {
        let url = &self.abci_query_url;
        let host_url = &self.host;
        formatter(url.to_string(), &[Formatter::Str(host_url.to_string())])
    }

    pub fn get_coingecko_info_url(&self, coingeckoId: String) -> String {
        let url = &self.coingecko_info_url;
        formatter(
            url.to_string(),
            &[Formatter::Str(encode(coingeckoId.as_str()).to_string())],
        )
    }

    pub fn get_coingecko_prices_url(&self, ids: &[String]) -> String {
        let url = &self.coingecko_prices_url;
        let ids = ids.join(",");
        let currency = self.stable_currency.to_owned();
        formatter(
            url.to_string(),
            &[Formatter::Str(ids), Formatter::Str(currency)],
        )
    }

    pub fn get_coingecko_market_data_range_url(&self, id: String, from: i64, to: i64) -> String {
        let url = &self.coingecko_market_data_range_url;
        formatter(
            url.to_string(),
            &[
                Formatter::Str(id),
                Formatter::Str(self.stable_currency.to_string()),
                Formatter::Number(from),
                Formatter::Number(to),
            ],
        )
    }
}

pub fn get_configuration() -> Result<Config, Error> {
    let host = env::var("HOST")?;
    let websocket_host = env::var("WEBSOCKET_HOST")?;
    let database_url = env::var("DATABASE_URL")?;
    let new_block_event = env::var("NEW_BLOCK_EVENT")?;
    let abci_info_url = env::var("ABCI_INFO_URL")?;
    let abci_query_url = env::var("ABCI_QUERY_URL")?;
    let block_results_event = env::var("BLOCK_RESULTS_EVENT")?;
    let sync_threads: i16 = env::var("SYNC_THREADS")?.parse()?;
    let coingecko_info_url = env::var("COINGECKO_INFO_URL")?;
    let coingecko_prices_url = env::var("COINGECKO_PRICES_URL")?;
    let coingecko_market_data_range_url = env::var("COINGECKO_MARKET_DATA_RANGE_URL")?;
    let stable_currency = env::var("STABLE_CURRENCY")?;
    let aggregation_interval = env::var("AGGREGATION_INTTERVAL")?.parse()?;
    let mp_asset_interval = env::var("MP_ASSET_INTERVAL_IN_MINUTES")?.parse()?;
    let cache_state_interval = env::var("CACHE_INTERVAL_IN_MINUTES")?.parse()?;
    let timeout = env::var("TIMEOUT")?.parse()?;
    let max_tasks = env::var("MAX_TASKS")?.parse()?;
    let admin_contract = env::var("ADMIN_CONTRACT")?.parse()?;
    let ignore_protocols = env::var("IGNORE_PROTOCOLS")?
        .split(',')
        .map(|item| item.to_string())
        .collect::<Vec<String>>();

    let supported_currencies = get_supported_currencies()?;
    let lp_pools = get_lp_pools()?;
    let native_currency = env::var("NATIVE_CURRENCY")?;
    let treasury_contract = env::var("TREASURY_CONTRACT")?;

    let server_host = env::var("SERVER_HOST")?;
    let port: u16 = env::var("PORT")?.parse()?;
    let allowed_origins = env::var("ALLOWED_ORIGINS")?
        .split(',')
        .map(|item| item.to_string())
        .collect::<Vec<String>>();
    let static_dir = format!(
        "{}/{}",
        env!("CARGO_MANIFEST_DIR"),
        env::var("STATIC_DIRECTORY")?
    );

    let mut hash_map_currencies: HashMap<String, Currency> = HashMap::new();
    let mut hash_map_pool_currency: HashMap<String, Currency> = HashMap::new();

    for currency in &supported_currencies {
        let c = currency.clone();
        hash_map_currencies.insert(currency.2.to_string(), c);
    }

    for pool in &lp_pools {
        if let Some(item) = hash_map_currencies.get(&pool.1) {
            let c = item.clone();
            hash_map_pool_currency.insert(pool.0.to_string(), c);
        }
    }

    let config = Config {
        host,
        websocket_host,
        database_url,
        new_block_event,
        abci_info_url,
        abci_query_url,
        block_results_event,
        sync_threads,
        coingecko_info_url,
        coingecko_prices_url,
        coingecko_market_data_range_url,
        stable_currency,
        aggregation_interval,
        mp_asset_interval,
        cache_state_interval,
        timeout,
        supported_currencies,
        lp_pools,
        native_currency,
        hash_map_currencies,
        hash_map_pool_currency,
        treasury_contract,
        server_host,
        port,
        allowed_origins,
        static_dir,
        max_tasks,
        admin_contract,
        ignore_protocols,
    };

    Ok(config)
}

pub fn set_configuration() -> Result<(), Error> {
    let config_file: &str = ".env";
    let etl_config_file: &str = "etl.conf";

    let directory = env!("CARGO_MANIFEST_DIR");
    let path = format!("{}/{}", directory, config_file);
    let etl_config_path = format!("{}/{}", directory, etl_config_file);

    let config_string = fs::read_to_string(path)?;
    let etl_config_string = fs::read_to_string(etl_config_path)?;

    parse_config_string(config_string)?;
    parse_config_string(etl_config_string)?;

    Ok(())
}

fn parse_config_string(config: String) -> Result<(), Error> {
    let params: Vec<Option<(&str, &str)>> = config
        .split('\n')
        .map(|s| {
            let element = s.find('=');
            if let Some(e) = element {
                return Some(s.split_at(e));
            }
            None
        })
        .map(|value| {
            if let Some((k, v)) = value {
                return Some((k, &v[1..]));
            }
            None
        })
        .collect();

    for (key, value) in params.into_iter().flatten() {
        let parsed_value = match key {
            "WEBSOCKET_HOST" => {
                let host = env::var("HOST")?;
                formatter(value.to_string(), &[Formatter::Str(host)])
            }
            _ => value.to_string(),
        };
        std::env::set_var(key, parsed_value);
    }

    Ok(())
}

fn get_supported_currencies() -> Result<Vec<Currency>, Error> {
    let mut data: Vec<Currency> = Vec::new();
    let supported_currencies = parse_tuple_string(env::var("SUPPORTED_CURRENCIES")?);

    for c in supported_currencies {
        let items: Vec<&str> = c.split(',').collect();
        assert_eq!(items.len(), 4);
        let chain = items[0].to_owned();
        let internal_symbl = items[1].to_owned();
        let symbol = items[2].to_owned();
        let decimal = items[3].parse()?;
        data.push(Currency(chain, internal_symbl, symbol, decimal));
    }

    Ok(data)
}

fn get_lp_pools() -> Result<Vec<(String, String)>, Error> {
    let mut data: Vec<(String, String)> = Vec::new();
    let lp_pools = parse_tuple_string(env::var("LP_POOLS")?);

    for c in lp_pools {
        let items: Vec<&str> = c.split(',').collect();
        assert_eq!(items.len(), 2);
        let internal_symbl = items[0].to_owned();
        let symbol = items[1].to_owned();
        data.push((internal_symbl, symbol));
    }

    Ok(data)
}
