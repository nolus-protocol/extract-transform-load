use std::{
    collections::HashMap,
    env, fs,
    num::NonZeroUsize,
    str::FromStr,
    sync::{Arc, RwLock},
};

use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};

use crate::{
    dao::get_path,
    error::Error,
    helpers::{formatter, parse_tuple_string, Formatter, Protocol_Types},
    model::LP_Pool,
    provider::{DatabasePool, Grpc},
    try_join,
    types::{AdminProtocolExtendType, Currency},
};

#[derive(Debug)]
pub struct Cache {
    pub total_value_locked: Option<BigDecimal>,
}

#[derive(Debug)]
pub struct State {
    pub config: Config,
    pub database: DatabasePool,
    pub grpc: Grpc,
    pub protocols: HashMap<String, AdminProtocolExtendType>,
    pub cache: RwLock<Cache>,
}

impl State {
    pub async fn new(
        config: Config,
        database: DatabasePool,
        grpc: Grpc,
    ) -> Result<State, Error> {
        Self::init_migrations(&database).await?;

        Self::init_pools(&config.lp_pools, &database).await?;

        Self::init_admin_protocols(&grpc, &config)
            .await
            .map(|protocols| Self {
                config,
                database,
                grpc,
                protocols,
                cache: RwLock::new(Cache {
                    total_value_locked: None,
                }),
            })
    }

    async fn init_migrations(database: &DatabasePool) -> Result<(), Error> {
        const FILES: [&str; 23] = [
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
            "mp_asset.sql",
            "mp_yield.sql",
            "pl_state.sql",
            "tr_profit.sql",
            "tr_rewards_distribution.sql",
            "tr_state.sql",
            "ls_close_position.sql",
            "raw_message.sql",
            "ls_liquidation_warning.sql",
            "reserve_cover_loss.sql",
            "ls_loan_closing.sql",
        ];

        let dir = env!("CARGO_MANIFEST_DIR");

        for file in FILES {
            sqlx::query(get_path(dir, file)?.as_str())
                .execute(&database.pool)
                .await?;
        }

        Ok(())
    }

    async fn init_pools(
        pools: &[(String, String, Protocol_Types)],
        database: &DatabasePool,
    ) -> Result<(), Error> {
        for (id, symbol, _) in pools {
            database
                .lp_pool
                .insert(&LP_Pool {
                    LP_Pool_id: id.to_owned(),
                    LP_symbol: symbol.to_owned(),
                })
                .await?;
        }

        Ok(())
    }

    async fn init_admin_protocols(
        grpc: &Grpc,
        config: &Config,
    ) -> Result<HashMap<String, AdminProtocolExtendType>, Error> {
        try_join(
            grpc.get_admin_config(config.admin_contract.clone())
                .await?
                .into_iter()
                .filter_map(|protocol| {
                    (!config.ignore_protocols.contains(&protocol)).then(|| {
                        let wasm_query_client = grpc.wasm_query_client.clone();

                        let admin_contract = config.admin_contract.clone();

                        async move {
                            Grpc::get_protocol_config(
                                wasm_query_client,
                                admin_contract,
                                &protocol,
                            )
                            .await
                            .map(|item| (item.protocol.clone(), item))
                        }
                    })
                }),
        )
        .await
    }

    pub async fn in_stabe(
        &self,
        currency_symbol: &str,
        protocol: Option<&str>,
        value: BigDecimal,
    ) -> Result<BigDecimal, Error> {
        let Currency {
            denominator: symbol,
            exponent: _,
        } = self.get_currency(currency_symbol)?;

        self.database
            .mp_asset
            .get_price(symbol, protocol)
            .await
            .map(|stabe_price| value * stabe_price)
            .map_err(From::from)
    }

    pub async fn in_stabe_by_date(
        &self,
        currency_symbol: &str,
        value: BigDecimal,
        protocol: Option<&str>,
        date_time: DateTime<Utc>,
    ) -> Result<BigDecimal, Error> {
        let Currency {
            denominator: symbol,
            exponent: _,
        } = self.get_currency(currency_symbol)?;

        self.database
            .mp_asset
            .get_price_by_date(symbol, protocol, date_time)
            .await
            .map(|stabe_price| value * stabe_price)
            .map_err(From::from)
    }

    pub async fn in_stabe_by_pool_id(
        &self,
        pool_id: &str,
        value: &BigDecimal,
    ) -> Result<BigDecimal, Error> {
        let Currency {
            denominator: symbol,
            exponent: _,
        } = self.get_currency_by_pool_id(pool_id)?;

        self.database
            .mp_asset
            .get_price(symbol, self.get_protocol_by_pool_id(pool_id))
            .await
            .map(|stable_price| stable_price * value)
            .map_err(From::from)
    }

    pub fn get_protocol_by_pool_id<'r>(
        &self,
        pool_id: &str,
    ) -> Option<&'r str> {
        self.protocols.iter().find_map(|(protocol, data)| {
            (data.contracts.lpp == pool_id).then(|| &**protocol)
        })
    }

    pub fn get_currency(
        &self,
        currency_symbol: &str,
    ) -> Result<&Currency, Error> {
        self.config
            .hash_map_currencies
            .get(currency_symbol)
            .ok_or_else(|| {
                Error::NotSupportedCurrency(format!(
                    "Currency {} not found",
                    currency_symbol
                ))
            })
    }

    pub fn get_currency_by_pool_id(
        &self,
        pool_id: &str,
    ) -> Result<&Currency, Error> {
        self.config
            .hash_map_pool_currency
            .get(pool_id)
            .ok_or_else(|| {
                Error::NotSupportedCurrency(format!(
                    "Pool with id {} not found",
                    pool_id
                ))
            })
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    pub host: String,
    pub websocket_host: String,
    pub database_url: String,
    pub sync_threads: i16,
    pub aggregation_interval: u8,
    pub mp_asset_interval: u8,
    pub cache_state_interval: u16,
    pub timeout: u64,
    pub supported_currencies: Vec<Currency>,
    pub lp_pools: Vec<(String, String, Protocol_Types)>,
    pub hash_map_lp_pools: HashMap<String, (String, String, Protocol_Types)>,
    pub native_currency: String,
    pub hash_map_currencies: HashMap<String, Currency>,
    pub hash_map_pool_currency: HashMap<String, Currency>,
    pub treasury_contract: String,
    pub server_host: String,
    pub port: u16,
    pub allowed_origins: Vec<String>,
    pub static_dir: String,
    pub max_tasks: NonZeroUsize,
    pub admin_contract: String,
    pub ignore_protocols: Vec<String>,
    pub initial_protocol: String,
    pub socket_reconnect_interval: u64,
    pub grpc_host: String,
    pub events_subscribe: Vec<String>,
    pub enable_sync: bool,
}

impl Config {}

pub fn get_configuration() -> Result<Config, Error> {
    let host = env::var("HOST")?;
    let websocket_host = env::var("WEBSOCKET_HOST")?;
    let database_url = env::var("DATABASE_URL")?;
    let sync_threads: i16 = env::var("SYNC_THREADS")?.parse()?;
    let aggregation_interval = env::var("AGGREGATION_INTTERVAL")?.parse()?;
    let mp_asset_interval = env::var("MP_ASSET_INTERVAL_IN_SEC")?.parse()?;
    let cache_state_interval =
        env::var("CACHE_INTERVAL_IN_MINUTES")?.parse()?;
    let timeout = env::var("TIMEOUT")?.parse()?;
    let max_tasks = env::var("MAX_TASKS")?.parse()?;
    let admin_contract = env::var("ADMIN_CONTRACT")?.parse()?;
    let socket_reconnect_interval =
        env::var("SOCKET_RECONNECT_INTERVAL")?.parse()?;
    let grpc_host = env::var("GRPC_HOST")?.parse()?;
    let events_subscribe: String = env::var("EVENTS_SUBSCRIBE")?.parse()?;

    let ignore_protocols = env::var("IGNORE_PROTOCOLS")?
        .split(',')
        .map(|item| item.to_owned())
        .collect::<Vec<String>>();

    let initial_protocol = env::var("INITIAL_PROTOCOL")?.parse()?;

    let supported_currencies = get_supported_currencies()?;
    let lp_pools = get_lp_pools()?;
    let mut hash_map_lp_pools: HashMap<
        String,
        (String, String, Protocol_Types),
    > = HashMap::new();

    let native_currency = env::var("NATIVE_CURRENCY")?;
    let treasury_contract = env::var("TREASURY_CONTRACT")?;

    let server_host = env::var("SERVER_HOST")?;
    let port: u16 = env::var("PORT")?.parse()?;
    let allowed_origins = env::var("ALLOWED_ORIGINS")?
        .split(',')
        .map(|item| item.to_owned())
        .collect::<Vec<String>>();
    let static_dir = format!(
        concat!(env!("CARGO_MANIFEST_DIR"), "/{}"),
        env::var("STATIC_DIRECTORY")?
    );
    let enable_sync = env::var("ENABLE_SYNC")?.parse()?;
    let mut hash_map_currencies: HashMap<String, Currency> = HashMap::new();
    let mut hash_map_pool_currency: HashMap<String, Currency> = HashMap::new();

    for currency in &supported_currencies {
        hash_map_currencies
            .insert(currency.denominator.to_owned(), currency.clone());
    }

    for pool in &lp_pools {
        if let Some(item) = hash_map_currencies.get(&pool.1) {
            hash_map_pool_currency.insert(pool.0.to_owned(), item.clone());
        }
        hash_map_lp_pools.insert(pool.0.to_owned(), pool.clone());
    }

    let events_subscribe = events_subscribe
        .split(',')
        .map(|item| item.to_owned())
        .collect();

    let config = Config {
        host,
        websocket_host,
        database_url,
        sync_threads,
        aggregation_interval,
        mp_asset_interval,
        cache_state_interval,
        timeout,
        supported_currencies,
        lp_pools,
        hash_map_lp_pools,
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
        initial_protocol,
        socket_reconnect_interval,
        grpc_host,
        events_subscribe,
        enable_sync,
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
    for (key, value) in config.split('\n').filter_map(|s| s.split_once('=')) {
        let parsed_value = match key {
            "WEBSOCKET_HOST" => {
                let host = env::var("HOST")?;
                formatter(value.to_owned(), &[Formatter::Str(host)])
            },
            _ => value.to_owned(),
        };
        std::env::set_var(key, parsed_value);
    }

    Ok(())
}

fn get_supported_currencies() -> Result<Vec<Currency>, Error> {
    let mut data: Vec<Currency> = Vec::new();
    let supported_currencies =
        parse_tuple_string(env::var("SUPPORTED_CURRENCIES")?);

    for c in supported_currencies {
        let items: Vec<&str> = c.split(',').collect();
        assert_eq!(items.len(), 2);
        let ticker = items[0].to_owned();
        let decimal = items[1].parse()?;
        data.push(Currency {
            denominator: ticker,
            exponent: decimal,
        });
    }

    Ok(data)
}

fn get_lp_pools() -> Result<Vec<(String, String, Protocol_Types)>, Error> {
    let mut data: Vec<(String, String, Protocol_Types)> = Vec::new();
    let lp_pools = parse_tuple_string(env::var("LP_POOLS")?);

    for c in lp_pools {
        let items: Vec<&str> = c.split(',').collect();
        assert_eq!(items.len(), 3);
        let internal_symbl = items[0].to_owned();
        let symbol = items[1].to_owned();
        let r#type = Protocol_Types::from_str(&items[2])?;
        data.push((internal_symbl, symbol, r#type));
    }

    Ok(data)
}
