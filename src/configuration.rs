use std::{
    collections::HashMap,
    env, fs,
    ops::Deref,
    str::FromStr,
    sync::{Arc, Mutex},
};

use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use futures::future::join_all;

use crate::{
    dao::get_path,
    error::Error,
    helpers::{formatter, parse_tuple_string, Formatter, Protocol_Types},
    model::LP_Pool,
    provider::{DatabasePool, Grpc, HTTP},
    types::{AdminProtocolExtendType, Currency},
};

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
}

#[derive(Debug)]
pub struct State {
    pub config: Config,
    pub database: DatabasePool,
    pub grpc: Grpc,
    pub protocols: HashMap<String, AdminProtocolExtendType>,
    pub cache: Mutex<Cache>,
    pub http: HTTP,
}

impl State {
    pub async fn new(
        config: Config,
        database: DatabasePool,
        grpc: Grpc,
        http: HTTP,
    ) -> Result<State, Error> {
        Self::init_migrations(&database).await?;
        Self::init_pools(&config.lp_pools, &database).await?;
        let protocols = Self::init_admin_protocols(&grpc, &config).await?;
        Ok(Self {
            config,
            database,
            grpc,
            http,
            protocols,
            cache: Mutex::new(Cache {
                total_value_locked: None,
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
            "ls_auto_close_position.sql",
            "ls_slippage_anomaly.sql",
            "subscription.sql",
        ];

        let dir = env!("CARGO_MANIFEST_DIR");

        for file in files {
            let data = get_path(dir, file)?;
            sqlx::query(data.as_str()).execute(&database.pool).await?;
        }

        Ok(())
    }

    async fn init_pools(
        pools: &Vec<(String, String, Protocol_Types)>,
        database: &DatabasePool,
    ) -> Result<(), Error> {
        for (id, symbol, _) in pools {
            let pool = LP_Pool {
                LP_Pool_id: id.to_owned(),
                LP_symbol: symbol.to_owned(),
            };
            database.lp_pool.insert(pool).await?;
        }
        Ok(())
    }

    async fn init_admin_protocols(
        grpc: &Grpc,
        config: &Config,
    ) -> Result<HashMap<String, AdminProtocolExtendType>, Error> {
        let protocols = grpc
            .get_admin_config(config.admin_contract.to_owned())
            .await?;
        let mut joins = vec![];
        let mut protocolsMap =
            HashMap::<String, AdminProtocolExtendType>::new();

        for p in protocols {
            if !config.ignore_protocols.contains(&p) {
                joins.push(
                    grpc.get_protocol_config(
                        config.admin_contract.to_owned(),
                        p,
                    ),
                )
            }
        }

        let result = join_all(joins).await;

        for item in result.into_iter().flatten() {
            protocolsMap.insert(item.protocol.to_owned(), item);
        }

        Ok(protocolsMap)
    }

    pub async fn in_stabe(
        &self,
        currency_symbol: &str,
        protocol: Option<String>,
        value: &str,
    ) -> Result<BigDecimal, Error> {
        let currency = self.get_currency(currency_symbol)?;
        let Currency(symbol, _) = currency;
        let (stabe_price,) =
            self.database.mp_asset.get_price(symbol, protocol).await?;
        let val = self.in_stabe_calc(&stabe_price, value)?;

        Ok(val)
    }

    pub async fn in_stabe_by_date(
        &self,
        currency_symbol: &str,
        value: &str,
        protocol: Option<String>,
        date_time: &DateTime<Utc>,
    ) -> Result<BigDecimal, Error> {
        let currency = self.get_currency(currency_symbol)?;
        let Currency(symbol, _) = currency;

        let (stabe_price,) = self
            .database
            .mp_asset
            .get_price_by_date(symbol, protocol, date_time)
            .await?;
        let val = self.in_stabe_calc(&stabe_price, value)?;

        Ok(val)
    }

    pub async fn in_stabe_by_pool_id(
        &self,
        pool_id: &str,
        value: &str,
    ) -> Result<BigDecimal, Error> {
        let currency = self.get_currency_by_pool_id(pool_id)?;
        let Currency(symbol, _) = currency;
        let protocol = self.get_protocol_by_pool_id(pool_id);

        let (stabe_price,) =
            self.database.mp_asset.get_price(symbol, protocol).await?;
        let val = self.in_stabe_calc(&stabe_price, value)?;

        Ok(val)
    }

    pub fn get_protocol_by_pool_id(&self, pool_id: &str) -> Option<String> {
        let protocols = &self.protocols;
        let protocol = protocols
            .iter()
            .find(|(_protocol, data)| data.contracts.lpp == pool_id);
        protocol.map(|(protocol, _)| protocol.to_owned())
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

    pub fn get_currency(
        &self,
        currency_symbol: &str,
    ) -> Result<&Currency, Error> {
        let currency =
            match self.config.hash_map_currencies.get(currency_symbol) {
                Some(c) => c,
                None => {
                    return Err(Error::NotSupportedCurrency(format!(
                        "Currency {} not found",
                        currency_symbol
                    )));
                },
            };

        Ok(currency)
    }

    pub fn get_currency_by_pool_id(
        &self,
        pool_id: &str,
    ) -> Result<&Currency, Error> {
        let currency = match self.config.hash_map_pool_currency.get(pool_id) {
            Some(c) => c,
            None => {
                return Err(Error::NotSupportedCurrency(format!(
                    "Pool with id {} not found",
                    pool_id
                )));
            },
        };

        Ok(currency)
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
    pub max_tasks: usize,
    pub admin_contract: String,
    pub ignore_protocols: Vec<String>,
    pub initial_protocol: String,
    pub socket_reconnect_interval: u64,
    pub grpc_host: String,
    pub events_subscribe: Vec<String>,
    pub enable_sync: bool,
    pub tasks_interval: u64,
    pub status_code_to_delete: Vec<u16>,
    pub mail_to: String,
    pub vapid_private_key: Vec<u8>,
    pub vapid_public_key: Vec<u8>,
    pub auth: String,
}

impl Config {}

fn parse_config_vapid_keys() -> Result<(Vec<u8>, Vec<u8>), Error> {
    let directory = env!("CARGO_MANIFEST_DIR");
    let private_key_dir = format!("{}/cert/vapid_private.pem", directory);
    let public_key_dir = format!("{}/cert/vapid_public.b64", directory);

    let private_key = fs::read(private_key_dir)?;
    let public_key = fs::read(public_key_dir)?;

    Ok((private_key, public_key))
}

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
        "{}/{}",
        env!("CARGO_MANIFEST_DIR"),
        env::var("STATIC_DIRECTORY")?
    );
    let enable_sync = env::var("ENABLE_SYNC")?.parse()?;
    let tasks_interval: u64 = env::var("TASKS_INTERVAL")?.parse()?;

    let mut hash_map_currencies: HashMap<String, Currency> = HashMap::new();
    let mut hash_map_pool_currency: HashMap<String, Currency> = HashMap::new();

    for currency in &supported_currencies {
        let c = currency.clone();
        hash_map_currencies.insert(currency.0.to_owned(), c);
    }

    for pool in &lp_pools {
        if let Some(item) = hash_map_currencies.get(&pool.1) {
            let c = item.clone();
            hash_map_pool_currency.insert(pool.0.to_owned(), c);
        }
        hash_map_lp_pools.insert(pool.0.to_owned(), pool.clone());
    }

    let events_subscribe = events_subscribe
        .split(',')
        .map(|item| item.to_owned())
        .collect();

    let codes = env::var("STATUS_COODE_TO_DELETE")?
        .split(",")
        .map(|item| item.to_string())
        .collect::<Vec<String>>();
    let mut status_code_to_delete = vec![];
    let mail_to: String = env::var("MAIL_TO")?;

    for code in codes {
        status_code_to_delete.push(code.parse::<u16>()?);
    }

    let (vapid_private_key, vapid_public_key) = parse_config_vapid_keys()?;
    let auth = env::var("AUTH")?.parse()?;

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
        tasks_interval,
        status_code_to_delete,
        mail_to,
        vapid_private_key,
        vapid_public_key,
        auth,
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
        data.push(Currency(ticker, decimal));
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
