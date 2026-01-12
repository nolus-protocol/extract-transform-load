use std::{
    collections::HashMap,
    env, fs,
    ops::Deref,
    str::FromStr,
    sync::Arc,
};

use tokio::sync::{RwLock, Semaphore};

use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use futures::future::join_all;

use crate::{
    cache::TimedCache,
    dao::postgre::{
        lp_deposit::HistoricalLender,
        lp_lender_state::CurrentLender,
        lp_pool_state::PoolUtilizationLevel,
        ls_liquidation::{HistoricallyLiquidated, LiquidationData},
        ls_opening::{HistoricallyOpened, LoanGranted, RealizedPnlWallet},
        ls_repayment::{HistoricallyRepaid, InterestRepaymentData},
        ls_state::LeaseValueStats,
    },
    error::Error,
    helpers::{formatter, parse_tuple_string, Formatter, Protocol_Types},
    model::{
        Buyback, DailyPositionsPoint, Leased_Asset, Leases_Monthly,
        LP_Pool, MonthlyActiveWallet, Position, PositionBucket, RevenueSeriesPoint,
        Supplied_Borrowed_Series, TokenLoan, TokenPosition, Utilization_Level,
    },
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

/// Cache TTL constants (in seconds)
const CACHE_TTL_STANDARD: u64 = 1800;    // 30 minutes for most endpoints
const CACHE_TTL_LONG: u64 = 3600;        // 1 hour for stable historical series

/// Unified API response cache with TTL support
#[derive(Debug)]
pub struct ApiCache {
    // Aggregates / Dashboard endpoints
    pub total_value_locked: TimedCache<BigDecimal>,
    pub total_tx_value: TimedCache<BigDecimal>,
    pub realized_pnl_stats: TimedCache<BigDecimal>,
    pub revenue: TimedCache<BigDecimal>,
    pub open_position_value: TimedCache<BigDecimal>,
    pub open_interest: TimedCache<BigDecimal>,
    pub leased_assets: TimedCache<Vec<Leased_Asset>>,
    pub supplied_funds: TimedCache<BigDecimal>,
    pub positions: TimedCache<Vec<Position>>,
    pub supplied_borrowed_history: TimedCache<Vec<Supplied_Borrowed_Series>>,
    pub leases_monthly: TimedCache<Vec<Leases_Monthly>>,
    pub current_lenders: TimedCache<Vec<CurrentLender>>,
    pub liquidations: TimedCache<Vec<LiquidationData>>,
    pub lease_value_stats: TimedCache<Vec<LeaseValueStats>>,
    pub historical_lenders: TimedCache<Vec<HistoricalLender>>,
    pub loans_granted: TimedCache<Vec<LoanGranted>>,
    pub historically_liquidated: TimedCache<Vec<HistoricallyLiquidated>>,
    pub historically_repaid: TimedCache<Vec<HistoricallyRepaid>>,
    pub historically_opened: TimedCache<Vec<HistoricallyOpened>>,
    pub interest_repayments: TimedCache<Vec<InterestRepaymentData>>,
    pub realized_pnl_wallet: TimedCache<Vec<RealizedPnlWallet>>,
    // Additional cached endpoints
    pub buyback_total: TimedCache<BigDecimal>,
    pub distributed: TimedCache<BigDecimal>,
    pub incentives_pool: TimedCache<BigDecimal>,
    pub monthly_active_wallets: TimedCache<Vec<MonthlyActiveWallet>>,
    pub revenue_series: TimedCache<Vec<RevenueSeriesPoint>>,
    pub daily_positions: TimedCache<Vec<DailyPositionsPoint>>,
    pub position_buckets: TimedCache<Vec<PositionBucket>>,
    pub loans_by_token: TimedCache<Vec<TokenLoan>>,
    pub open_positions_by_token: TimedCache<Vec<TokenPosition>>,
    pub unrealized_pnl: TimedCache<BigDecimal>,
    pub pools: TimedCache<Vec<PoolUtilizationLevel>>,
    // Period-based endpoints (new)
    pub buyback: TimedCache<Vec<Buyback>>,
    pub utilization_level: TimedCache<Vec<Utilization_Level>>,
    pub borrowed: TimedCache<BigDecimal>,
}

impl ApiCache {
    pub fn new() -> Self {
        Self {
            // 30-minute TTL endpoints (auto-refreshed)
            total_value_locked: TimedCache::new(CACHE_TTL_STANDARD),
            total_tx_value: TimedCache::new(CACHE_TTL_STANDARD),
            realized_pnl_stats: TimedCache::new(CACHE_TTL_STANDARD),
            revenue: TimedCache::new(CACHE_TTL_STANDARD),

            // 1-hour TTL endpoints (auto-refreshed)
            current_lenders: TimedCache::new(CACHE_TTL_LONG),         // /api/current-lenders - 1h
            liquidations: TimedCache::new(CACHE_TTL_LONG),            // /api/liquidations - 1h
            lease_value_stats: TimedCache::new(CACHE_TTL_LONG),       // /api/lease-value-stats - 1h
            historical_lenders: TimedCache::new(CACHE_TTL_LONG),      // /api/historical-lenders - 1h
            loans_granted: TimedCache::new(CACHE_TTL_LONG),           // /api/loans-granted - 1h
            historically_repaid: TimedCache::new(CACHE_TTL_LONG),     // /api/historically-repaid - 1h
            realized_pnl_wallet: TimedCache::new(CACHE_TTL_LONG),     // /api/realized-pnl-wallet - 1h
            daily_positions: TimedCache::new(CACHE_TTL_LONG),
            position_buckets: TimedCache::new(CACHE_TTL_LONG),
            loans_by_token: TimedCache::new(CACHE_TTL_LONG),
            open_positions_by_token: TimedCache::new(CACHE_TTL_LONG),
            historically_liquidated: TimedCache::new(CACHE_TTL_LONG),
            interest_repayments: TimedCache::new(CACHE_TTL_LONG),
            buyback_total: TimedCache::new(CACHE_TTL_LONG),
            distributed: TimedCache::new(CACHE_TTL_LONG),
            incentives_pool: TimedCache::new(CACHE_TTL_LONG),
            open_position_value: TimedCache::new(CACHE_TTL_LONG),
            open_interest: TimedCache::new(CACHE_TTL_LONG),
            supplied_funds: TimedCache::new(CACHE_TTL_LONG),
            unrealized_pnl: TimedCache::new(CACHE_TTL_LONG),
            leases_monthly: TimedCache::new(CACHE_TTL_LONG),
            monthly_active_wallets: TimedCache::new(CACHE_TTL_LONG),
            revenue_series: TimedCache::new(CACHE_TTL_LONG),
            pools: TimedCache::new(CACHE_TTL_STANDARD),  // /api/pools - 30m
            // Paginated/parameterized endpoints (lazy cache only)
            positions: TimedCache::new(CACHE_TTL_LONG),
            leased_assets: TimedCache::new(CACHE_TTL_LONG),
            supplied_borrowed_history: TimedCache::new(CACHE_TTL_LONG),
            historically_opened: TimedCache::new(CACHE_TTL_LONG),
            // Period-based endpoints
            buyback: TimedCache::new(CACHE_TTL_LONG),
            utilization_level: TimedCache::new(CACHE_TTL_STANDARD),
            borrowed: TimedCache::new(CACHE_TTL_STANDARD),
        }
    }
}

impl Default for ApiCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Maximum number of concurrent push notification tasks
const MAX_PUSH_TASKS: usize = 100;

/// Key for the latest prices cache: (symbol, protocol)
pub type PriceCacheKey = (String, String);

/// In-memory cache for the latest asset prices
/// Updated every time prices are fetched from the oracle
pub type LatestPricesCache = Arc<RwLock<HashMap<PriceCacheKey, BigDecimal>>>;

pub struct State {
    pub config: Config,
    pub database: DatabasePool,
    pub grpc: Grpc,
    pub protocols: HashMap<String, AdminProtocolExtendType>,
    pub api_cache: ApiCache,
    pub http: HTTP,
    /// Semaphore to limit concurrent push notification tasks
    pub push_permits: Arc<Semaphore>,
    /// In-memory cache for the latest asset prices (updated every price fetch cycle)
    pub latest_prices: LatestPricesCache,
}

impl std::fmt::Debug for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("State")
            .field("config", &self.config)
            .field("database", &self.database)
            .field("grpc", &self.grpc)
            .field("protocols", &self.protocols)
            .field("api_cache", &self.api_cache)
            .field("http", &self.http)
            .field("push_permits", &"<Semaphore>")
            .field("latest_prices", &"<RwLock<HashMap>>")
            .finish()
    }
}

impl State {
    pub async fn new(
        config: Config,
        database: DatabasePool,
        grpc: Grpc,
        http: HTTP,
    ) -> Result<State, Error> {
        Self::init_pools(&config.lp_pools, &database).await?;
        let protocols = Self::init_admin_protocols(&grpc, &config).await?;
        Ok(Self {
            config,
            database,
            grpc,
            http,
            protocols,
            api_cache: ApiCache::new(),
            push_permits: Arc::new(Semaphore::new(MAX_PUSH_TASKS)),
            latest_prices: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    async fn init_pools(
        pools: &Vec<(String, String, Protocol_Types, bool)>,
        database: &DatabasePool,
    ) -> Result<(), Error> {
        for (id, symbol, _, status) in pools {
            let pool = LP_Pool {
                LP_Pool_id: id.to_owned(),
                LP_symbol: symbol.to_owned(),
                LP_status: status.to_owned(),
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

    /// Get the latest price for a symbol, checking the in-memory cache first.
    /// Falls back to database if not found in cache.
    pub async fn get_cached_price(
        &self,
        symbol: &str,
        protocol: Option<String>,
    ) -> Result<BigDecimal, Error> {
        // Try to get from in-memory cache first
        if let Some(protocol_str) = &protocol {
            let cache = self.latest_prices.read().await;
            if let Some(price) = cache.get(&(symbol.to_owned(), protocol_str.clone())) {
                return Ok(price.clone());
            }
        }

        // Fall back to database
        let (price,) = self.database.mp_asset.get_price(symbol, protocol).await?;
        Ok(price)
    }

    pub async fn in_stable(
        &self,
        currency_symbol: &str,
        protocol: Option<String>,
        value: &str,
    ) -> Result<BigDecimal, Error> {
        let currency = self.get_currency(currency_symbol)?;
        let Currency(symbol, _, _) = currency;
        let stabe_price = self.get_cached_price(symbol, protocol).await?;
        let val = self.in_stable_calc(&stabe_price, value)?;

        Ok(val)
    }

    pub async fn in_stable_by_date(
        &self,
        currency_symbol: &str,
        value: &str,
        protocol: Option<String>,
        date_time: &DateTime<Utc>,
    ) -> Result<BigDecimal, Error> {
        let currency = self.get_currency(currency_symbol)?;
        let Currency(symbol, _, _) = currency;

        let (stabe_price,) = self
            .database
            .mp_asset
            .get_price_by_date(symbol, protocol, date_time)
            .await?;
        let val = self.in_stable_calc(&stabe_price, value)?;

        Ok(val)
    }

    pub async fn in_stable_by_pool_id(
        &self,
        pool_id: &str,
        value: &str,
    ) -> Result<BigDecimal, Error> {
        let currency = self.get_currency_by_pool_id(pool_id)?;
        let Currency(symbol, _, _) = currency;
        let protocol = self.get_protocol_by_pool_id(pool_id);

        let stabe_price = self.get_cached_price(symbol, protocol).await?;
        let val = self.in_stable_calc(&stabe_price, value)?;

        Ok(val)
    }

    pub fn get_protocol_by_pool_id(&self, pool_id: &str) -> Option<String> {
        let protocols = &self.protocols;
        let protocol = protocols
            .iter()
            .find(|(_protocol, data)| data.contracts.lpp == pool_id);
        protocol.map(|(protocol, _)| protocol.to_owned())
    }

    pub fn in_stable_calc(
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
    pub lp_pools: Vec<(String, String, Protocol_Types, bool)>,
    pub hash_map_lp_pools:
        HashMap<String, (String, String, Protocol_Types, bool)>,
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
    pub grpc_connections: usize,
    pub grpc_permits: usize,
    // Database pool settings (optimized for PgBouncer)
    pub db_max_connections: u32,
    pub db_min_connections: u32,
    pub db_acquire_timeout: u64,
    pub db_idle_timeout: u64,
    pub db_statement_timeout: u64,
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
        (String, String, Protocol_Types, bool),
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
    let grpc_connections = env::var("GRPC_CONNECTIONS")?.parse()?;
    let grpc_permits = env::var("GRPC_PERMITS")?.parse()?;

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

    // Database pool settings with PgBouncer-friendly defaults
    // Low connection count works well with PgBouncer's connection multiplexing
    let db_max_connections: u32 = env::var("DB_MAX_CONNECTIONS")
        .unwrap_or_else(|_| "5".to_string())
        .parse()?;
    let db_min_connections: u32 = env::var("DB_MIN_CONNECTIONS")
        .unwrap_or_else(|_| "1".to_string())
        .parse()?;
    let db_acquire_timeout: u64 = env::var("DB_ACQUIRE_TIMEOUT")
        .unwrap_or_else(|_| "30".to_string())
        .parse()?;
    let db_idle_timeout: u64 = env::var("DB_IDLE_TIMEOUT")
        .unwrap_or_else(|_| "300".to_string())
        .parse()?;
    let db_statement_timeout: u64 = env::var("DB_STATEMENT_TIMEOUT")
        .unwrap_or_else(|_| "60".to_string())
        .parse()?;

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
        grpc_connections,
        grpc_permits,
        db_max_connections,
        db_min_connections,
        db_acquire_timeout,
        db_idle_timeout,
        db_statement_timeout,
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
        assert_eq!(items.len(), 3);
        let ticker = items[0].to_owned();
        let decimal = items[1].parse()?;
        let ibc = items[2].parse::<String>()?.to_uppercase();

        data.push(Currency(ticker, decimal, ibc));
    }

    Ok(data)
}

fn get_lp_pools() -> Result<Vec<(String, String, Protocol_Types, bool)>, Error>
{
    let mut data: Vec<(String, String, Protocol_Types, bool)> = Vec::new();
    let lp_pools = parse_tuple_string(env::var("LP_POOLS")?);

    for c in lp_pools {
        let items: Vec<&str> = c.split(',').collect();
        assert_eq!(items.len(), 4);
        let internal_symbl = items[0].to_owned();
        let symbol = items[1].to_owned();
        let r#type = Protocol_Types::from_str(items[2])?;
        let status = items[3].parse()?;
        data.push((internal_symbl, symbol, r#type, status));
    }

    Ok(data)
}
