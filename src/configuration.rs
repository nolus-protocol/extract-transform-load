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
    helpers::{formatter, Formatter},
    model::{
        Buyback, DailyPositionsPoint, Leased_Asset, Leases_Monthly, LP_Pool,
        MonthlyActiveWallet, Position, PositionBucket, ProtocolRegistry,
        RevenueSeriesPoint, Supplied_Borrowed_Series, TokenLoan, TokenPosition,
        TvlPoolParams, Utilization_Level,
    },
    provider::{DatabasePool, Grpc, HTTP},
    types::{AdminProtocolExtendType, Currency, ProtocolContracts},
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
const CACHE_TTL_STANDARD: u64 = 300;     // 5 minutes for all endpoints

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

            // All endpoints use 5-minute TTL
            current_lenders: TimedCache::new(CACHE_TTL_STANDARD),
            liquidations: TimedCache::new(CACHE_TTL_STANDARD),
            lease_value_stats: TimedCache::new(CACHE_TTL_STANDARD),
            historical_lenders: TimedCache::new(CACHE_TTL_STANDARD),
            loans_granted: TimedCache::new(CACHE_TTL_STANDARD),
            historically_repaid: TimedCache::new(CACHE_TTL_STANDARD),
            realized_pnl_wallet: TimedCache::new(CACHE_TTL_STANDARD),
            daily_positions: TimedCache::new(CACHE_TTL_STANDARD),
            position_buckets: TimedCache::new(CACHE_TTL_STANDARD),
            loans_by_token: TimedCache::new(CACHE_TTL_STANDARD),
            open_positions_by_token: TimedCache::new(CACHE_TTL_STANDARD),
            historically_liquidated: TimedCache::new(CACHE_TTL_STANDARD),
            interest_repayments: TimedCache::new(CACHE_TTL_STANDARD),
            buyback_total: TimedCache::new(CACHE_TTL_STANDARD),
            distributed: TimedCache::new(CACHE_TTL_STANDARD),
            incentives_pool: TimedCache::new(CACHE_TTL_STANDARD),
            open_position_value: TimedCache::new(CACHE_TTL_STANDARD),
            open_interest: TimedCache::new(CACHE_TTL_STANDARD),
            supplied_funds: TimedCache::new(CACHE_TTL_STANDARD),
            unrealized_pnl: TimedCache::new(CACHE_TTL_STANDARD),
            leases_monthly: TimedCache::new(CACHE_TTL_STANDARD),
            monthly_active_wallets: TimedCache::new(CACHE_TTL_STANDARD),
            revenue_series: TimedCache::new(CACHE_TTL_STANDARD),
            pools: TimedCache::new(CACHE_TTL_STANDARD),
            positions: TimedCache::new(CACHE_TTL_STANDARD),
            leased_assets: TimedCache::new(CACHE_TTL_STANDARD),
            supplied_borrowed_history: TimedCache::new(CACHE_TTL_STANDARD),
            historically_opened: TimedCache::new(CACHE_TTL_STANDARD),
            buyback: TimedCache::new(CACHE_TTL_STANDARD),
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
    /// Active protocols only - used for price fetching
    pub protocols: HashMap<String, AdminProtocolExtendType>,
    /// All protocols (active + deprecated) - pool_id -> protocol_name mapping
    pub hash_map_pool_protocol: HashMap<String, String>,
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
            .field("hash_map_pool_protocol", &self.hash_map_pool_protocol)
            .field("api_cache", &self.api_cache)
            .field("http", &self.http)
            .field("push_permits", &"<Semaphore>")
            .field("latest_prices", &"<RwLock<HashMap>>")
            .finish()
    }
}

impl State {
    pub async fn new(
        mut config: Config,
        database: DatabasePool,
        grpc: Grpc,
        http: HTTP,
    ) -> Result<State, Error> {
        // =====================================================================
        // PHASE 1: Fetch active data from contracts
        // =====================================================================

        // Get platform info (treasury contract)
        let platform = grpc.get_platform(config.admin_contract.clone()).await?;
        config.treasury_contract = platform.treasury;
        tracing::info!("Loaded treasury contract: {}", config.treasury_contract);

        // Get all active protocols from admin contract
        let active_protocol_names = grpc
            .get_admin_config(config.admin_contract.clone())
            .await?;
        tracing::info!(
            "Found {} protocols from admin contract",
            active_protocol_names.len()
        );

        let mut active_currencies: HashMap<String, Currency> = HashMap::new();
        let mut active_protocols: HashMap<String, AdminProtocolExtendType> =
            HashMap::new();
        let mut protocol_registry_entries: Vec<ProtocolRegistry> = Vec::new();

        for protocol_name in &active_protocol_names {
            if config.ignore_protocols.contains(protocol_name) {
                tracing::info!("Ignoring protocol: {}", protocol_name);
                continue;
            }

            // Get protocol contracts from admin
            let protocol_config = grpc
                .get_protocol_config_full(
                    config.admin_contract.clone(),
                    protocol_name.clone(),
                )
                .await?;

            // Get currencies from this protocol's oracle
            let currencies = grpc
                .get_currencies(protocol_config.contracts.oracle.clone())
                .await?;

            for currency in &currencies {
                // Upsert to database registry
                database
                    .currency_registry
                    .upsert_active(currency, protocol_name)
                    .await?;

                // Build runtime currency
                active_currencies.insert(
                    currency.ticker.clone(),
                    Currency(
                        currency.ticker.clone(),
                        currency.decimal_digits,
                        currency.bank_symbol.clone(),
                    ),
                );
            }

            // Get LPN from LPP contract
            // Get LPN from LPP contract and stable currency from oracle
            let (lpn, stable_currency) = tokio::try_join!(
                grpc.get_lpn(protocol_config.contracts.lpp.clone()),
                grpc.get_stable_currency(protocol_config.contracts.oracle.clone())
            )?;

            // Determine position type: Long if LPN is the stable currency, Short otherwise
            let position_type = if lpn == stable_currency { "Long" } else { "Short" };

            // Get dex as string for storage
            let dex_str = protocol_config
                .dex
                .as_ref()
                .map(|d| d.to_string());

            // Create protocol registry entry
            let registry_entry = ProtocolRegistry {
                protocol_name: protocol_name.clone(),
                network: Some(protocol_config.network.clone()),
                dex: dex_str,
                leaser_contract: Some(protocol_config.contracts.leaser.clone()),
                lpp_contract: Some(protocol_config.contracts.lpp.clone()),
                oracle_contract: Some(protocol_config.contracts.oracle.clone()),
                profit_contract: Some(protocol_config.contracts.profit.clone()),
                reserve_contract: protocol_config.contracts.reserve.clone(),
                lpn_symbol: lpn.clone(),
                position_type: position_type.to_string(),
                is_active: true,
                first_seen_at: chrono::Utc::now(),
                deprecated_at: None,
            };
            protocol_registry_entries.push(registry_entry);

            // Build active protocol for price fetching
            active_protocols.insert(
                protocol_name.clone(),
                AdminProtocolExtendType {
                    network: protocol_config.network,
                    protocol: protocol_name.clone(),
                    contracts: ProtocolContracts {
                        leaser: protocol_config.contracts.leaser,
                        lpp: protocol_config.contracts.lpp,
                        oracle: protocol_config.contracts.oracle,
                        profit: protocol_config.contracts.profit,
                        reserve: protocol_config.contracts.reserve,
                    },
                },
            );

            tracing::info!(
                "Loaded protocol: {} (LPN: {}, type: {})",
                protocol_name,
                lpn,
                position_type
            );
        }

        // =====================================================================
        // PHASE 2: Sync to database registry
        // =====================================================================

        // Upsert active protocols
        for entry in &protocol_registry_entries {
            database.protocol_registry.upsert_active(entry).await?;
        }

        // Mark currencies NOT in active set as deprecated
        let active_tickers: Vec<String> =
            active_currencies.keys().cloned().collect();
        let deprecated_currencies = database
            .currency_registry
            .mark_deprecated_except(&active_tickers)
            .await?;
        if deprecated_currencies > 0 {
            tracing::info!(
                "Marked {} currencies as deprecated",
                deprecated_currencies
            );
        }

        // Mark protocols NOT in active set as deprecated
        let active_proto_names: Vec<String> =
            active_protocols.keys().cloned().collect();
        let deprecated_protocols = database
            .protocol_registry
            .mark_deprecated_except(&active_proto_names)
            .await?;
        if deprecated_protocols > 0 {
            tracing::info!(
                "Marked {} protocols as deprecated",
                deprecated_protocols
            );
        }

        // =====================================================================
        // PHASE 3: Load ALL data (active + deprecated) into runtime state
        // =====================================================================

        // Load ALL currencies for historical lookups
        let all_currencies = database.currency_registry.get_all().await?;
        let mut hash_map_currencies: HashMap<String, Currency> = HashMap::new();
        for c in all_currencies {
            hash_map_currencies.insert(
                c.ticker.clone(),
                Currency(
                    c.ticker,
                    c.decimal_digits,
                    c.bank_symbol.unwrap_or_default(),
                ),
            );
        }
        config.hash_map_currencies = hash_map_currencies;

        // Load ALL protocols for historical lookups
        let all_protocols_db = database.protocol_registry.get_all().await?;

        // Build pool_id -> protocol_name mapping (for get_protocol_by_pool_id)
        let mut hash_map_pool_protocol: HashMap<String, String> = HashMap::new();
        let mut hash_map_pool_currency: HashMap<String, Currency> = HashMap::new();

        for p in &all_protocols_db {
            if let Some(lpp) = &p.lpp_contract {
                hash_map_pool_protocol
                    .insert(lpp.clone(), p.protocol_name.clone());

                // Also build pool -> currency mapping
                if let Some(currency) =
                    config.hash_map_currencies.get(&p.lpn_symbol)
                {
                    hash_map_pool_currency.insert(lpp.clone(), currency.clone());
                }
            }
        }
        config.hash_map_pool_currency = hash_map_pool_currency;

        // =====================================================================
        // PHASE 4: Initialize LP_Pool table (for backward compatibility)
        // =====================================================================

        for p in &all_protocols_db {
            if let Some(lpp) = &p.lpp_contract {
                let pool = LP_Pool {
                    LP_Pool_id: lpp.clone(),
                    LP_symbol: p.lpn_symbol.clone(),
                    LP_status: p.is_active,
                };
                database.lp_pool.insert(pool).await?;
            }
        }

        // Log summary
        let (active_curr, deprecated_curr) =
            database.currency_registry.count_by_status().await?;
        let (active_proto, deprecated_proto) =
            database.protocol_registry.count_by_status().await?;
        tracing::info!(
            "Configuration loaded: {} active currencies ({} deprecated), {} active protocols ({} deprecated)",
            active_curr,
            deprecated_curr,
            active_proto,
            deprecated_proto
        );

        Ok(Self {
            config,
            database,
            grpc,
            http,
            protocols: active_protocols,
            hash_map_pool_protocol,
            api_cache: ApiCache::new(),
            push_permits: Arc::new(Semaphore::new(MAX_PUSH_TASKS)),
            latest_prices: Arc::new(RwLock::new(HashMap::new())),
        })
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

    /// Get protocol name by pool_id (LPP contract address)
    /// Uses hash_map_pool_protocol which includes ALL protocols (active + deprecated)
    /// This ensures historical lookups work even for deprecated protocols
    pub fn get_protocol_by_pool_id(&self, pool_id: &str) -> Option<String> {
        self.hash_map_pool_protocol.get(pool_id).cloned()
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

    /// Get the first (default) protocol name for treasury operations
    /// Returns None if no protocols are configured
    pub fn get_default_protocol(&self) -> Option<String> {
        self.protocols.keys().next().cloned()
    }

    /// Get all active LP pool IDs
    pub fn get_active_pool_ids(&self) -> Vec<String> {
        self.protocols
            .values()
            .map(|p| p.contracts.lpp.clone())
            .collect()
    }

    /// Get position type (Long/Short) by pool_id
    /// Looks up in database protocol registry
    pub async fn get_position_type_by_pool_id(
        &self,
        pool_id: &str,
    ) -> Result<String, Error> {
        if let Some(protocol) = self.database.protocol_registry.get_by_lpp_contract(pool_id).await? {
            Ok(protocol.position_type)
        } else {
            Err(Error::ProtocolError(format!(
                "Protocol not found for pool {}",
                pool_id
            )))
        }
    }

    /// Build TvlPoolParams from dynamic protocol configuration
    /// Maps protocol names to their LPP contract addresses
    /// Returns empty strings for missing protocols (SQL will handle gracefully)
    pub fn build_tvl_pool_params(&self) -> TvlPoolParams {
        let get_pool = |name: &str| -> String {
            self.protocols
                .get(name)
                .map(|p| p.contracts.lpp.clone())
                .unwrap_or_default()
        };

        TvlPoolParams {
            osmosis_usdc: get_pool("OSMOSIS"),
            neutron_axelar: get_pool("NEUTRON-USDC-AXELAR"),
            osmosis_usdc_noble: get_pool("OSMOSIS-USDC-NOBLE"),
            neutron_usdc_noble: get_pool("NEUTRON"),
            osmosis_st_atom: get_pool("OSMOSIS-ST-ATOM"),
            osmosis_all_btc: get_pool("OSMOSIS-ALL-BTC"),
            osmosis_all_sol: get_pool("OSMOSIS-ALL-SOL"),
            osmosis_akt: get_pool("OSMOSIS-AKT"),
        }
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
    // Dynamic configuration - populated from registry at startup
    pub hash_map_currencies: HashMap<String, Currency>,
    pub hash_map_pool_currency: HashMap<String, Currency>,
    // Treasury contract - loaded from admin contract's platform query
    pub treasury_contract: String,
    pub server_host: String,
    pub port: u16,
    pub allowed_origins: Vec<String>,
    pub static_dir: String,
    pub max_tasks: usize,
    // Admin contract - the bootstrap contract for dynamic configuration
    pub admin_contract: String,
    pub ignore_protocols: Vec<String>,
    // Native currency symbol (NLS) - used for filtering
    pub native_currency: String,
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
    let aggregation_interval = env::var("AGGREGATION_INTERVAL")?.parse()?;
    let mp_asset_interval = env::var("MP_ASSET_INTERVAL_IN_SEC")?.parse()?;
    let cache_state_interval =
        env::var("CACHE_INTERVAL_IN_MINUTES")?.parse()?;
    let timeout = env::var("TIMEOUT")?.parse()?;
    let max_tasks = env::var("MAX_TASKS")?.parse()?;
    
    // Admin contract is the bootstrap for dynamic configuration
    let admin_contract = env::var("ADMIN_CONTRACT")?.parse()?;
    
    let socket_reconnect_interval =
        env::var("SOCKET_RECONNECT_INTERVAL")?.parse()?;
    let grpc_host = env::var("GRPC_HOST")?.parse()?;
    let events_subscribe: String = env::var("EVENTS_SUBSCRIBE")?.parse()?;

    // Parse ignore protocols (optional, defaults to empty)
    let ignore_protocols = env::var("IGNORE_PROTOCOLS")
        .unwrap_or_default()
        .split(',')
        .filter(|s| !s.is_empty())
        .map(|item| item.to_owned())
        .collect::<Vec<String>>();

    // Native currency (defaults to NLS)
    let native_currency = env::var("NATIVE_CURRENCY")
        .unwrap_or_else(|_| "NLS".to_string());

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
        // These will be populated dynamically from the registry in State::new()
        hash_map_currencies: HashMap::new(),
        hash_map_pool_currency: HashMap::new(),
        // Treasury contract will be loaded from admin contract's platform query
        treasury_contract: String::new(),
        server_host,
        port,
        allowed_origins,
        static_dir,
        max_tasks,
        admin_contract,
        ignore_protocols,
        native_currency,
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


