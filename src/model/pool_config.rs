use sqlx::FromRow;

/// Pool configuration for position type lookups
/// Eliminates hardcoded CTEs in queries
#[derive(Debug, Clone, FromRow)]
pub struct Pool_Config {
    pub pool_id: String,
    pub position_type: String,
    pub lpn_symbol: String,
    pub lpn_decimals: i64,
    pub label: String,
}
