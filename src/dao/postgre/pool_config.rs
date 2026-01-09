use sqlx::Error;

use crate::model::{Pool_Config, Table};

impl Table<Pool_Config> {
    /// Get pool configuration by pool ID
    pub async fn get_by_pool_id(
        &self,
        pool_id: &str,
    ) -> Result<Option<Pool_Config>, Error> {
        sqlx::query_as(
            r#"
            SELECT "pool_id", "position_type", "lpn_symbol", "lpn_decimals", "label"
            FROM "pool_config"
            WHERE "pool_id" = $1
            "#,
        )
        .bind(pool_id)
        .persistent(false)
        .fetch_optional(&self.pool)
        .await
    }

    /// Get all pool configurations
    pub async fn get_all(&self) -> Result<Vec<Pool_Config>, Error> {
        sqlx::query_as(
            r#"
            SELECT "pool_id", "position_type", "lpn_symbol", "lpn_decimals", "label"
            FROM "pool_config"
            ORDER BY "position_type", "label"
            "#,
        )
        .persistent(false)
        .fetch_all(&self.pool)
        .await
    }

    /// Get all Long position pools
    pub async fn get_long_pools(&self) -> Result<Vec<Pool_Config>, Error> {
        sqlx::query_as(
            r#"
            SELECT "pool_id", "position_type", "lpn_symbol", "lpn_decimals", "label"
            FROM "pool_config"
            WHERE "position_type" = 'Long'
            ORDER BY "label"
            "#,
        )
        .persistent(false)
        .fetch_all(&self.pool)
        .await
    }

    /// Get all Short position pools
    pub async fn get_short_pools(&self) -> Result<Vec<Pool_Config>, Error> {
        sqlx::query_as(
            r#"
            SELECT "pool_id", "position_type", "lpn_symbol", "lpn_decimals", "label"
            FROM "pool_config"
            WHERE "position_type" = 'Short'
            ORDER BY "label"
            "#,
        )
        .persistent(false)
        .fetch_all(&self.pool)
        .await
    }
}
