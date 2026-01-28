use sqlx::Error;

use crate::model::{PoolConfigUpsert, Pool_Config, Table};

impl Table<Pool_Config> {
    /// Upsert an active pool configuration from blockchain data
    /// Sets is_active = true and clears deprecated_at
    pub async fn upsert(
        &self,
        data: &PoolConfigUpsert<'_>,
    ) -> Result<(), Error> {
        sqlx::query(
            r#"
            INSERT INTO "pool_config" (
                "pool_id", "position_type", "lpn_symbol", "lpn_decimals", "label", "protocol",
                "is_active", "first_seen_at", "stable_currency_symbol", "stable_currency_decimals"
            )
            VALUES ($1, $2, $3, $4, $5, $6, true, NOW(), $7, $8)
            ON CONFLICT ("pool_id") DO UPDATE SET
                "position_type" = EXCLUDED."position_type",
                "lpn_symbol" = EXCLUDED."lpn_symbol",
                "lpn_decimals" = EXCLUDED."lpn_decimals",
                "label" = EXCLUDED."label",
                "protocol" = EXCLUDED."protocol",
                "is_active" = true,
                "deprecated_at" = NULL,
                "stable_currency_symbol" = EXCLUDED."stable_currency_symbol",
                "stable_currency_decimals" = EXCLUDED."stable_currency_decimals"
            "#,
        )
        .bind(data.pool_id)
        .bind(data.position_type)
        .bind(data.lpn_symbol)
        .bind(data.lpn_decimals)
        .bind(data.label)
        .bind(data.protocol)
        .bind(data.stable_currency_symbol)
        .bind(data.stable_currency_decimals)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Mark all pools NOT in the provided list as deprecated
    pub async fn mark_deprecated_except(
        &self,
        active_pool_ids: &[String],
    ) -> Result<u64, Error> {
        let result = sqlx::query(
            r#"
            UPDATE "pool_config"
            SET "is_active" = false, "deprecated_at" = NOW()
            WHERE "pool_id" != ALL($1) AND "is_active" = true
            "#,
        )
        .bind(active_pool_ids)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected())
    }

    /// Get pool configuration by pool ID (active or deprecated, for historical queries)
    pub async fn get_by_pool_id(
        &self,
        pool_id: &str,
    ) -> Result<Option<Pool_Config>, Error> {
        sqlx::query_as(
            r#"
            SELECT "pool_id", "position_type", "lpn_symbol", "lpn_decimals", "label",
                   "is_active", "first_seen_at", "deprecated_at",
                   "stable_currency_symbol", "stable_currency_decimals"
            FROM "pool_config"
            WHERE "pool_id" = $1
            "#,
        )
        .bind(pool_id)
        .persistent(true)
        .fetch_optional(&self.pool)
        .await
    }

    /// Get all pool configurations (active and deprecated)
    pub async fn get_all(&self) -> Result<Vec<Pool_Config>, Error> {
        sqlx::query_as(
            r#"
            SELECT "pool_id", "position_type", "lpn_symbol", "lpn_decimals", "label",
                   "is_active", "first_seen_at", "deprecated_at",
                   "stable_currency_symbol", "stable_currency_decimals"
            FROM "pool_config"
            ORDER BY "is_active" DESC, "position_type", "label"
            "#,
        )
        .persistent(true)
        .fetch_all(&self.pool)
        .await
    }

    /// Get all active Long position pools
    pub async fn get_long_pools(&self) -> Result<Vec<Pool_Config>, Error> {
        sqlx::query_as(
            r#"
            SELECT "pool_id", "position_type", "lpn_symbol", "lpn_decimals", "label",
                   "is_active", "first_seen_at", "deprecated_at",
                   "stable_currency_symbol", "stable_currency_decimals"
            FROM "pool_config"
            WHERE "position_type" = 'Long' AND "is_active" = true
            ORDER BY "label"
            "#,
        )
        .persistent(true)
        .fetch_all(&self.pool)
        .await
    }

    /// Get all active Short position pools
    pub async fn get_short_pools(&self) -> Result<Vec<Pool_Config>, Error> {
        sqlx::query_as(
            r#"
            SELECT "pool_id", "position_type", "lpn_symbol", "lpn_decimals", "label",
                   "is_active", "first_seen_at", "deprecated_at",
                   "stable_currency_symbol", "stable_currency_decimals"
            FROM "pool_config"
            WHERE "position_type" = 'Short' AND "is_active" = true
            ORDER BY "label"
            "#,
        )
        .persistent(true)
        .fetch_all(&self.pool)
        .await
    }

    /// Count active and deprecated pools
    pub async fn count_by_status(&self) -> Result<(i64, i64), Error> {
        let active: (i64,) = sqlx::query_as(
            r#"SELECT COUNT(*) FROM "pool_config" WHERE "is_active" = true"#,
        )
        .fetch_one(&self.pool)
        .await?;

        let deprecated: (i64,) = sqlx::query_as(
            r#"SELECT COUNT(*) FROM "pool_config" WHERE "is_active" = false"#,
        )
        .fetch_one(&self.pool)
        .await?;

        Ok((active.0, deprecated.0))
    }
}
