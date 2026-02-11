use sqlx::Error;

use crate::model::{CurrencyRegistry, Table};
use crate::types::OracleCurrency;

impl Table<CurrencyRegistry> {
    /// Upsert an active currency from oracle
    /// Sets is_active = true and clears deprecated_at
    pub async fn upsert_active(
        &self,
        currency: &OracleCurrency,
    ) -> Result<(), Error> {
        sqlx::query(
            r#"
            INSERT INTO "currency_registry"
                ("ticker", "decimal_digits", "is_active", "first_seen_at")
            VALUES ($1, $2, true, NOW())
            ON CONFLICT ("ticker") DO UPDATE SET
                "decimal_digits" = EXCLUDED."decimal_digits",
                "is_active" = true,
                "deprecated_at" = NULL
            "#,
        )
        .bind(&currency.ticker)
        .bind(currency.decimal_digits)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Mark all currencies NOT in the provided list as deprecated
    pub async fn mark_deprecated_except(
        &self,
        active_tickers: &[String],
    ) -> Result<u64, Error> {
        let result = sqlx::query(
            r#"
            UPDATE "currency_registry"
            SET "is_active" = false, "deprecated_at" = NOW()
            WHERE "ticker" != ALL($1) AND "is_active" = true
            "#,
        )
        .bind(active_tickers)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected())
    }

    /// Get all currencies (active and deprecated)
    pub async fn get_all(&self) -> Result<Vec<CurrencyRegistry>, Error> {
        sqlx::query_as(
            r#"
            SELECT "ticker", "decimal_digits", "is_active",
                   "first_seen_at", "deprecated_at"
            FROM "currency_registry"
            ORDER BY "is_active" DESC, "ticker"
            "#,
        )
        .persistent(true)
        .fetch_all(&self.pool)
        .await
    }

    /// Get only active currencies
    pub async fn get_active(&self) -> Result<Vec<CurrencyRegistry>, Error> {
        sqlx::query_as(
            r#"
            SELECT "ticker", "decimal_digits", "is_active",
                   "first_seen_at", "deprecated_at"
            FROM "currency_registry"
            WHERE "is_active" = true
            ORDER BY "ticker"
            "#,
        )
        .persistent(true)
        .fetch_all(&self.pool)
        .await
    }

    /// Get a currency by ticker
    pub async fn get_by_ticker(
        &self,
        ticker: &str,
    ) -> Result<Option<CurrencyRegistry>, Error> {
        sqlx::query_as(
            r#"
            SELECT "ticker", "decimal_digits", "is_active",
                   "first_seen_at", "deprecated_at"
            FROM "currency_registry"
            WHERE "ticker" = $1
            "#,
        )
        .bind(ticker)
        .persistent(true)
        .fetch_optional(&self.pool)
        .await
    }

    /// Count currencies by status
    pub async fn count_by_status(&self) -> Result<(i64, i64), Error> {
        let active: (i64,) = sqlx::query_as(
            r#"SELECT COUNT(*) FROM "currency_registry" WHERE "is_active" = true"#,
        )
        .fetch_one(&self.pool)
        .await?;

        let deprecated: (i64,) = sqlx::query_as(
            r#"SELECT COUNT(*) FROM "currency_registry" WHERE "is_active" = false"#,
        )
        .fetch_one(&self.pool)
        .await?;

        Ok((active.0, deprecated.0))
    }
}
