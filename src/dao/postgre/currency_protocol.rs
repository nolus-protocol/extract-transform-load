use sqlx::Error;

use crate::model::{CurrencyProtocol, Table};

impl Table<CurrencyProtocol> {
    /// Upsert a currency-protocol relationship
    pub async fn upsert(
        &self,
        ticker: &str,
        protocol: &str,
        group: &str,
    ) -> Result<(), Error> {
        sqlx::query(
            r#"
            INSERT INTO "currency_protocol" ("ticker", "protocol", "group")
            VALUES ($1, $2, $3)
            ON CONFLICT ("ticker", "protocol") DO UPDATE SET
                "group" = EXCLUDED."group"
            "#,
        )
        .bind(ticker)
        .bind(protocol)
        .bind(group)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Get all currency-protocol relationships
    pub async fn get_all(&self) -> Result<Vec<CurrencyProtocol>, Error> {
        sqlx::query_as(
            r#"
            SELECT "ticker", "protocol", "group"
            FROM "currency_protocol"
            ORDER BY "ticker", "protocol"
            "#,
        )
        .persistent(true)
        .fetch_all(&self.pool)
        .await
    }

    /// Get protocols for a specific currency
    pub async fn get_by_ticker(
        &self,
        ticker: &str,
    ) -> Result<Vec<CurrencyProtocol>, Error> {
        sqlx::query_as(
            r#"
            SELECT "ticker", "protocol", "group"
            FROM "currency_protocol"
            WHERE "ticker" = $1
            ORDER BY "protocol"
            "#,
        )
        .bind(ticker)
        .persistent(true)
        .fetch_all(&self.pool)
        .await
    }

    /// Remove all entries for tickers not in the active list
    pub async fn remove_deprecated(
        &self,
        active_tickers: &[String],
    ) -> Result<u64, Error> {
        let result = sqlx::query(
            r#"
            DELETE FROM "currency_protocol"
            WHERE "ticker" != ALL($1)
            "#,
        )
        .bind(active_tickers)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }
}
