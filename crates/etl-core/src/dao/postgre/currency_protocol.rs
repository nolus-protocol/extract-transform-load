use sqlx::Error;

use crate::model::{CurrencyProtocol, Table};

impl Table<CurrencyProtocol> {
    /// Upsert a currency-protocol relationship with per-protocol denoms
    pub async fn upsert(
        &self,
        ticker: &str,
        protocol: &str,
        group: &str,
        bank_symbol: &str,
        dex_symbol: Option<&str>,
    ) -> Result<(), Error> {
        sqlx::query(
            r#"
            INSERT INTO "currency_protocol" ("ticker", "protocol", "group", "bank_symbol", "dex_symbol")
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT ("ticker", "protocol") DO UPDATE SET
                "group" = EXCLUDED."group",
                "bank_symbol" = EXCLUDED."bank_symbol",
                "dex_symbol" = EXCLUDED."dex_symbol"
            "#,
        )
        .bind(ticker)
        .bind(protocol)
        .bind(group)
        .bind(bank_symbol)
        .bind(dex_symbol)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Get all currency-protocol relationships
    pub async fn get_all(&self) -> Result<Vec<CurrencyProtocol>, Error> {
        sqlx::query_as(
            r#"
            SELECT "ticker", "protocol", "group", "bank_symbol", "dex_symbol"
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
            SELECT "ticker", "protocol", "group", "bank_symbol", "dex_symbol"
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
}
