use super::QueryResult;
use crate::model::{MP_Asset, Table};
use chrono::{DateTime, Utc};
use sqlx::{error::Error, types::BigDecimal};

impl Table<MP_Asset> {
    pub async fn insert(&self, data: MP_Asset) -> Result<QueryResult, Error> {
        sqlx::query(
            r#"
            INSERT INTO `MP_Asset` (`MP_asset_symbol`,`MP_asset_timestamp`, `MP_price_in_stable`)
            VALUES(?, ?, ?)
            "#,
        )
        .bind(&data.MP_asset_symbol)
        .bind(data.MP_asset_timestamp)
        .bind(&data.MP_price_in_stable)
        .execute(&self.pool)
        .await
    }

    pub async fn get_min_max_from_range(
        &self,
        key: String,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Option<(BigDecimal, BigDecimal)>, Error> {
        sqlx::query_as(
            r#"
            SELECT MIN(`MP_price_in_stable`), MAX(`MP_price_in_stable`)
            FROM `MP_Asset`
            WHERE `MP_asset_symbol` = ? AND `MP_asset_timestamp` >= ? AND `MP_asset_timestamp` <= ?;
            "#,
        )
        .bind(key)
        .bind(from)
        .bind(to)
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn get_price(&self, key: &str) -> Result<(BigDecimal,), Error> {
        sqlx::query_as(
            r#"
            SELECT `MP_price_in_stable`
            FROM `MP_Asset`
            WHERE `MP_asset_symbol` = ? ORDER BY `MP_asset_timestamp` DESC LIMIT 1
            "#,
        )
        .bind(key)
        .fetch_one(&self.pool)
        .await
    }
}
