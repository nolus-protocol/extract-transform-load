use chrono::{DateTime, Utc};
use sqlx::{types::BigDecimal, Error, QueryBuilder};

use crate::model::{MP_Asset, Table};

impl Table<MP_Asset> {
    pub async fn insert(&self, data: MP_Asset) -> Result<(), Error> {
        const SQL: &str = r#"
        INSERT INTO "MP_Asset" (
            "MP_asset_symbol",
            "MP_asset_timestamp",
            "MP_price_in_stable",
            "Protocol"
        )
        VALUES ($1, $2, $3, $4)
        "#;

        sqlx::query(SQL)
            .bind(&data.MP_asset_symbol)
            .bind(data.MP_asset_timestamp)
            .bind(&data.MP_price_in_stable)
            .bind(&data.Protocol)
            .execute(&self.pool)
            .await
            .map(drop)
    }

    pub async fn insert_many(&self, data: &Vec<MP_Asset>) -> Result<(), Error> {
        const SQL: &str = r#"
        INSERT INTO "MP_Asset" (
            "MP_asset_symbol",
            "MP_asset_timestamp",
            "MP_price_in_stable",
            "Protocol"
        )
        "#;

        if data.is_empty() {
            return Ok(());
        }

        QueryBuilder::new(SQL)
            .push_values(data, |mut b, mp| {
                b.push_bind(&mp.MP_asset_symbol)
                    .push_bind(mp.MP_asset_timestamp)
                    .push_bind(&mp.MP_price_in_stable)
                    .push_bind(&mp.Protocol);
            })
            .build()
            .persistent(false)
            .execute(&self.pool)
            .await
            .map(drop)
    }

    pub async fn get_min_max_from_range(
        &self,
        key: String,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Option<(BigDecimal, BigDecimal)>, Error> {
        const SQL: &str = r#"
        SELECT
            MIN("MP_price_in_stable"),
            MAX("MP_price_in_stable")
        FROM "MP_Asset"
        WHERE
            "MP_asset_symbol" = $1 AND
            "MP_asset_timestamp" >= $2 AND
            "MP_asset_timestamp" <= $3
        "#;

        sqlx::query_as(SQL)
            .bind(key)
            .bind(from)
            .bind(to)
            .fetch_optional(&self.pool)
            .await
    }

    pub async fn get_prices(
        &self,
        key: String,
        protocol: String,
        date_time: DateTime<Utc>,
        group: i32,
    ) -> Result<Vec<(DateTime<Utc>, BigDecimal)>, Error> {
        const SQL: &str = r#"
        SELECT
            (
                date_trunc(
                    'hour',
                    "MP_asset_timestamp"
                ) + (
                    (
                        (
                            date_part(
                                'minute',
                                "MP_asset_timestamp"
                            )::integer / $1::integer
                        ) * $2::integer
                    ) || ' minutes'
                )::interval
            ) AS "MP_asset_timestamp",
            MAX("MP_price_in_stable") AS "MP_price_in_stable"
        FROM "MP_Asset"
        WHERE
            "MP_asset_symbol" = $3 AND
            "Protocol" = $4 AND
            "MP_asset_timestamp" >= $5
        GROUP BY 1
        ORDER BY "MP_asset_timestamp" DESC
        "#;

        sqlx::query_as(SQL)
            .bind(group)
            .bind(group)
            .bind(key)
            .bind(protocol)
            .bind(date_time)
            .fetch_all(&self.pool)
            .await
    }

    pub async fn get_price(
        &self,
        key: &str,
        protocol: Option<String>,
    ) -> Result<(BigDecimal,), Error> {
        const WITH_PROTOCOL_SQL: &str = r#"
        SELECT
            "MP_price_in_stable"
        FROM "MP_Asset"
        WHERE
            "Protocol" = $1 AND
            "MP_asset_symbol" = $2
        ORDER BY "MP_asset_timestamp" DESC
        LIMIT 1
        "#;

        const WITHOUT_PROTOCOL_SQL: &str = r#"
        SELECT
            "MP_price_in_stable"
        FROM "MP_Asset"
        WHERE "MP_asset_symbol" = $1
        ORDER BY "MP_asset_timestamp" DESC
        LIMIT 1
        "#;

        protocol
            .map_or_else(
                || sqlx::query_as(WITHOUT_PROTOCOL_SQL),
                |protocol| sqlx::query_as(WITH_PROTOCOL_SQL).bind(protocol),
            )
            .bind(key)
            .fetch_one(&self.pool)
            .await
    }

    pub async fn get_price_by_date(
        &self,
        key: &str,
        protocol: Option<String>,
        date_time: &DateTime<Utc>,
    ) -> Result<(BigDecimal,), Error> {
        const WITHOUT_PROTOCOL_SQL: &str = r#"
        SELECT
            "MP_price_in_stable"
        FROM "MP_Asset"
        WHERE
            "MP_asset_symbol" = $1 AND
            "MP_asset_timestamp" >= $2
        ORDER BY "MP_asset_timestamp" ASC
        LIMIT 1
        "#;

        const WITH_PROTOCOL_SQL: &str = r#"
        SELECT
            "MP_price_in_stable"
        FROM "MP_Asset"
        WHERE
            "Protocol" = $1 AND
            "MP_asset_symbol" = $2 AND
            "MP_asset_timestamp" >= $3
        ORDER BY "MP_asset_timestamp" ASC
        LIMIT 1
        "#;

        if let Some(result) = protocol
            .map_or_else(
                || sqlx::query_as(WITHOUT_PROTOCOL_SQL),
                |protocol| sqlx::query_as(WITH_PROTOCOL_SQL).bind(protocol),
            )
            .bind(key)
            .bind(date_time)
            .fetch_optional(&self.pool)
            .await?
        {
            Ok(result)
        } else {
            self.get_price(key, None).await
        }
    }
}
