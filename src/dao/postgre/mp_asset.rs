use std::iter;

use chrono::{DateTime, Utc};
use sqlx::{error::Error, types::BigDecimal, QueryBuilder};

use crate::{
    custom_uint::UInt7,
    model::{MP_Asset, Table},
};

impl Table<MP_Asset> {
    pub async fn insert(
        &self,
        &MP_Asset {
            ref MP_asset_symbol,
            MP_asset_timestamp,
            ref MP_price_in_stable,
            ref Protocol,
        }: &MP_Asset,
    ) -> Result<(), Error> {
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
            .bind(MP_asset_symbol)
            .bind(MP_asset_timestamp)
            .bind(MP_price_in_stable)
            .bind(Protocol)
            .execute(&self.pool)
            .await
            .map(drop)
    }

    pub async fn insert_many<'r, T>(&self, data: T) -> Result<(), Error>
    where
        T: IntoIterator<Item = &'r MP_Asset>,
    {
        const SQL: &'static str = r#"
        INSERT INTO "MP_Asset" (
            "MP_asset_symbol",
            "MP_asset_timestamp",
            "MP_price_in_stable",
            "Protocol"
        )
        "#;

        let mut iter = data.into_iter();

        let Some(first) = iter.next() else {
            return Ok(());
        };

        QueryBuilder::new(SQL)
            .push_values(
                iter::once(first).chain(iter),
                |mut b,
                 &MP_Asset {
                     ref MP_asset_symbol,
                     MP_asset_timestamp,
                     ref MP_price_in_stable,
                     ref Protocol,
                 }| {
                    b.push_bind(MP_asset_symbol)
                        .push_bind(MP_asset_timestamp)
                        .push_bind(MP_price_in_stable)
                        .push_bind(Protocol);
                },
            )
            .build()
            .execute(&self.pool)
            .await
            .map(drop)
    }

    pub async fn get_min_max_from_range(
        &self,
        key: &str,
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
        key: &str,
        protocol: &str,
        date_time: DateTime<Utc>,
        group: UInt7,
    ) -> Result<Vec<(DateTime<Utc>, BigDecimal)>, Error> {
        const SQL: &str = r#"
        SELECT
            (
                date_trunc('hour', "MP_asset_timestamp") + interval(
                    (
                        (
                            date_part(
                                'minute',
                                "MP_asset_timestamp"
                            )::integer / $1::integer
                        ) * $2::integer
                    ) || ' minutes'
                )
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
        protocol: Option<&str>,
    ) -> Result<BigDecimal, Error> {
        const SQL_NO_PROTOCOL: &'static str = r#"
        SELECT "MP_price_in_stable"
        FROM "MP_Asset"
        WHERE "MP_asset_symbol" = $1
        ORDER BY "MP_asset_timestamp" DESC
        LIMIT 1
        "#;

        const SQL_WITH_PROTOCOL: &'static str = r#"
        SELECT "MP_price_in_stable"
        FROM "MP_Asset"
        WHERE
            "Protocol" = $1
            "MP_asset_symbol" = $2 AND
        ORDER BY "MP_asset_timestamp" DESC
        LIMIT 1
        "#;

        protocol
            .map_or_else(
                || sqlx::query_as(SQL_NO_PROTOCOL),
                |protocol| sqlx::query_as(SQL_WITH_PROTOCOL).bind(protocol),
            )
            .bind(key)
            .fetch_one(&self.pool)
            .await
            .map(|(result,)| result)
    }

    pub async fn get_price_by_date(
        &self,
        key: &str,
        protocol: Option<&str>,
        date_time: DateTime<Utc>,
    ) -> Result<BigDecimal, Error> {
        const SQL_NO_PROTOCOL: &'static str = r#"
        SELECT "MP_price_in_stable"
        FROM "MP_Asset"
        WHERE
            "MP_asset_symbol" = $1 AND
            "MP_asset_timestamp" >= $2
        ORDER BY "MP_asset_timestamp" ASC
        LIMIT 1
        "#;

        const SQL_WITH_PROTOCOL: &'static str = r#"
        SELECT "MP_price_in_stable"
        FROM "MP_Asset"
        WHERE
            "Protocol" = $1 AND
            "MP_asset_symbol" = $2 AND 
            "MP_asset_timestamp" >= $3
        ORDER BY "MP_asset_timestamp" ASC
        LIMIT 1
        "#;

        let item = protocol
            .map_or_else(
                || sqlx::query_as(SQL_NO_PROTOCOL),
                |protocol| sqlx::query_as(SQL_WITH_PROTOCOL).bind(protocol),
            )
            .bind(key)
            .bind(date_time)
            .fetch_optional(&self.pool)
            .await?;

        if let Some((item,)) = item {
            Ok(item)
        } else {
            self.get_price(key, None).await
        }
    }
}
