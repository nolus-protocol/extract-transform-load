use super::{DataBase, QueryResult};
use crate::model::{MP_Asset, Table};
use chrono::{DateTime, Utc};
use sqlx::{error::Error, types::BigDecimal, QueryBuilder};

impl Table<MP_Asset> {
    pub async fn insert(&self, data: MP_Asset) -> Result<QueryResult, Error> {
        sqlx::query(
            r#"
            INSERT INTO "MP_Asset" ("MP_asset_symbol","MP_asset_timestamp", "MP_price_in_stable")
            VALUES($1, $2, $3)
            "#,
        )
        .bind(&data.MP_asset_symbol)
        .bind(data.MP_asset_timestamp)
        .bind(&data.MP_price_in_stable)
        .execute(&self.pool)
        .await
    }

    pub async fn insert_many(&self, data: &Vec<MP_Asset>) -> Result<(), Error> {
        if data.is_empty() {
            return Ok(());
        }

        let mut query_builder: QueryBuilder<DataBase> = QueryBuilder::new(
            r#"
            INSERT INTO "MP_Asset" (
                "MP_asset_symbol",
                "MP_asset_timestamp",
                "MP_price_in_stable"
            )"#,
        );

        query_builder.push_values(data, |mut b, mp| {
            b.push_bind(&mp.MP_asset_symbol)
                .push_bind(mp.MP_asset_timestamp)
                .push_bind(&mp.MP_price_in_stable);
        });

        let query = query_builder.build();
        query.execute(&self.pool).await?;

        Ok(())
    }

    pub async fn get_min_max_from_range(
        &self,
        key: String,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Option<(BigDecimal, BigDecimal)>, Error> {
        sqlx::query_as(
            r#"
            SELECT MIN("MP_price_in_stable"), MAX("MP_price_in_stable")
            FROM "MP_Asset"
            WHERE "MP_asset_symbol" = $1 AND "MP_asset_timestamp" >= $2 AND "MP_asset_timestamp" <= $3;
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
            SELECT "MP_price_in_stable"
            FROM "MP_Asset"
            WHERE "MP_asset_symbol" = $1 ORDER BY "MP_asset_timestamp" DESC LIMIT 1
            "#,
        )
        .bind(key)
        .fetch_one(&self.pool)
        .await
    }

    pub async fn get_price_by_date(
        &self,
        key: &str,
        date_time: &DateTime<Utc>,
    ) -> Result<(BigDecimal,), Error> {
        let item = sqlx::query_as(
            r#"
            SELECT "MP_price_in_stable"
            FROM "MP_Asset"
            WHERE
                "MP_asset_symbol" = $1
                AND
                "MP_asset_timestamp" >= $2

            ORDER BY "MP_asset_timestamp" ASC LIMIT 1
            "#,
        )
        .bind(key)
        .bind(date_time)
        .fetch_one(&self.pool)
        .await;

        if let Err(err) = item {
            match err {
                Error::RowNotFound => {
                    return self.get_price(key).await;
                }
                _ => {
                    return Err(err);
                }
            }
        }

        item
    }
}
