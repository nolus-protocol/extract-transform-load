use bigdecimal::Zero as _;
use chrono::{DateTime, Utc};
use sqlx::{types::BigDecimal, Error, QueryBuilder};

use crate::model::{TR_State, Table};

impl Table<TR_State> {
    pub async fn insert(&self, data: TR_State) -> Result<(), Error> {
        const SQL: &str = r#"
        INSERT INTO "TR_State" (
            "TR_timestamp",
            "TR_amnt_stable",
            "TR_amnt_nls"
        )
        VALUES ($1, $2, $3)
        "#;

        sqlx::query(SQL)
            .bind(data.TR_timestamp)
            .bind(&data.TR_amnt_stable)
            .bind(&data.TR_amnt_nls)
            .execute(&self.pool)
            .await
            .map(drop)
    }

    pub async fn insert_many(&self, data: &Vec<TR_State>) -> Result<(), Error> {
        const SQL: &str = r#"
        INSERT INTO "TR_State" (
            "TR_timestamp",
            "TR_amnt_stable",
            "TR_amnt_nls"
        )
        "#;

        if data.is_empty() {
            return Ok(());
        }

        QueryBuilder::new(SQL)
            .push_values(data, |mut b, data| {
                b.push_bind(data.TR_timestamp)
                    .push_bind(&data.TR_amnt_stable)
                    .push_bind(&data.TR_amnt_nls);
            })
            .build()
            .persistent(false)
            .execute(&self.pool)
            .await
            .map(drop)
    }

    pub async fn get_amnt_stable(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<BigDecimal, Error> {
        const SQL: &str = r#"
        SELECT
            SUM("TR_amnt_stable")
        FROM "TR_State"
        WHERE
            "TR_timestamp" > $1 AND
            "TR_timestamp" <= $2
        "#;

        sqlx::query_as(SQL)
            .bind(from)
            .bind(to)
            .fetch_optional(&self.pool)
            .await
            .map(|result| {
                result.map_or_else(BigDecimal::zero, |(result,)| result)
            })
    }

    pub async fn get_amnt_nls(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<BigDecimal, Error> {
        const SQL: &str = r#"
        SELECT
            SUM("TR_amnt_nls")
        FROM "TR_State"
        WHERE
            "TR_timestamp" > $1 AND
            "TR_timestamp" <= $2
        "#;

        sqlx::query_as(SQL)
            .bind(from)
            .bind(to)
            .fetch_optional(&self.pool)
            .await
            .map(|result| {
                result.map_or_else(BigDecimal::zero, |(result,)| result)
            })
    }

    pub async fn get_incentives_pool(&self) -> Result<BigDecimal, Error> {
        const SQL: &str = r#"
        SELECT
            (
                "TR_amnt_nls" / 1000000
            )
        FROM "TR_State"
        ORDER BY "TR_timestamp" DESC
        LIMIT 1
        "#;

        sqlx::query_as(SQL)
            .fetch_optional(&self.pool)
            .await
            .map(|result| {
                result.map_or_else(BigDecimal::zero, |(result,)| result)
            })
    }
}
