use std::iter;

use chrono::{DateTime, Utc};
use sqlx::{error::Error, types::BigDecimal, QueryBuilder};

use crate::model::{TR_State, Table};

impl Table<TR_State> {
    pub async fn insert(
        &self,
        &TR_State {
            TR_timestamp,
            ref TR_amnt_stable,
            ref TR_amnt_nls,
        }: &TR_State,
    ) -> Result<(), Error> {
        const SQL: &'static str = r#"
        INSERT INTO "TR_State" (
            "TR_timestamp",
            "TR_amnt_stable",
            "TR_amnt_nls"
        )
        VALUES ($1, $2, $3)
        "#;

        sqlx::query(SQL)
            .bind(TR_timestamp)
            .bind(TR_amnt_stable)
            .bind(TR_amnt_nls)
            .execute(&self.pool)
            .await
            .map(drop)
    }

    pub async fn insert_many<'r, T>(&self, data: T) -> Result<(), Error>
    where
        T: IntoIterator<Item = &'r TR_State>,
    {
        const SQL: &str = r#"
        INSERT INTO "TR_State" (
            "TR_timestamp", 
            "TR_amnt_stable", 
            "TR_amnt_nls"
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
                 &TR_State {
                     TR_timestamp,
                     ref TR_amnt_stable,
                     ref TR_amnt_nls,
                 }| {
                    b.push_bind(TR_timestamp)
                        .push_bind(TR_amnt_stable)
                        .push_bind(TR_amnt_nls);
                },
            )
            .build()
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
        SELECT COALESCE(SUM("TR_amnt_stable"), 0)
        FROM "TR_State"
        WHERE
            "TR_timestamp" > $1 AND
            "TR_timestamp" <= $2
        "#;

        sqlx::query_as(SQL)
            .bind(from)
            .bind(to)
            .fetch_one(&self.pool)
            .await
            .map(|(result,)| result)
    }

    pub async fn get_amnt_nls(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<BigDecimal, Error> {
        const SQL: &str = r#"
        SELECT COALESCE(SUM("TR_amnt_nls"), 0)
        FROM "TR_State"
        WHERE
            "TR_timestamp" > $1 AND
            "TR_timestamp" <= $2
        "#;

        sqlx::query_as(SQL)
            .bind(from)
            .bind(to)
            .fetch_one(&self.pool)
            .await
            .map(|(result,)| result)
    }

    pub async fn get_incentives_pool(&self) -> Result<BigDecimal, Error> {
        const SQL: &str = r#"
        SELECT "TR_amnt_nls" / 1000000 AS "Incentives Pool"
        FROM "TR_State"
        ORDER BY "TR_timestamp" DESC
        LIMIT 1
        "#;

        sqlx::query_as(SQL)
            .fetch_one(&self.pool)
            .await
            .map(|(result,)| result)
    }
}
