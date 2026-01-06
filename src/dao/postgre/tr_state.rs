use std::str::FromStr as _;

use chrono::{DateTime, Utc};
use sqlx::{types::BigDecimal, Error, QueryBuilder};

use crate::model::{TR_State, Table};

use super::{DataBase, QueryResult};

impl Table<TR_State> {
    pub async fn insert(&self, data: TR_State) -> Result<QueryResult, Error> {
        sqlx::query(
            r#"
            INSERT INTO "TR_State" ("TR_timestamp", "TR_amnt_stable", "TR_amnt_nls")
            VALUES($1, $2, $3)
            "#,
        )
        .bind(data.TR_timestamp)
        .bind(&data.TR_amnt_stable)
        .bind(&data.TR_amnt_nls)
        .persistent(true)
        .execute(&self.pool)
        .await
    }

    pub async fn insert_many(&self, data: &Vec<TR_State>) -> Result<(), Error> {
        if data.is_empty() {
            return Ok(());
        }

        let mut query_builder: QueryBuilder<DataBase> = QueryBuilder::new(
            r#"
            INSERT INTO "TR_State" (
                "TR_timestamp",
                "TR_amnt_stable",
                "TR_amnt_nls"
            )"#,
        );

        query_builder.push_values(data, |mut b, data| {
            b.push_bind(data.TR_timestamp)
                .push_bind(&data.TR_amnt_stable)
                .push_bind(&data.TR_amnt_nls);
        });

        let query = query_builder.build().persistent(true);
        query.execute(&self.pool).await?;
        Ok(())
    }

    pub async fn get_amnt_stable(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<BigDecimal, crate::error::Error> {
        let value: (Option<BigDecimal>,) = sqlx::query_as(
            r#"
            SELECT
                SUM("TR_amnt_stable")
            FROM "TR_State" WHERE "TR_timestamp" > $1 AND "TR_timestamp" <= $2
            "#,
        )
        .bind(from)
        .bind(to)
        .persistent(true)
        .fetch_one(&self.pool)
        .await?;
        let (amnt,) = value;
        let amnt = amnt.unwrap_or(BigDecimal::from_str("0")?);

        Ok(amnt)
    }

    pub async fn get_amnt_nls(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<BigDecimal, crate::error::Error> {
        let value: (Option<BigDecimal>,) = sqlx::query_as(
            r#"
            SELECT
                SUM("TR_amnt_nls")
            FROM "TR_State" WHERE "TR_timestamp" > $1 AND "TR_timestamp" <= $2
            "#,
        )
        .bind(from)
        .bind(to)
        .persistent(true)
        .fetch_one(&self.pool)
        .await?;
        let (amnt,) = value;
        let amnt = amnt.unwrap_or(BigDecimal::from_str("0")?);

        Ok(amnt)
    }

    pub async fn get_incentives_pool(
        &self,
    ) -> Result<BigDecimal, crate::error::Error> {
        let value: Option<(BigDecimal,)>   = sqlx::query_as(
            r#"
                SELECT "TR_amnt_nls" / 1000000 AS "Incentives Pool" FROM "TR_State" ORDER BY "TR_timestamp" DESC LIMIT 1
            "#,
        )
        .persistent(true)
        .fetch_optional(&self.pool)
        .await?;
        let amnt = value.unwrap_or((BigDecimal::from_str("0")?,));

        Ok(amnt.0)
    }
}
