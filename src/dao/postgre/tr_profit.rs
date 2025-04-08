use bigdecimal::Zero as _;
use chrono::{DateTime, Utc};
use sqlx::{types::BigDecimal, Error, QueryBuilder, Transaction};

use crate::model::{Buyback, TR_Profit, Table};

use super::DataBase;

impl Table<TR_Profit> {
    pub async fn isExists(&self, tr_profit: &TR_Profit) -> Result<bool, Error> {
        const SQL: &str = r#"
        SELECT EXISTS(
            SELECT
            FROM "TR_Profit"
            WHERE
                "TR_Profit_height" = $1 AND
                "TR_Profit_timestamp" = $2
        )
        "#;

        sqlx::query_as(SQL)
            .bind(tr_profit.TR_Profit_height)
            .bind(tr_profit.TR_Profit_timestamp)
            .fetch_one(&self.pool)
            .await
            .map(|(result,)| result)
    }

    pub async fn insert(
        &self,
        data: TR_Profit,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<(), Error> {
        const SQL: &str = r#"
        INSERT INTO "TR_Profit" (
            "TR_Profit_height",
            "TR_Profit_timestamp",
            "TR_Profit_amnt_stable",
            "TR_Profit_amnt_nls",
            "Tx_Hash"
        )
        VALUES ($1, $2, $3, $4, $5)
        "#;

        sqlx::query(SQL)
            .bind(data.TR_Profit_height)
            .bind(data.TR_Profit_timestamp)
            .bind(&data.TR_Profit_amnt_stable)
            .bind(&data.TR_Profit_amnt_nls)
            .bind(&data.Tx_Hash)
            .execute(&mut **transaction)
            .await
            .map(drop)
    }

    pub async fn insert_many(
        &self,
        data: &Vec<TR_Profit>,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<(), Error> {
        const SQL: &str = r#"
        INSERT INTO "TR_Profit" (
            "TR_Profit_height",
            "TR_Profit_timestamp",
            "TR_Profit_amnt_stable",
            "TR_Profit_amnt_nls",
            "Tx_Hash"
        )
        "#;

        if data.is_empty() {
            return Ok(());
        }

        QueryBuilder::new(SQL)
            .push_values(data, |mut b, tr| {
                b.push_bind(tr.TR_Profit_height)
                    .push_bind(tr.TR_Profit_timestamp)
                    .push_bind(&tr.TR_Profit_amnt_stable)
                    .push_bind(&tr.TR_Profit_amnt_nls)
                    .push_bind(&tr.Tx_Hash);
            })
            .build()
            .persistent(false)
            .execute(&mut **transaction)
            .await
            .map(drop)
    }

    pub async fn get_amnt_stable(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<(BigDecimal, BigDecimal), Error> {
        const SQL: &str = r#"
        SELECT
            SUM("TR_Profit_amnt_stable"),
            SUM("TR_Profit_amnt_nls")
        FROM "TR_Profit"
        WHERE
            "TR_Profit_timestamp" > $1 AND
            "TR_Profit_timestamp" <= $2
        "#;

        sqlx::query_as(SQL)
            .bind(from)
            .bind(to)
            .fetch_optional(&self.pool)
            .await
            .map(|result| {
                result
                    .unwrap_or_else(|| (BigDecimal::zero(), BigDecimal::zero()))
            })
    }

    pub async fn get_buyback(
        &self,
        skip: i64,
        limit: i64,
    ) -> Result<Vec<Buyback>, Error> {
        const SQL: &str = r#"
        SELECT
            (
                SUM(
                    "TR_Profit_amnt_nls" / 1000000
                ) OVER (
                    ORDER BY "TR_Profit_timestamp"
                )
            ) AS "Bought-back",
            "TR_Profit_timestamp" AS "time"
        FROM "TR_Profit"
        OFFSET $1
        LIMIT $2
        "#;

        sqlx::query_as(SQL)
            .bind(skip)
            .bind(limit)
            .fetch_all(&self.pool)
            .await
    }

    pub async fn get_buyback_total(&self) -> Result<BigDecimal, Error> {
        const SQL: &str = r#"
        SELECT
            (
                SUM("TR_Profit_amnt_nls") / 1000000
            )
        FROM "TR_Profit"
        "#;

        sqlx::query_as(SQL)
            .fetch_optional(&self.pool)
            .await
            .map(|result| {
                result.map_or_else(BigDecimal::zero, |(result,)| result)
            })
    }

    pub async fn get_revenue(&self) -> Result<BigDecimal, Error> {
        const SQL: &str = r#"
        SELECT
            (
                SUM("TR_Profit_amnt_stable") / 1000000
            )
        FROM "TR_Profit"
        "#;

        sqlx::query_as(SQL)
            .fetch_optional(&self.pool)
            .await
            .map(|result| {
                result.map_or_else(BigDecimal::zero, |(result,)| result)
            })
    }
}
