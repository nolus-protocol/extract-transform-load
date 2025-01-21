use std::iter;

use chrono::{DateTime, Utc};
use sqlx::{error::Error, types::BigDecimal, QueryBuilder, Transaction};

use crate::{
    custom_uint::UInt63,
    model::{Buyback, TR_Profit, Table},
};

use super::DataBase;

impl Table<TR_Profit> {
    pub async fn isExists(
        &self,
        &TR_Profit {
            TR_Profit_height,
            TR_Profit_timestamp,
            ..
        }: &TR_Profit,
    ) -> Result<bool, Error> {
        const SQL: &str = r#"
        SELECT COUNT(1) > 0
        FROM "TR_Profit"
        WHERE
            "TR_Profit_height" = $1 AND
            "TR_Profit_timestamp" = $2
        "#;

        sqlx::query_as(SQL)
            .bind(TR_Profit_height)
            .bind(TR_Profit_timestamp)
            .fetch_one(&self.pool)
            .await
            .map(|(result,)| result)
    }

    pub async fn insert(
        &self,
        &TR_Profit {
            TR_Profit_height,
            TR_Profit_idx: _,
            TR_Profit_timestamp,
            ref TR_Profit_amnt_stable,
            ref TR_Profit_amnt_nls,
            Tx_Hash,
        }: &TR_Profit,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<(), Error> {
        const SQL: &str = r#"
        INSERT INTO "TR_Profit" (
            "TR_Profit_height",
            "TR_Profit_timestamp",
            "TR_Profit_amnt_stable",
            "TR_Profit_amnt_nls",
            "Tx_Hash",
        )
        VALUES ($1, $2, $3, $4, $5)
        "#;

        sqlx::query(SQL)
            .bind(TR_Profit_height)
            .bind(TR_Profit_timestamp)
            .bind(TR_Profit_amnt_stable)
            .bind(TR_Profit_amnt_nls)
            .bind(Tx_Hash)
            .execute(&mut **transaction)
            .await
            .map(drop)
    }

    pub async fn insert_many<'r, T>(
        &self,
        data: T,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<(), Error>
    where
        T: IntoIterator<Item = &'r TR_Profit>,
    {
        const SQL: &'static str = r#"
        INSERT INTO "TR_Profit" (
            "TR_Profit_height",
            "TR_Profit_timestamp",
            "TR_Profit_amnt_stable",
            "TR_Profit_amnt_nls",
            "Tx_Hash"
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
                 &TR_Profit {
                     TR_Profit_height,
                     TR_Profit_idx: _,
                     TR_Profit_timestamp,
                     ref TR_Profit_amnt_stable,
                     ref TR_Profit_amnt_nls,
                     ref Tx_Hash,
                 }| {
                    b.push_bind(TR_Profit_height)
                        .push_bind(TR_Profit_timestamp)
                        .push_bind(TR_Profit_amnt_stable)
                        .push_bind(TR_Profit_amnt_nls)
                        .push_bind(Tx_Hash);
                },
            )
            .build()
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
            COALESCE(SUM("TR_Profit_amnt_stable"), 0),
            COALESCE(SUM("TR_Profit_amnt_nls"), 0)
        FROM "TR_Profit"
        WHERE
            "TR_Profit_timestamp" > $1 AND
            "TR_Profit_timestamp" <= $2
        "#;

        sqlx::query_as(SQL)
            .bind(from)
            .bind(to)
            .fetch_one(&self.pool)
            .await
    }

    pub async fn get_buyback(
        &self,
        skip: UInt63,
        limit: UInt63,
    ) -> Result<Vec<Buyback>, Error> {
        const SQL: &str = r#"
        SELECT
            "TR_Profit_timestamp" AS time,
            (
                SUM("TR_Profit_amnt_nls" / 1000000)
                OVER(Order By "TR_Profit_timestamp")
            ) AS "Bought-back"
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
        SELECT COALESCE(SUM("TR_Profit_amnt_nls") / 1000000, 0) AS "Distributed"
        FROM "TR_Profit"
        "#;

        sqlx::query_as(SQL)
            .fetch_one(&self.pool)
            .await
            .map(|(amnt,)| amnt)
    }

    pub async fn get_revenue(&self) -> Result<BigDecimal, Error> {
        const SQL: &str = r#"
        SELECT COALESCE(SUM("TR_Profit_amnt_stable") / 1000000, 0) AS "Distributed"
        FROM "TR_Profit"
        "#;

        sqlx::query_as(SQL)
            .fetch_one(&self.pool)
            .await
            .map(|(amnt,)| amnt)
    }
}
