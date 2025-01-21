use std::iter;

use chrono::{DateTime, Utc};
use sqlx::{error::Error, types::BigDecimal, QueryBuilder, Transaction};

use crate::model::{TR_Rewards_Distribution, Table};

use super::DataBase;

impl Table<TR_Rewards_Distribution> {
    pub async fn isExists(
        &self,
        &TR_Rewards_Distribution {
            TR_Rewards_height,
            ref TR_Rewards_Pool_id,
            Event_Block_Index,
            ..
        }: &TR_Rewards_Distribution,
    ) -> Result<bool, Error> {
        const SQL: &str = r#"
        SELECT COUNT(1) > 0
        FROM "TR_Rewards_Distribution"
        WHERE
            "TR_Rewards_height" = $1 AND
            "TR_Rewards_Pool_id" = $2 AND
            "Event_Block_Index" = $3
        "#;

        sqlx::query_as(SQL)
            .bind(TR_Rewards_height)
            .bind(TR_Rewards_Pool_id)
            .bind(Event_Block_Index)
            .fetch_one(&self.pool)
            .await
            .map(|(result,)| result)
    }

    pub async fn insert(
        &self,
        &TR_Rewards_Distribution {
            TR_Rewards_height,
            TR_Rewards_idx: _,
            ref TR_Rewards_Pool_id,
            TR_Rewards_timestamp,
            ref TR_Rewards_amnt_stable,
            ref TR_Rewards_amnt_nls,
            Event_Block_Index,
            ref Tx_Hash,
        }: &TR_Rewards_Distribution,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<(), Error> {
        const SQL: &str = r#"
        INSERT INTO "TR_Rewards_Distribution" (
            "TR_Rewards_height",
            "TR_Rewards_Pool_id",
            "TR_Rewards_timestamp",
            "TR_Rewards_amnt_stable",
            "TR_Rewards_amnt_nls",
            "Event_Block_Index",
            "Tx_Hash"
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        "#;

        sqlx::query(SQL)
            .bind(TR_Rewards_height)
            .bind(TR_Rewards_Pool_id)
            .bind(TR_Rewards_timestamp)
            .bind(TR_Rewards_amnt_stable)
            .bind(TR_Rewards_amnt_nls)
            .bind(Event_Block_Index)
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
        T: IntoIterator<Item = &'r TR_Rewards_Distribution>,
    {
        const SQL: &str = r#"
        INSERT INTO "TR_Rewards_Distribution" (
            "TR_Rewards_height",
            "TR_Rewards_Pool_id",
            "TR_Rewards_timestamp",
            "TR_Rewards_amnt_stable",
            "TR_Rewards_amnt_nls",
            "Event_Block_Index",
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
                 &TR_Rewards_Distribution {
                     TR_Rewards_height,
                     TR_Rewards_idx: _,
                     ref TR_Rewards_Pool_id,
                     TR_Rewards_timestamp,
                     ref TR_Rewards_amnt_stable,
                     ref TR_Rewards_amnt_nls,
                     Event_Block_Index,
                     ref Tx_Hash,
                 }| {
                    b.push_bind(TR_Rewards_height)
                        .push_bind(&TR_Rewards_Pool_id)
                        .push_bind(TR_Rewards_timestamp)
                        .push_bind(&TR_Rewards_amnt_stable)
                        .push_bind(&TR_Rewards_amnt_nls)
                        .push_bind(Event_Block_Index)
                        .push_bind(&Tx_Hash);
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
    ) -> Result<BigDecimal, Error> {
        const SQL: &str = r#"
        SELECT COALESCE(SUM("TR_Rewards_amnt_stable"), 0)
        FROM "TR_Rewards_Distribution"
        WHERE
            "TR_Rewards_timestamp" > $1 AND
            "TR_Rewards_timestamp" <= $2
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
        SELECT COALESCE(SUM("TR_Rewards_amnt_nls"), 0)
        FROM "TR_Rewards_Distribution"
        WHERE
            "TR_Rewards_timestamp" > $1 AND
            "TR_Rewards_timestamp" <= $2
        "#;

        sqlx::query_as(SQL)
            .bind(from)
            .bind(to)
            .fetch_one(&self.pool)
            .await
            .map(|(result,)| result)
    }

    pub async fn get_distributed(&self) -> Result<BigDecimal, Error> {
        const SQL: &str = r#"
        SELECT COALESCE(SUM("TR_Rewards_amnt_nls") / 1000000, 0) AS "Distributed"
        FROM "TR_Rewards_Distribution"
        "#;

        sqlx::query_as(SQL)
            .fetch_one(&self.pool)
            .await
            .map(|(result,)| result)
    }
}
