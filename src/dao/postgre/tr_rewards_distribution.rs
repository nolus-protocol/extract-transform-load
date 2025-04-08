use bigdecimal::Zero as _;
use chrono::{DateTime, Utc};
use sqlx::{types::BigDecimal, Error, QueryBuilder, Transaction};

use crate::model::{TR_Rewards_Distribution, Table};

use super::DataBase;

impl Table<TR_Rewards_Distribution> {
    // FIXME Pass data by reference, as separate arguments or as a dedicated
    //  structure.
    pub async fn isExists(
        &self,
        tr_reward: &TR_Rewards_Distribution,
    ) -> Result<bool, Error> {
        const SQL: &str = r#"
        SELECT EXISTS(
            SELECT
            FROM "TR_Rewards_Distribution"
            WHERE
                "TR_Rewards_height" = $1 AND
                "TR_Rewards_Pool_id" = $2 AND
                "Event_Block_Index" = $3
        )
        "#;

        sqlx::query_as(SQL)
            .bind(tr_reward.TR_Rewards_height)
            .bind(&tr_reward.TR_Rewards_Pool_id)
            .bind(tr_reward.Event_Block_Index)
            .fetch_one(&self.pool)
            .await
            .map(|(result,)| result)
    }

    pub async fn insert(
        &self,
        data: TR_Rewards_Distribution,
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
            .bind(data.TR_Rewards_height)
            .bind(&data.TR_Rewards_Pool_id)
            .bind(data.TR_Rewards_timestamp)
            .bind(&data.TR_Rewards_amnt_stable)
            .bind(&data.TR_Rewards_amnt_nls)
            .bind(data.Event_Block_Index)
            .bind(data.Tx_Hash)
            .execute(&mut **transaction)
            .await
            .map(drop)
    }

    // FIXME Pass data by reference, as separate arguments or as a dedicated
    //  structure.
    // FIXME Use iterators instead.
    pub async fn insert_many(
        &self,
        data: &Vec<TR_Rewards_Distribution>,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<(), Error> {
        if data.is_empty() {
            return Ok(());
        }

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

        QueryBuilder::new(SQL)
            .push_values(data, |mut b, tr| {
                b.push_bind(tr.TR_Rewards_height)
                    .push_bind(&tr.TR_Rewards_Pool_id)
                    .push_bind(tr.TR_Rewards_timestamp)
                    .push_bind(&tr.TR_Rewards_amnt_stable)
                    .push_bind(&tr.TR_Rewards_amnt_nls)
                    .push_bind(tr.Event_Block_Index)
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
    ) -> Result<BigDecimal, Error> {
        const SQL: &str = r#"
        SELECT
            SUM("TR_Rewards_amnt_stable")
        FROM "TR_Rewards_Distribution"
        WHERE
            "TR_Rewards_timestamp" > $1 AND
            "TR_Rewards_timestamp" <= $2
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
            SUM("TR_Rewards_amnt_nls")
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
        SELECT
            (
                SUM("TR_Rewards_amnt_nls") / 1000000
            )
        FROM "TR_Rewards_Distribution"
        "#;

        sqlx::query_as(SQL)
            .fetch_optional(&self.pool)
            .await
            .map(|result| {
                result.map_or_else(BigDecimal::zero, |(result,)| result)
            })
    }
}
