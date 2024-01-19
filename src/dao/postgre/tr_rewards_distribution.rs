use super::{DataBase, QueryResult};
use crate::model::{TR_Rewards_Distribution, Table};
use chrono::{DateTime, Utc};
use sqlx::{error::Error, types::BigDecimal, QueryBuilder, Transaction};
use std::str::FromStr;

impl Table<TR_Rewards_Distribution> {
    pub async fn isExists(
        &self,
        tr_reward: &TR_Rewards_Distribution,
    ) -> Result<bool, crate::error::Error> {
        let (value,): (i64,) = sqlx::query_as(
            r#"
            SELECT 
                COUNT(*)
            FROM "TR_Rewards_Distribution" 
            WHERE 
                "TR_Rewards_height" = $1 AND
                "TR_Rewards_Pool_id" = $2 AND
                "Event_Block_Index" = $3
            "#,
        )
        .bind(tr_reward.TR_Rewards_height)
        .bind(&tr_reward.TR_Rewards_Pool_id)
        .bind(tr_reward.Event_Block_Index)
        .fetch_one(&self.pool)
        .await?;

        if value > 0 {
            return Ok(true);
        }

        Ok(false)
    }

    pub async fn insert(
        &self,
        data: TR_Rewards_Distribution,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<QueryResult, Error> {
        sqlx::query(
            r#"
            INSERT INTO "TR_Rewards_Distribution" (
                "TR_Rewards_height",
                "TR_Rewards_Pool_id",
                "TR_Rewards_timestamp",
                "TR_Rewards_amnt_stable",
                "TR_Rewards_amnt_nls",
                "Event_Block_Index"
            )
            VALUES($1, $2, $3, $4, $5, $6)
        "#,
        )
        .bind(data.TR_Rewards_height)
        .bind(&data.TR_Rewards_Pool_id)
        .bind(data.TR_Rewards_timestamp)
        .bind(&data.TR_Rewards_amnt_stable)
        .bind(&data.TR_Rewards_amnt_nls)
        .bind(data.Event_Block_Index)
        .execute(&mut **transaction)
        .await
    }

    pub async fn insert_many(
        &self,
        data: &Vec<TR_Rewards_Distribution>,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<(), Error> {
        if data.is_empty() {
            return Ok(());
        }

        let mut query_builder: QueryBuilder<DataBase> = QueryBuilder::new(
            r#"
            INSERT INTO "TR_Rewards_Distribution" (
                "TR_Rewards_height",
                "TR_Rewards_Pool_id",
                "TR_Rewards_timestamp",
                "TR_Rewards_amnt_stable",
                "TR_Rewards_amnt_nls",
                "Event_Block_Index"
            )"#,
        );

        query_builder.push_values(data, |mut b, tr| {
            b.push_bind(tr.TR_Rewards_height)
                .push_bind(&tr.TR_Rewards_Pool_id)
                .push_bind(tr.TR_Rewards_timestamp)
                .push_bind(&tr.TR_Rewards_amnt_stable)
                .push_bind(&tr.TR_Rewards_amnt_nls)
                .push_bind(tr.Event_Block_Index);
        });

        let query = query_builder.build();
        query.execute(&mut **transaction).await?;
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
                SUM("TR_Rewards_amnt_stable")
            FROM "TR_Rewards_Distribution" WHERE "TR_Rewards_timestamp" > $1 AND "TR_Rewards_timestamp" <= $2
            "#,
        )
        .bind(from)
        .bind(to)
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
                SUM("TR_Rewards_amnt_nls")
            FROM "TR_Rewards_Distribution" WHERE "TR_Rewards_timestamp" > $1 AND "TR_Rewards_timestamp" <= $2
            "#,
        )
        .bind(from)
        .bind(to)
        .fetch_one(&self.pool)
        .await?;
        let (amnt,) = value;
        let amnt = amnt.unwrap_or(BigDecimal::from_str("0")?);

        Ok(amnt)
    }

    pub async fn get_distributed(&self) -> Result<BigDecimal, crate::error::Error> {
        let value: (Option<BigDecimal>,)  = sqlx::query_as(
            r#"
                SELECT SUM("TR_Rewards_amnt_nls") / 1000000 AS "Distributed" FROM "TR_Rewards_Distribution"
            "#,
        )
        .fetch_one(&self.pool)
        .await?;

        let (amnt,) = value;
        let amnt = amnt.unwrap_or(BigDecimal::from_str("0")?);

        Ok(amnt)
    }
}
