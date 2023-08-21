use super::{DataBase, QueryResult};
use crate::model::{TR_Rewards_Distribution, Table};
use chrono::{DateTime, Utc};
use sqlx::{error::Error, types::BigDecimal, QueryBuilder, Transaction};
use std::str::FromStr;

impl Table<TR_Rewards_Distribution> {
    pub async fn insert(
        &self,
        data: TR_Rewards_Distribution,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<QueryResult, Error> {
        sqlx::query(
            r#"
            INSERT INTO `TR_Rewards_Distribution` (
                `TR_Rewards_height`,
                `TR_Rewards_Pool_id`,
                `TR_Rewards_timestamp`,
                `TR_Rewards_amnt_stable`,
                `TR_Rewards_amnt_nls`
            )
            VALUES(?, ?, ?, ?, ?)
        "#,
        )
        .bind(data.TR_Rewards_height)
        .bind(&data.TR_Rewards_Pool_id)
        .bind(data.TR_Rewards_timestamp)
        .bind(&data.TR_Rewards_amnt_stable)
        .bind(&data.TR_Rewards_amnt_nls)
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
            INSERT INTO `TR_Rewards_Distribution` (
                `TR_Rewards_height`,
                `TR_Rewards_Pool_id`,
                `TR_Rewards_timestamp`,
                `TR_Rewards_amnt_stable`,
                `TR_Rewards_amnt_nls`
            )"#,
        );

        query_builder.push_values(data, |mut b, tr| {
            b.push_bind(tr.TR_Rewards_height)
                .push_bind(&tr.TR_Rewards_Pool_id)
                .push_bind(tr.TR_Rewards_timestamp)
                .push_bind(&tr.TR_Rewards_amnt_stable)
                .push_bind(&tr.TR_Rewards_amnt_nls);
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
                SUM(`TR_Rewards_amnt_stable`)
            FROM `TR_Rewards_Distribution` WHERE `TR_Rewards_timestamp` > ? AND `TR_Rewards_timestamp` <= ?
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
                SUM(`TR_Rewards_amnt_nls`)
            FROM `TR_Rewards_Distribution` WHERE `TR_Rewards_timestamp` > ? AND `TR_Rewards_timestamp` <= ?
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
}
