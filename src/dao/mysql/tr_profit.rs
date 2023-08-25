use super::{DataBase, QueryResult};
use crate::model::{TR_Profit, Table, Buyback};
use chrono::{DateTime, Utc};
use sqlx::{error::Error, types::BigDecimal, QueryBuilder, Transaction};
use std::str::FromStr;

impl Table<TR_Profit> {
    pub async fn insert(
        &self,
        data: TR_Profit,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<QueryResult, Error> {
        sqlx::query(
            r#"
            INSERT INTO `TR_Profit` (
                `TR_Profit_height`,
                `TR_Profit_timestamp`,
                `TR_Profit_amnt_stable`,
                `TR_Profit_amnt_nls`
            )
            VALUES(?, ?, ?, ?)
        "#,
        )
        .bind(data.TR_Profit_height)
        .bind(data.TR_Profit_timestamp)
        .bind(&data.TR_Profit_amnt_stable)
        .bind(&data.TR_Profit_amnt_nls)
        .execute(&mut **transaction)
        .await
    }

    pub async fn insert_many(
        &self,
        data: &Vec<TR_Profit>,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<(), Error> {
        if data.is_empty() {
            return Ok(());
        }

        let mut query_builder: QueryBuilder<DataBase> = QueryBuilder::new(
            r#"
            INSERT INTO `TR_Profit` (
                `TR_Profit_height`,
                `TR_Profit_timestamp`,
                `TR_Profit_amnt_stable`,
                `TR_Profit_amnt_nls`
            )"#,
        );

        query_builder.push_values(data, |mut b, tr| {
            b.push_bind(tr.TR_Profit_height)
                .push_bind(tr.TR_Profit_timestamp)
                .push_bind(&tr.TR_Profit_amnt_stable)
                .push_bind(&tr.TR_Profit_amnt_nls);
        });

        let query = query_builder.build();
        query.execute(&mut **transaction).await?;
        Ok(())
    }

    pub async fn get_amnt_stable(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<(BigDecimal, BigDecimal), crate::error::Error> {
        let value: (Option<BigDecimal>, Option<BigDecimal>) = sqlx::query_as(
            r#"
            SELECT 
                SUM(`TR_Profit_amnt_stable`),
                SUM(`TR_Profit_amnt_nls`)
            FROM `TR_Profit` WHERE `TR_Profit_timestamp` > ? AND `TR_Profit_timestamp` <= ?
            "#,
        )
        .bind(from)
        .bind(to)
        .fetch_one(&self.pool)
        .await?;
        let (amnt, amnt_nolus) = value;
        let amnt = amnt.unwrap_or(BigDecimal::from_str("0")?);
        let amnt_nolus = amnt_nolus.unwrap_or(BigDecimal::from_str("0")?);

        Ok((amnt, amnt_nolus))
    }

    pub async fn get_buyback(&self, skip: i64, limit: i64) -> Result<Vec<Buyback>, Error> { 
        let data = sqlx::query_as(
            r#"
                SELECT `TR_Profit_timestamp` AS time, (SUM(`TR_Profit_amnt_nls` / 1000000) OVER ( Order By `TR_Profit_timestamp`)) AS `Bought-back` FROM `TR_Profit` LIMIT ? OFFSET ? 
            "#,
        )
        .bind(limit)
        .bind(skip)
        .fetch_all(&self.pool)
        .await?;
        Ok(data)
    }

    pub async fn get_revenue(&self) -> Result<BigDecimal, crate::error::Error> {
        let value: (Option<BigDecimal>,) = sqlx::query_as(
            r#"
                SELECT SUM(`TR_Profit_amnt_stable`) / 1000000 AS `Distributed` FROM `TR_Profit`
            "#,
        )
        .fetch_one(&self.pool)
        .await?;

        let (amnt,) = value;
        let amnt = amnt.unwrap_or(BigDecimal::from_str("0")?);

        Ok(amnt)

    }
}
