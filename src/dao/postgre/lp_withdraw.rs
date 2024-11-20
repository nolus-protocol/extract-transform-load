use super::{DataBase, QueryResult};
use crate::model::{LP_Withdraw, Table};
use chrono::{DateTime, Utc};
use sqlx::{error::Error, types::BigDecimal, QueryBuilder, Transaction};
use std::str::FromStr;

impl Table<LP_Withdraw> {
    pub async fn isExists(
        &self,
        lp_widthdraw: &LP_Withdraw,
    ) -> Result<bool, crate::error::Error> {
        let (value,): (i64,) = sqlx::query_as(
            r#"
            SELECT 
                COUNT(*)
            FROM "LP_Withdraw" 
            WHERE 
                "LP_withdraw_height" = $1 AND
                "LP_address_id" = $2 AND
                "LP_timestamp" = $3 AND
                "LP_Pool_id" = $4
            "#,
        )
        .bind(lp_widthdraw.LP_withdraw_height)
        .bind(&lp_widthdraw.LP_address_id)
        .bind(lp_widthdraw.LP_timestamp)
        .bind(&lp_widthdraw.LP_Pool_id)
        .fetch_one(&self.pool)
        .await?;

        if value > 0 {
            return Ok(true);
        }

        Ok(false)
    }

    pub async fn insert(
        &self,
        data: LP_Withdraw,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<QueryResult, Error> {
        sqlx::query(
            r#"
            INSERT INTO "LP_Withdraw" (
                "LP_withdraw_height",
                "LP_address_id",
                "LP_timestamp",
                "LP_Pool_id",
                "LP_amnt_stable",
                "LP_amnt_asset",
                "LP_amnt_receipts",
                "LP_deposit_close",
                "Tx_Hash"
            )
            VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9)
        "#,
        )
        .bind(data.LP_withdraw_height)
        .bind(&data.LP_address_id)
        .bind(data.LP_timestamp)
        .bind(&data.LP_Pool_id)
        .bind(&data.LP_amnt_stable)
        .bind(&data.LP_amnt_asset)
        .bind(&data.LP_amnt_receipts)
        .bind(data.LP_deposit_close)
        .bind(data.Tx_Hash)
        .execute(&mut **transaction)
        .await
    }

    pub async fn insert_many(
        &self,
        data: &Vec<LP_Withdraw>,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<(), Error> {
        if data.is_empty() {
            return Ok(());
        }

        let mut query_builder: QueryBuilder<DataBase> = QueryBuilder::new(
            r#"
            INSERT INTO "LP_Withdraw" (
                "LP_withdraw_height",
                "LP_address_id",
                "LP_timestamp",
                "LP_Pool_id",
                "LP_amnt_stable",
                "LP_amnt_asset",
                "LP_amnt_receipts",
                "LP_deposit_close",
                "Tx_Hash"
            )"#,
        );

        query_builder.push_values(data, |mut b, lp| {
            b.push_bind(lp.LP_withdraw_height)
                .push_bind(&lp.LP_address_id)
                .push_bind(lp.LP_timestamp)
                .push_bind(&lp.LP_Pool_id)
                .push_bind(&lp.LP_amnt_stable)
                .push_bind(&lp.LP_amnt_asset)
                .push_bind(&lp.LP_amnt_receipts)
                .push_bind(lp.LP_deposit_close)
                .push_bind(&lp.Tx_Hash);
        });

        let query = query_builder.build();
        query.execute(&mut **transaction).await?;
        Ok(())
    }

    pub async fn count_closed(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<i64, crate::error::Error> {
        let (value,): (i64,) = sqlx::query_as(
            r#"
            SELECT 
                COUNT(*)
            FROM "LP_Withdraw" WHERE "LP_timestamp" > $1 AND "LP_timestamp" <= $2 AND "LP_deposit_close" = true
            "#,
        )
        .bind(from)
        .bind(to)
        .fetch_one(&self.pool)
        .await?;
        Ok(value)
    }

    pub async fn get_amnt_stable(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<BigDecimal, crate::error::Error> {
        let value: (Option<BigDecimal>,) = sqlx::query_as(
            r#"
            SELECT 
                SUM("LP_amnt_stable")
            FROM "LP_Withdraw" WHERE "LP_timestamp" > $1 AND "LP_timestamp" <= $2
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
