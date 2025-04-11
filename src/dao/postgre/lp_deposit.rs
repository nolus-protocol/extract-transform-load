use super::{DataBase, QueryResult};
use crate::model::{LP_Deposit, Table};
use chrono::{DateTime, Utc};
use sqlx::{error::Error, types::BigDecimal, QueryBuilder, Transaction};
use std::str::FromStr;

impl Table<LP_Deposit> {
    pub async fn isExists(
        &self,
        ls_deposit: &LP_Deposit,
    ) -> Result<bool, Error> {
        let (value,): (i64,) = sqlx::query_as(
            r#"
            SELECT 
                COUNT(*)
            FROM "LP_Deposit" 
            WHERE 
                "LP_deposit_height" = $1 AND
                "LP_address_id" = $2 AND
                "LP_timestamp" = $3 AND
                "LP_Pool_id" = $4
            "#,
        )
        .bind(ls_deposit.LP_deposit_height)
        .bind(&ls_deposit.LP_address_id)
        .bind(ls_deposit.LP_timestamp)
        .bind(&ls_deposit.LP_Pool_id)
        .persistent(false)
        .fetch_one(&self.pool)
        .await?;

        if value > 0 {
            return Ok(true);
        }

        Ok(false)
    }

    pub async fn insert(
        &self,
        data: LP_Deposit,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<QueryResult, Error> {
        sqlx::query(
            r#"
            INSERT INTO "LP_Deposit" (
                "LP_deposit_height",
                "LP_address_id",
                "LP_timestamp",
                "LP_Pool_id",
                "LP_amnt_stable",
                "LP_amnt_asset",
                "LP_amnt_receipts",
                "Tx_Hash"
            )
            VALUES($1, $2, $3, $4, $5, $6, $7, $8)
        "#,
        )
        .bind(data.LP_deposit_height)
        .bind(&data.LP_address_id)
        .bind(data.LP_timestamp)
        .bind(&data.LP_Pool_id)
        .bind(&data.LP_amnt_stable)
        .bind(&data.LP_amnt_asset)
        .bind(&data.LP_amnt_receipts)
        .bind(&data.Tx_Hash)
        .persistent(false)
        .execute(&mut **transaction)
        .await
    }

    pub async fn insert_many(
        &self,
        data: &Vec<LP_Deposit>,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<(), Error> {
        if data.is_empty() {
            return Ok(());
        }

        let mut query_builder: QueryBuilder<DataBase> = QueryBuilder::new(
            r#"
            INSERT INTO "LP_Deposit" (
                "LP_deposit_height",
                "LP_address_id",
                "LP_timestamp",
                "LP_Pool_id",
                "LP_amnt_stable",
                "LP_amnt_asset",
                "LP_amnt_receipts",
                "Tx_Hash"
            )"#,
        );

        query_builder.push_values(data, |mut b, lp| {
            b.push_bind(lp.LP_deposit_height)
                .push_bind(&lp.LP_address_id)
                .push_bind(lp.LP_timestamp)
                .push_bind(&lp.LP_Pool_id)
                .push_bind(&lp.LP_amnt_stable)
                .push_bind(&lp.LP_amnt_asset)
                .push_bind(&lp.LP_amnt_receipts)
                .push_bind(&lp.Tx_Hash);
        });

        let query = query_builder.build().persistent(false);
        query.execute(&mut **transaction).await?;

        Ok(())
    }

    pub async fn count(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<i64, crate::error::Error> {
        let (value,): (i64,) = sqlx::query_as(
            r#"
            SELECT 
                COUNT(*)
            FROM "LP_Deposit" WHERE "LP_timestamp" > $1 AND "LP_timestamp" <= $2
            "#,
        )
        .bind(from)
        .bind(to)
        .persistent(false)
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
            FROM "LP_Deposit" WHERE "LP_timestamp" > $1 AND "LP_timestamp" <= $2
            "#,
        )
        .bind(from)
        .bind(to)
        .persistent(false)
        .fetch_one(&self.pool)
        .await?;
        let (amnt,) = value;
        let amnt = amnt.unwrap_or(BigDecimal::from_str("0")?);

        Ok(amnt)
    }
}
