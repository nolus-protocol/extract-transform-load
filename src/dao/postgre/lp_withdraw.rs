use std::str::FromStr as _;

use chrono::{DateTime, Utc};
use sqlx::{types::BigDecimal, Error, QueryBuilder, Transaction};

use crate::model::{LP_Withdraw, Table};

use super::{DataBase, QueryResult};

impl Table<LP_Withdraw> {
    /// Inserts a record if it doesn't already exist, using ON CONFLICT DO NOTHING.
    pub async fn insert_if_not_exists(
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
            ON CONFLICT ("LP_withdraw_height", "LP_address_id", "LP_timestamp", "LP_Pool_id") DO NOTHING
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
        .persistent(true)
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

        let query = query_builder.build().persistent(true);
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
        .persistent(true)
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
        .persistent(true)
        .fetch_one(&self.pool)
        .await?;
        let (amnt,) = value;
        let amnt = amnt.unwrap_or(BigDecimal::from_str("0")?);

        Ok(amnt)
    }

    pub async fn get_by_tx(
        &self,
        tx: String,
    ) -> Result<Option<LP_Withdraw>, Error> {
        sqlx::query_as(
            r#"
             select * from "LP_Withdraw" WHERE "Tx_Hash" = $1
            "#,
        )
        .bind(tx)
        .persistent(true)
        .fetch_optional(&self.pool)
        .await
    }
}
