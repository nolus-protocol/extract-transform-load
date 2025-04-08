use bigdecimal::Zero as _;
use chrono::{DateTime, Utc};
use sqlx::{types::BigDecimal, Error, QueryBuilder, Transaction};

use crate::model::{LP_Withdraw, Table};

use super::DataBase;

impl Table<LP_Withdraw> {
    // FIXME Pass data by reference, as separate arguments or as a dedicated
    //  structure. Avoid the need for owned data.
    pub async fn isExists(
        &self,
        lp_widthdraw: &LP_Withdraw,
    ) -> Result<bool, Error> {
        const SQL: &str = r#"
        SELECT EXISTS(
            SELECT
            FROM "LP_Withdraw"
            WHERE
                "LP_withdraw_height" = $1 AND
                "LP_address_id" = $2 AND
                "LP_timestamp" = $3 AND
                "LP_Pool_id" = $4
        )
        "#;

        sqlx::query_as(SQL)
            .bind(lp_widthdraw.LP_withdraw_height)
            .bind(&lp_widthdraw.LP_address_id)
            .bind(lp_widthdraw.LP_timestamp)
            .bind(&lp_widthdraw.LP_Pool_id)
            .fetch_one(&self.pool)
            .await
            .map(|(result,)| result)
    }

    // FIXME Pass data by reference, as separate arguments or as a dedicated
    //  structure. Avoid the need for owned data.
    pub async fn insert(
        &self,
        data: LP_Withdraw,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<(), Error> {
        const SQL: &str = r#"
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
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        "#;

        sqlx::query(SQL)
            .bind(data.LP_withdraw_height)
            .bind(&data.LP_address_id)
            .bind(data.LP_timestamp)
            .bind(&data.LP_Pool_id)
            .bind(&data.LP_amnt_stable)
            .bind(&data.LP_amnt_asset)
            .bind(&data.LP_amnt_receipts)
            .bind(data.LP_deposit_close)
            .bind(data.Tx_Hash)
            .execute(transaction.as_mut())
            .await
            .map(drop)
    }

    // FIXME Pass data by reference, as separate arguments or as a dedicated
    //  structure. Avoid the need for owned data.
    // FIXME Use iterators instead.
    pub async fn insert_many(
        &self,
        data: &Vec<LP_Withdraw>,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<(), Error> {
        const SQL: &str = r#"
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
        "#;

        if data.is_empty() {
            return Ok(());
        }

        QueryBuilder::new(SQL)
            .push_values(data, |mut b, lp| {
                b.push_bind(lp.LP_withdraw_height)
                    .push_bind(&lp.LP_address_id)
                    .push_bind(lp.LP_timestamp)
                    .push_bind(&lp.LP_Pool_id)
                    .push_bind(&lp.LP_amnt_stable)
                    .push_bind(&lp.LP_amnt_asset)
                    .push_bind(&lp.LP_amnt_receipts)
                    .push_bind(lp.LP_deposit_close)
                    .push_bind(&lp.Tx_Hash);
            })
            .build()
            .persistent(false)
            .execute(transaction.as_mut())
            .await
            .map(drop)
    }

    // FIXME Use `UInt63` instead.
    pub async fn count_closed(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<i64, Error> {
        const SQL: &str = r#"
        SELECT
            COUNT(*)
        FROM "LP_Withdraw"
        WHERE
            "LP_timestamp" > $1 AND
            "LP_timestamp" <= $2 AND
            "LP_deposit_close" = true
        "#;

        sqlx::query_as(SQL)
            .bind(from)
            .bind(to)
            .fetch_one(&self.pool)
            .await
            .map(|(result,)| result)
    }

    pub async fn get_amnt_stable(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<BigDecimal, Error> {
        const SQL: &str = r#"
        SELECT
            SUM("LP_amnt_stable")
        FROM "LP_Withdraw"
        WHERE
            "LP_timestamp" > $1 AND
            "LP_timestamp" <= $2
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
}
