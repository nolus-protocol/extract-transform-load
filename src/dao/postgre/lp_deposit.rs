use bigdecimal::Zero as _;
use chrono::{DateTime, Utc};
use sqlx::{types::BigDecimal, Error, QueryBuilder, Transaction};

use crate::model::{LP_Deposit, Table};

use super::DataBase;

impl Table<LP_Deposit> {
    pub async fn isExists(
        &self,
        ls_deposit: &LP_Deposit,
    ) -> Result<bool, Error> {
        const SQL: &str = r#"
        SELECT EXISTS(
            SELECT
            FROM "LP_Deposit"
            WHERE
                "LP_deposit_height" = $1 AND
                "LP_address_id" = $2 AND
                "LP_timestamp" = $3 AND
                "LP_Pool_id" = $4
        )
        "#;

        sqlx::query_as(SQL)
            .bind(ls_deposit.LP_deposit_height)
            .bind(&ls_deposit.LP_address_id)
            .bind(ls_deposit.LP_timestamp)
            .bind(&ls_deposit.LP_Pool_id)
            .fetch_one(&self.pool)
            .await
            .map(|(result,)| result)
    }

    pub async fn insert(
        &self,
        data: LP_Deposit,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<(), Error> {
        const SQL: &str = r#"
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
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        "#;

        sqlx::query(SQL)
            .bind(data.LP_deposit_height)
            .bind(&data.LP_address_id)
            .bind(data.LP_timestamp)
            .bind(&data.LP_Pool_id)
            .bind(&data.LP_amnt_stable)
            .bind(&data.LP_amnt_asset)
            .bind(&data.LP_amnt_receipts)
            .bind(&data.Tx_Hash)
            .execute(transaction.as_mut())
            .await
            .map(drop)
    }

    pub async fn insert_many(
        &self,
        data: &Vec<LP_Deposit>,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<(), Error> {
        const SQL: &str = r#"
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
        "#;

        if data.is_empty() {
            return Ok(());
        }

        QueryBuilder::new(SQL)
            .push_values(data, |mut b, lp| {
                b.push_bind(lp.LP_deposit_height)
                    .push_bind(&lp.LP_address_id)
                    .push_bind(lp.LP_timestamp)
                    .push_bind(&lp.LP_Pool_id)
                    .push_bind(&lp.LP_amnt_stable)
                    .push_bind(&lp.LP_amnt_asset)
                    .push_bind(&lp.LP_amnt_receipts)
                    .push_bind(&lp.Tx_Hash);
            })
            .build()
            .persistent(false)
            .execute(transaction.as_mut())
            .await
            .map(drop)
    }

    pub async fn count(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<i64, Error> {
        const SQL: &str = r#"
        SELECT
            COUNT(*)
        FROM "LP_Deposit"
        WHERE
            "LP_timestamp" > $1 AND
            "LP_timestamp" <= $2
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
        FROM "LP_Deposit"
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
