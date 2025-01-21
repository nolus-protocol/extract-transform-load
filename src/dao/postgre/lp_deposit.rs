use std::iter;

use chrono::{DateTime, Utc};
use sqlx::{error::Error, types::BigDecimal, QueryBuilder, Transaction};

use crate::{
    custom_uint::UInt63,
    model::{LP_Deposit, Table},
};

use super::DataBase;

impl Table<LP_Deposit> {
    pub async fn isExists(
        &self,
        &LP_Deposit {
            LP_deposit_height,
            ref LP_address_id,
            LP_timestamp,
            ref LP_Pool_id,
            ..
        }: &LP_Deposit,
    ) -> Result<bool, Error> {
        const SQL: &str = r#"
        SELECT COUNT(1) > 0
        FROM "LP_Deposit"
        WHERE
            "LP_deposit_height" = $1 AND
            "LP_address_id" = $2 AND
            "LP_timestamp" = $3 AND
            "LP_Pool_id" = $4
        "#;

        sqlx::query_as(SQL)
            .bind(LP_deposit_height)
            .bind(LP_address_id)
            .bind(LP_timestamp)
            .bind(LP_Pool_id)
            .fetch_one(&self.pool)
            .await
            .map(|(result,)| result)
    }

    pub async fn insert(
        &self,
        &LP_Deposit {
            ref Tx_Hash,
            LP_deposit_height,
            LP_deposit_idx: _,
            ref LP_address_id,
            LP_timestamp,
            ref LP_Pool_id,
            ref LP_amnt_stable,
            ref LP_amnt_asset,
            ref LP_amnt_receipts,
        }: &LP_Deposit,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<(), Error> {
        const SQL: &str = r#"
        INSERT INTO "LP_Deposit" (
            "Tx_Hash",
            "LP_deposit_height",
            "LP_address_id",
            "LP_timestamp",
            "LP_Pool_id",
            "LP_amnt_stable",
            "LP_amnt_asset",
            "LP_amnt_receipts"
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        "#;

        sqlx::query(SQL)
            .bind(Tx_Hash)
            .bind(LP_deposit_height)
            .bind(LP_address_id)
            .bind(LP_timestamp)
            .bind(LP_Pool_id)
            .bind(LP_amnt_stable)
            .bind(LP_amnt_asset)
            .bind(LP_amnt_receipts)
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
        T: IntoIterator<Item = &'r LP_Deposit>,
    {
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

        let mut iter = data.into_iter();

        let Some(first) = iter.next() else {
            return Ok(());
        };

        QueryBuilder::new(SQL)
            .push_values(
                iter::once(first).chain(iter),
                |mut b,
                 &LP_Deposit {
                     ref Tx_Hash,
                     LP_deposit_height,
                     LP_deposit_idx: _,
                     ref LP_address_id,
                     LP_timestamp,
                     ref LP_Pool_id,
                     ref LP_amnt_stable,
                     ref LP_amnt_asset,
                     ref LP_amnt_receipts,
                 }| {
                    b.push_bind(Tx_Hash)
                        .push_bind(LP_deposit_height)
                        .push_bind(LP_address_id)
                        .push_bind(LP_timestamp)
                        .push_bind(LP_Pool_id)
                        .push_bind(LP_amnt_stable)
                        .push_bind(LP_amnt_asset)
                        .push_bind(LP_amnt_receipts);
                },
            )
            .build()
            .execute(&mut **transaction)
            .await
            .map(drop)
    }

    pub async fn count(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<UInt63, Error> {
        const SQL: &str = r#"
        SELECT COUNT(1)
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

    pub async fn get_amount_stable(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<BigDecimal, Error> {
        const SQL: &str = r#"
        SELECT COALESCE(SUM("LP_amnt_stable"), 0)
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
            .map(|(amnt,)| amnt)
    }
}
