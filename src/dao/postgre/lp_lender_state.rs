use std::iter;

use chrono::{DateTime, Utc};
use sqlx::{error::Error, QueryBuilder};

use crate::{
    custom_uint::UInt63,
    model::{LP_Lender_State, Table},
};

impl Table<LP_Lender_State> {
    pub async fn insert(
        &self,
        &LP_Lender_State {
            ref LP_Lender_id,
            ref LP_Pool_id,
            LP_timestamp,
            ref LP_Lender_stable,
            ref LP_Lender_asset,
            ref LP_Lender_receipts,
        }: &LP_Lender_State,
    ) -> Result<(), Error> {
        const SQL: &str = r#"
        INSERT INTO "LP_Lender_State" (
            "LP_Lender_id",
            "LP_Pool_id",
            "LP_timestamp",
            "LP_Lender_stable",
            "LP_Lender_asset",
            "LP_Lender_receipts"
        )
        VALUES ($1, $2, $3, $4, $5, $6)
        "#;

        sqlx::query(SQL)
            .bind(LP_Lender_id)
            .bind(LP_Pool_id)
            .bind(LP_timestamp)
            .bind(LP_Lender_stable)
            .bind(LP_Lender_asset)
            .bind(LP_Lender_receipts)
            .execute(&self.pool)
            .await
            .map(drop)
    }

    pub async fn get_active_states(
        &self,
    ) -> Result<Vec<(String, String)>, Error> {
        const SQL: &str = r#"
        SELECT
            "deposit"."LP_address_id",
            "deposit"."LP_Pool_id"
        FROM "LP_Deposit" AS "deposit"
        WHERE "deposit"."LP_timestamp" > COALESCE(
            (
                SELECT "LP_timestamp"
                FROM "LP_Withdraw" AS "withdraw"
                WHERE
                    "LP_deposit_close" = true AND
                    "withdraw"."LP_address_id" = "deposit"."LP_address_id" AND
                    "withdraw"."LP_Pool_id" = "deposit"."LP_Pool_id"
                ORDER BY "LP_timestamp" DESC
                LIMIT 1
            ),
            to_timestamp(0)
        )
        GROUP BY
            "LP_address_id",
            "LP_Pool_id"
        "#;

        sqlx::query_as(SQL).fetch_all(&self.pool).await
    }

    pub async fn insert_many<'r, T>(&self, data: T) -> Result<(), Error>
    where
        T: IntoIterator<Item = &'r LP_Lender_State>,
    {
        const SQL: &str = r#"
        INSERT INTO "LP_Lender_State" (
            "LP_Lender_id",
            "LP_Pool_id",
            "LP_timestamp",
            "LP_Lender_stable",
            "LP_Lender_asset",
            "LP_Lender_receipts"
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
                 &LP_Lender_State {
                     ref LP_Lender_id,
                     ref LP_Pool_id,
                     LP_timestamp,
                     ref LP_Lender_stable,
                     ref LP_Lender_asset,
                     ref LP_Lender_receipts,
                 }| {
                    b.push_bind(LP_Lender_id)
                        .push_bind(LP_Pool_id)
                        .push_bind(LP_timestamp)
                        .push_bind(LP_Lender_stable)
                        .push_bind(LP_Lender_asset)
                        .push_bind(LP_Lender_receipts);
                },
            )
            .build()
            .execute(&self.pool)
            .await
            .map(drop)
    }

    pub async fn count(
        &self,
        timestamp: DateTime<Utc>,
    ) -> Result<UInt63, Error> {
        const SQL: &str = r#"
        SELECT COUNT(1)
        FROM "LP_Lender_State"
        WHERE "LP_timestamp" = $1
        "#;

        sqlx::query_as(SQL)
            .bind(timestamp)
            .fetch_one(&self.pool)
            .await
            .map(|(result,)| result)
    }
}
