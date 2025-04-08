use chrono::{DateTime, Utc};
use sqlx::{Error, QueryBuilder};

use crate::model::{LP_Lender_State, Table};

impl Table<LP_Lender_State> {
    // FIXME Pass data by reference, as separate arguments or as a dedicated
    //  structure. Avoid the need for owned data.
    pub async fn insert(&self, data: LP_Lender_State) -> Result<(), Error> {
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
            .bind(&data.LP_Lender_id)
            .bind(&data.LP_Pool_id)
            .bind(data.LP_timestamp)
            .bind(&data.LP_Lender_stable)
            .bind(&data.LP_Lender_asset)
            .bind(&data.LP_Lender_receipts)
            .execute(&self.pool)
            .await
            .map(drop)
    }

    // FIXME Return data in a dedicated structure instead of as a tuple.
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
                    "deposit"."LP_address_id" = "withdraw"."LP_address_id" AND
                    "deposit"."LP_Pool_id" = "withdraw"."LP_Pool_id"
                ORDER BY "LP_timestamp" DESC
                LIMIT 1
            ),
            to_timestamp(0)
        )
        GROUP BY "LP_address_id", "LP_Pool_id"
        "#;

        sqlx::query_as(SQL).fetch_all(&self.pool).await
    }

    // FIXME Pass data by reference, as separate arguments or as a dedicated
    //  structure. Avoid the need for owned data.
    // FIXME Use iterators instead.
    pub async fn insert_many(
        &self,
        data: &Vec<LP_Lender_State>,
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
        "#;

        if data.is_empty() {
            return Ok(());
        }

        QueryBuilder::new(SQL)
            .push_values(data, |mut b, data| {
                b.push_bind(&data.LP_Lender_id)
                    .push_bind(&data.LP_Pool_id)
                    .push_bind(data.LP_timestamp)
                    .push_bind(&data.LP_Lender_stable)
                    .push_bind(&data.LP_Lender_asset)
                    .push_bind(&data.LP_Lender_receipts);
            })
            .build()
            .persistent(false)
            .execute(&self.pool)
            .await
            .map(drop)
    }

    // FIXME Use `UInt63` instead.
    pub async fn count(&self, timestamp: DateTime<Utc>) -> Result<i64, Error> {
        const SQL: &str = r#"
        SELECT
            COUNT(*)
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
