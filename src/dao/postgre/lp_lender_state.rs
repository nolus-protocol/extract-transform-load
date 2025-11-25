use chrono::{DateTime, Utc};
use sqlx::{Error, QueryBuilder};

use crate::model::{LP_Lender_State, Table};

use super::{DataBase, QueryResult};

impl Table<LP_Lender_State> {
    pub async fn insert(
        &self,
        data: LP_Lender_State,
    ) -> Result<QueryResult, Error> {
        sqlx::query(
            r#"
            INSERT INTO "LP_Lender_State" (
                "LP_Lender_id",
                "LP_Pool_id",
                "LP_timestamp",
                "LP_Lender_stable",
                "LP_Lender_asset",
                "LP_Lender_receipts"
            )
            VALUES($1, $2, $3, $4, $5, $6)
        "#,
        )
        .bind(&data.LP_Lender_id)
        .bind(&data.LP_Pool_id)
        .bind(data.LP_timestamp)
        .bind(&data.LP_Lender_stable)
        .bind(&data.LP_Lender_asset)
        .bind(&data.LP_Lender_receipts)
        .persistent(false)
        .execute(&self.pool)
        .await
    }

    pub async fn get_active_states(
        &self,
    ) -> Result<Vec<(String, String)>, Error> {
        sqlx::query_as(
            r#"
            SELECT 
                a."LP_address_id",
                a."LP_Pool_id"
            FROM "LP_Deposit" as a
            WHERE a."LP_timestamp" > COALESCE((
                SELECT "LP_timestamp"
                FROM "LP_Withdraw" as b
                WHERE  "LP_deposit_close" = true AND  b."LP_address_id" = a."LP_address_id" AND  b."LP_Pool_id" = a."LP_Pool_id"
                ORDER BY "LP_timestamp" DESC 
                LIMIT 1
            ), to_timestamp(0))
            GROUP BY "LP_address_id", "LP_Pool_id"
            "#,
        )
        .persistent(false)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn insert_many(
        &self,
        data: &Vec<LP_Lender_State>,
    ) -> Result<(), Error> {
        if data.is_empty() {
            return Ok(());
        }

        let mut query_builder: QueryBuilder<DataBase> = QueryBuilder::new(
            r#"
            INSERT INTO "LP_Lender_State" (
                "LP_Lender_id",
                "LP_Pool_id",
                "LP_timestamp",
                "LP_Lender_stable",
                "LP_Lender_asset",
                "LP_Lender_receipts"
            )"#,
        );

        query_builder.push_values(data, |mut b, data| {
            b.push_bind(&data.LP_Lender_id)
                .push_bind(&data.LP_Pool_id)
                .push_bind(data.LP_timestamp)
                .push_bind(&data.LP_Lender_stable)
                .push_bind(&data.LP_Lender_asset)
                .push_bind(&data.LP_Lender_receipts);
        });

        let query = query_builder.build().persistent(false);
        query.execute(&self.pool).await?;
        Ok(())
    }

    pub async fn count(
        &self,
        timestamp: DateTime<Utc>,
    ) -> Result<i64, crate::error::Error> {
        let (value,): (i64,) = sqlx::query_as(
            r#"
            SELECT 
                COUNT(*)
            FROM "LP_Lender_State" WHERE "LP_timestamp" = $1
            "#,
        )
        .bind(timestamp)
        .persistent(false)
        .fetch_one(&self.pool)
        .await?;
        Ok(value)
    }

    pub async fn get_all(&self) -> Result<Vec<LP_Lender_State>, Error> {
        sqlx::query_as(r#"SELECT * FROM "LP_Lender_State""#)
            .persistent(false)
            .fetch_all(&self.pool)
            .await
    }

    //TODO: delete
    pub async fn update(
        &self,
        data: LP_Lender_State,
    ) -> Result<QueryResult, Error> {
        sqlx::query(
            r#"
            UPDATE 
                "LP_Lender_State"
            SET
                "LP_Lender_stable" = $1,
                "LP_Lender_asset" = $2
            WHERE 
                "LP_Lender_id" = $3
                AND
                "LP_Pool_id" = $4
                 AND
                "LP_timestamp" = $5

        "#,
        )
        .bind(&data.LP_Lender_stable)
        .bind(&data.LP_Lender_asset)
        .bind(&data.LP_Lender_id)
        .bind(&data.LP_Pool_id)
        .bind(&data.LP_timestamp)
        .persistent(false)
        .execute(&self.pool)
        .await
    }
}
