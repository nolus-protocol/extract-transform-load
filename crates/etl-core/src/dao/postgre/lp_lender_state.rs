use chrono::{DateTime, Utc};
use sqlx::{types::BigDecimal, Error, FromRow, QueryBuilder};

use crate::model::{LP_Lender_State, Table};

use super::{DataBase, QueryResult};

#[derive(Debug, Clone, FromRow)]
pub struct CurrentLender {
    pub joined: Option<DateTime<Utc>>,
    pub pool: Option<String>,
    pub lender: String,
    pub lent_stables: BigDecimal,
}

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
        .persistent(true)
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
        .persistent(true)
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

        let query = query_builder.build().persistent(true);
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
        .persistent(true)
        .fetch_one(&self.pool)
        .await?;
        Ok(value)
    }

    pub async fn get_all(&self) -> Result<Vec<LP_Lender_State>, Error> {
        sqlx::query_as(r#"SELECT * FROM "LP_Lender_State""#)
            .persistent(true)
            .fetch_all(&self.pool)
            .await
    }

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
        .bind(data.LP_timestamp)
        .persistent(true)
        .execute(&self.pool)
        .await
    }

    pub async fn get_current_lenders(
        &self,
    ) -> Result<Vec<CurrentLender>, crate::error::Error> {
        let data = sqlx::query_as(
            r#"
            WITH LatestAggregation AS (
                SELECT MAX("LP_timestamp") AS max_ts FROM "LP_Lender_State"
            )
            SELECT
                unique_lpd."Joined" AS joined,
                COALESCE(pc.label, lps."LP_Pool_id") AS pool,
                lps."LP_Lender_id" AS lender,
                lps."LP_Lender_stable" / pc.lpn_decimals::numeric AS lent_stables
            FROM
                "LP_Lender_State" lps
            INNER JOIN pool_config pc ON pc.pool_id = lps."LP_Pool_id"
            CROSS JOIN
                LatestAggregation la
            LEFT JOIN (
                SELECT DISTINCT ON (lpd_inner."LP_address_id")
                    lpd_inner."LP_address_id",
                    lpd_inner."LP_timestamp" AS "Joined"
                FROM "LP_Deposit" lpd_inner
                ORDER BY lpd_inner."LP_address_id", lpd_inner."LP_timestamp" DESC
            ) AS unique_lpd ON lps."LP_Lender_id" = unique_lpd."LP_address_id"
            WHERE
                lps."LP_timestamp" = la.max_ts
            "#,
        )
        .persistent(true)
        .fetch_all(&self.pool)
        .await?;

        Ok(data)
    }
}
