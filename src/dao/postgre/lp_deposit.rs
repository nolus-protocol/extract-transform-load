use super::{DataBase, QueryResult};
use crate::model::{LP_Deposit, Table};
use chrono::{DateTime, Utc};
use sqlx::{error::Error, types::BigDecimal, QueryBuilder, Transaction};
use std::str::FromStr;

impl Table<LP_Deposit> {
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
                "LP_amnt_receipts"
            )
            VALUES($1, $2, $3, $4, $5, $6, $7)
        "#,
        )
        .bind(data.LP_deposit_height)
        .bind(&data.LP_address_id)
        .bind(data.LP_timestamp)
        .bind(&data.LP_Pool_id)
        .bind(&data.LP_amnt_stable)
        .bind(&data.LP_amnt_asset)
        .bind(&data.LP_amnt_receipts)
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
                "LP_amnt_receipts"
            )"#,
        );

        query_builder.push_values(data, |mut b, lp| {
            b.push_bind(lp.LP_deposit_height)
                .push_bind(&lp.LP_address_id)
                .push_bind(lp.LP_timestamp)
                .push_bind(&lp.LP_Pool_id)
                .push_bind(&lp.LP_amnt_stable)
                .push_bind(&lp.LP_amnt_asset)
                .push_bind(&lp.LP_amnt_receipts);
        });

        let query = query_builder.build();
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
        .fetch_one(&self.pool)
        .await?;
        let (amnt,) = value;
        let amnt = amnt.unwrap_or(BigDecimal::from_str("0")?);

        Ok(amnt)
    }

    pub async fn get_yield(&self) -> Result<BigDecimal, crate::error::Error> {
        let value: (Option<BigDecimal>,)   = sqlx::query_as(
            r#"
                SELECT ((("Price Per Receipt" - 1) / "Days Difference") * 365) * 100 AS "Yield" FROM (SELECT ROUND(CAST("LP_Pool_total_value_locked_stable" AS DECIMAL(38, 5)) / CAST("LP_Pool_total_issued_receipts" AS DECIMAL(38, 5)),5) AS "Price Per Receipt", EXTRACT(DAY FROM (NOW() - "LP_timestamp")) AS "Days Difference" FROM "LP_Deposit" LEFT JOIN "LP_Pool_State" ON "LP_Deposit"."LP_Pool_id"="LP_Pool_State"."LP_Pool_id" ORDER BY "LP_timestamp" ASC LIMIT 1) joined
            "#,
        )
        .fetch_one(&self.pool)
        .await?;

        let (amnt,) = value;
        let amnt = amnt.unwrap_or(BigDecimal::from_str("0")?);

        Ok(amnt)
    }
}
