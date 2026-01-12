use std::str::FromStr as _;

use chrono::{DateTime, Utc};
use sqlx::{types::BigDecimal, Error, FromRow, QueryBuilder, Transaction};

use crate::model::{LP_Deposit, Table};

use super::{DataBase, QueryResult};

#[derive(Debug, Clone, FromRow)]
pub struct HistoricalLender {
    pub transaction_type: String,
    pub timestamp: DateTime<Utc>,
    pub user: String,
    pub amount: BigDecimal,
    pub pool: String,
}

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
        .persistent(true)
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
        .persistent(true)
        .execute(&mut **transaction)
        .await
    }

    /// Inserts a record if it doesn't already exist, using ON CONFLICT DO NOTHING.
    /// More efficient than calling isExists() followed by insert().
    pub async fn insert_if_not_exists(
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
            ON CONFLICT ("LP_deposit_height", "LP_address_id", "LP_timestamp", "LP_Pool_id") DO NOTHING
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
        .persistent(true)
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

        let query = query_builder.build().persistent(true);
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
            FROM "LP_Deposit" WHERE "LP_timestamp" > $1 AND "LP_timestamp" <= $2
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

    pub async fn get_historical_lenders_with_window(
        &self,
        months: Option<i32>,
        from: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<Vec<HistoricalLender>, crate::error::Error> {
        // Build time conditions dynamically
        let mut conditions = Vec::new();

        if let Some(m) = months {
            conditions.push(format!(
                "timestamp > NOW() - INTERVAL '{} months'",
                m
            ));
        }

        if from.is_some() {
            conditions.push("timestamp > $1".to_string());
        }

        let time_condition = if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };

        let query = format!(
            r#"
            SELECT * FROM (
                SELECT 
                    'Deposit' AS transaction_type,
                    d."LP_timestamp" AS timestamp,
                    d."LP_address_id" AS user,
                    d."LP_amnt_stable" / COALESCE(pc.lpn_decimals, 1000000)::numeric AS amount,
                    COALESCE(pc.label, d."LP_Pool_id") AS pool
                FROM 
                    "LP_Deposit" d
                LEFT JOIN pool_config pc ON d."LP_Pool_id" = pc.pool_id

                UNION ALL

                SELECT 
                    'Withdraw' AS transaction_type,
                    w."LP_timestamp" AS timestamp,
                    w."LP_address_id" AS user,
                    w."LP_amnt_stable" / COALESCE(pc.lpn_decimals, 1000000)::numeric AS amount,
                    COALESCE(pc.label, w."LP_Pool_id") AS pool
                FROM 
                    "LP_Withdraw" w
                LEFT JOIN pool_config pc ON w."LP_Pool_id" = pc.pool_id
            ) combined
            {}
            ORDER BY timestamp DESC
            "#,
            time_condition
        );

        let mut query_builder = sqlx::query_as::<_, HistoricalLender>(&query);

        if let Some(from_ts) = from {
            query_builder = query_builder.bind(from_ts);
        }

        let data = query_builder.persistent(true).fetch_all(&self.pool).await?;

        Ok(data)
    }

    pub async fn get_all_historical_lenders(
        &self,
    ) -> Result<Vec<HistoricalLender>, crate::error::Error> {
        self.get_historical_lenders_with_window(None, None).await
    }
}
