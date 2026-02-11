use chrono::{DateTime, Utc};
use sqlx::{types::BigDecimal, Error, FromRow, QueryBuilder, Transaction};

use crate::model::{LS_Liquidation, Table};

use super::{DataBase, QueryResult};

#[derive(Debug, Clone, FromRow)]
pub struct LiquidationData {
    pub timestamp: DateTime<Utc>,
    pub ticker: String,
    pub contract_id: String,
    pub user: Option<String>,
    pub transaction_type: Option<String>,
    pub liquidation_amount: BigDecimal,
    pub closed_loan: bool,
    pub down_payment: BigDecimal,
    pub loan: BigDecimal,
    pub liquidation_price: Option<BigDecimal>,
}

#[derive(Debug, Clone, FromRow)]
pub struct HistoricallyLiquidated {
    pub contract_id: String,
    pub asset: String,
    pub loan: BigDecimal,
    pub total_liquidated: Option<BigDecimal>,
}

impl Table<LS_Liquidation> {
    pub async fn insert_if_not_exists(
        &self,
        data: &LS_Liquidation,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<QueryResult, Error> {
        sqlx::query(
            r#"
            INSERT INTO "LS_Liquidation" (
                "LS_liquidation_height",
                "LS_contract_id",
                "LS_amnt_symbol",
                "LS_timestamp",
                "LS_amnt_stable",
                "LS_transaction_type",
                "LS_prev_margin_stable",
                "LS_prev_interest_stable",
                "LS_current_margin_stable",
                "LS_current_interest_stable",
                "LS_principal_stable",
                "Tx_Hash",
                "LS_amnt",
                "LS_payment_symbol",
                "LS_payment_amnt",
                "LS_payment_amnt_stable",
                "LS_loan_close",
                "LS_liquidation_price"
            )
            VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
            ON CONFLICT ("LS_liquidation_height", "LS_contract_id") DO NOTHING
        "#,
        )
        .bind(data.LS_liquidation_height)
        .bind(&data.LS_contract_id)
        .bind(&data.LS_amnt_symbol)
        .bind(data.LS_timestamp)
        .bind(&data.LS_amnt_stable)
        .bind(&data.LS_transaction_type)
        .bind(&data.LS_prev_margin_stable)
        .bind(&data.LS_prev_interest_stable)
        .bind(&data.LS_current_margin_stable)
        .bind(&data.LS_current_interest_stable)
        .bind(&data.LS_principal_stable)
        .bind(&data.Tx_Hash)
        .bind(&data.LS_amnt)
        .bind(&data.LS_payment_symbol)
        .bind(&data.LS_payment_amnt)
        .bind(&data.LS_payment_amnt_stable)
        .bind(data.LS_loan_close)
        .bind(&data.LS_liquidation_price)
        .persistent(true)
        .execute(&mut **transaction)
        .await
    }

    pub async fn insert_many(
        &self,
        data: &Vec<LS_Liquidation>,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<(), Error> {
        if data.is_empty() {
            return Ok(());
        }

        let mut query_builder: QueryBuilder<DataBase> = QueryBuilder::new(
            r#"
            INSERT INTO "LS_Liquidation" (
                "LS_liquidation_height",
                "LS_contract_id",
                "LS_amnt_symbol",
                "LS_timestamp",
                "LS_amnt_stable",
                "LS_transaction_type",
                "LS_prev_margin_stable",
                "LS_prev_interest_stable",
                "LS_current_margin_stable",
                "LS_current_interest_stable",
                "LS_principal_stable",
                "Tx_Hash",
                "LS_amnt",
                "LS_payment_symbol",
                "LS_payment_amnt",
                "LS_payment_amnt_stable",
                "LS_loan_close",
                "LS_liquidation_price"
            )"#,
        );

        query_builder.push_values(data, |mut b, ls| {
            b.push_bind(ls.LS_liquidation_height)
                .push_bind(&ls.LS_contract_id)
                .push_bind(&ls.LS_amnt_symbol)
                .push_bind(ls.LS_timestamp)
                .push_bind(&ls.LS_amnt_stable)
                .push_bind(&ls.LS_transaction_type)
                .push_bind(&ls.LS_prev_margin_stable)
                .push_bind(&ls.LS_prev_interest_stable)
                .push_bind(&ls.LS_current_margin_stable)
                .push_bind(&ls.LS_current_interest_stable)
                .push_bind(&ls.LS_principal_stable)
                .push_bind(&ls.Tx_Hash)
                .push_bind(&ls.LS_amnt)
                .push_bind(&ls.LS_payment_symbol)
                .push_bind(&ls.LS_payment_amnt)
                .push_bind(&ls.LS_payment_amnt_stable)
                .push_bind(ls.LS_loan_close)
                .push_bind(&ls.LS_liquidation_price);
        });

        let query = query_builder.build().persistent(true);
        query.execute(&mut **transaction).await?;
        Ok(())
    }

    pub async fn get_by_contract(
        &self,
        contract: String,
    ) -> Result<Vec<LS_Liquidation>, Error> {
        let data = sqlx::query_as(
            r#"
            SELECT * FROM "LS_Liquidation" WHERE "LS_contract_id" = $1
            "#,
        )
        .bind(&contract)
        .persistent(true)
        .fetch_all(&self.pool)
        .await?;
        Ok(data)
    }

    pub async fn get_liquidations_with_window(
        &self,
        months: Option<i32>,
        from: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<Vec<LiquidationData>, crate::error::Error> {
        // Build time conditions dynamically
        let mut conditions = Vec::new();

        if let Some(m) = months {
            conditions.push(format!(
                "liq.\"LS_timestamp\" >= NOW() - INTERVAL '{} months'",
                m
            ));
        }

        if from.is_some() {
            conditions.push("liq.\"LS_timestamp\" > $1".to_string());
        }

        let time_condition = if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };

        // Simplified query using stored LS_liquidation_price instead of expensive CTE
        let query = format!(
            r#"
            SELECT
                liq."LS_timestamp" AS timestamp,
                liq."LS_amnt_symbol" AS ticker,
                liq."LS_contract_id" AS contract_id,
                o."LS_address_id" AS user,
                liq."LS_transaction_type" AS transaction_type,
                liq."LS_payment_amnt_stable" / pc.stable_currency_decimals::numeric AS liquidation_amount,
                liq."LS_loan_close" AS closed_loan,
                o."LS_cltr_amnt_stable" / POWER(10, cr_cltr.decimal_digits)::NUMERIC AS down_payment,
                o."LS_loan_amnt_asset" / pc.lpn_decimals::numeric AS loan,
                liq."LS_liquidation_price" AS liquidation_price
            FROM
                "LS_Liquidation" liq
                LEFT JOIN "LS_Opening" o ON o."LS_contract_id" = liq."LS_contract_id"
                INNER JOIN currency_registry cr_cltr ON cr_cltr.ticker = o."LS_cltr_symbol"
                INNER JOIN pool_config pc ON pc.pool_id = o."LS_loan_pool_id"
            {}
            ORDER BY
                liq."LS_timestamp" DESC
            "#,
            time_condition
        );

        let mut query_builder = sqlx::query_as::<_, LiquidationData>(&query);

        if let Some(from_ts) = from {
            query_builder = query_builder.bind(from_ts);
        }

        let data = query_builder.persistent(true).fetch_all(&self.pool).await?;

        Ok(data)
    }

    pub async fn get_all_liquidations(
        &self,
    ) -> Result<Vec<LiquidationData>, crate::error::Error> {
        self.get_liquidations_with_window(None, None).await
    }

    pub async fn get_historically_liquidated(
        &self,
    ) -> Result<Vec<HistoricallyLiquidated>, crate::error::Error> {
        let data = sqlx::query_as(
            r#"
            WITH LiquidationAmounts AS (
                SELECT
                    lso."LS_contract_id",
                    CASE
                        WHEN pc.position_type = 'Short' THEN CONCAT(pc.label, ' (Short)')
                        ELSE lso."LS_asset_symbol"
                    END AS "Asset",
                    lso."LS_loan_amnt_asset" / pc.lpn_decimals::numeric AS "Loan",
                    lsl."LS_amnt_stable" / POWER(10, cr_asset.decimal_digits)::NUMERIC AS "Liquidation Amount"
                FROM
                    "LS_Opening" lso
                    LEFT JOIN "LS_Liquidation" lsl ON lso."LS_contract_id" = lsl."LS_contract_id"
                    INNER JOIN pool_config pc ON lso."LS_loan_pool_id" = pc.pool_id
                    INNER JOIN currency_registry cr_asset ON cr_asset.ticker = lso."LS_asset_symbol"
            )
            SELECT
                "LS_contract_id" AS contract_id,
                "Asset" AS asset,
                "Loan" AS loan,
                SUM("Liquidation Amount") AS total_liquidated
            FROM
                LiquidationAmounts
            GROUP BY
                "LS_contract_id",
                "Asset",
                "Loan"
            "#,
        )
        .persistent(true)
        .fetch_all(&self.pool)
        .await?;

        Ok(data)
    }

    /// Get historically liquidated positions with optional time window filter
    pub async fn get_historically_liquidated_with_window(
        &self,
        months: Option<i32>,
        from: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<Vec<HistoricallyLiquidated>, crate::error::Error> {
        // Build time conditions dynamically
        let mut conditions = Vec::new();

        if let Some(m) = months {
            conditions.push(format!(
                "lso.\"LS_timestamp\" >= NOW() - INTERVAL '{} months'",
                m
            ));
        }

        if from.is_some() {
            conditions.push("lso.\"LS_timestamp\" > $1".to_string());
        }

        let time_condition = if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };

        let query = format!(
            r#"
            WITH LiquidationAmounts AS (
                SELECT
                    lso."LS_contract_id",
                    CASE
                        WHEN pc.position_type = 'Short' THEN CONCAT(pc.label, ' (Short)')
                        ELSE lso."LS_asset_symbol"
                    END AS "Asset",
                    lso."LS_loan_amnt_asset" / pc.lpn_decimals::numeric AS "Loan",
                    lsl."LS_amnt_stable" / POWER(10, cr_asset.decimal_digits)::NUMERIC AS "Liquidation Amount"
                FROM
                    "LS_Opening" lso
                    LEFT JOIN "LS_Liquidation" lsl ON lso."LS_contract_id" = lsl."LS_contract_id"
                    INNER JOIN pool_config pc ON lso."LS_loan_pool_id" = pc.pool_id
                    INNER JOIN currency_registry cr_asset ON cr_asset.ticker = lso."LS_asset_symbol"
                {}
            )
            SELECT
                "LS_contract_id" AS contract_id,
                "Asset" AS asset,
                "Loan" AS loan,
                SUM("Liquidation Amount") AS total_liquidated
            FROM
                LiquidationAmounts
            GROUP BY
                "LS_contract_id",
                "Asset",
                "Loan"
            "#,
            time_condition
        );

        let mut query_builder =
            sqlx::query_as::<_, HistoricallyLiquidated>(&query);

        if let Some(from_ts) = from {
            query_builder = query_builder.bind(from_ts);
        }

        let data = query_builder.persistent(true).fetch_all(&self.pool).await?;

        Ok(data)
    }
}
