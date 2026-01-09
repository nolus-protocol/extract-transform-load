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
    pub async fn isExists(
        &self,
        ls_liquidatiion: &LS_Liquidation,
    ) -> Result<bool, crate::error::Error> {
        let (value,): (i64,) = sqlx::query_as(
            r#"
            SELECT
                COUNT(*)
            FROM "LS_Liquidation"
            WHERE
                "LS_liquidation_height" = $1 AND
                "LS_contract_id" = $2
            "#,
        )
        .bind(ls_liquidatiion.LS_liquidation_height)
        .bind(&ls_liquidatiion.LS_contract_id)
        .persistent(false)
        .fetch_one(&self.pool)
        .await?;

        if value > 0 {
            return Ok(true);
        }

        Ok(false)
    }

    pub async fn insert(
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
                "LS_loan_close"
            )
            VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
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
        .persistent(false)
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
                "LS_loan_close"
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
                .push_bind(ls.LS_loan_close);
        });

        let query = query_builder.build().persistent(false);
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
        .persistent(false)
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
                "\"LS_Liquidation\".\"LS_timestamp\" >= NOW() - INTERVAL '{} months'",
                m
            ));
        }

        if from.is_some() {
            conditions.push("\"LS_Liquidation\".\"LS_timestamp\" > $1".to_string());
        }

        let time_condition = if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };

        let query = format!(
            r#"
            WITH Liquidation_With_DP AS (
                SELECT
                    "LS_Liquidation"."LS_timestamp" AS "Timestamp",
                    "LS_amnt_symbol" AS "Ticker",
                    "LS_Liquidation"."LS_contract_id" AS "Contract ID",
                    "LS_Opening"."LS_address_id" AS "User",
                    "LS_transaction_type" AS "Type",
                    "LS_payment_amnt_stable" / 1000000 AS "Liquidation Amount",
                    "LS_loan_close" AS "Closed Loan",
                    CASE
                        WHEN "LS_cltr_symbol" IN ('WBTC', 'CRO') THEN "LS_cltr_amnt_stable" / 100000000
                        WHEN "LS_cltr_symbol" IN ('PICA') THEN "LS_cltr_amnt_stable" / 1000000000000
                        WHEN "LS_cltr_symbol" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS', 'ALL_ETH') THEN "LS_cltr_amnt_stable" / 1000000000000000000
                        ELSE "LS_cltr_amnt_stable" / 1000000
                    END AS "Down Payment",
                    "LS_loan_amnt_asset" / 1000000 AS "Loan"
                FROM
                    "LS_Liquidation"
                    LEFT JOIN "LS_Opening" ON "LS_Opening"."LS_contract_id" = "LS_Liquidation"."LS_contract_id"
                {}
            ),
            Current_Market_Prices AS (
                SELECT
                    DATE_TRUNC('minute', "MP_asset_timestamp") AS "Truncated Timestamp",
                    "MP_asset_symbol",
                    AVG("MP_price_in_stable") AS "Price in Stable"
                FROM
                    "MP_Asset"
                WHERE "MP_asset_timestamp" > NOW() - INTERVAL '30 days' AND "Protocol" = 'OSMOSIS-OSMOSIS-USDC_NOBLE'
                GROUP BY
                    DATE_TRUNC('minute', "MP_asset_timestamp"), "MP_asset_symbol"
            )
            SELECT
                ldp."Timestamp" AS timestamp,
                ldp."Ticker" AS ticker,
                ldp."Contract ID" AS contract_id,
                ldp."User" AS user,
                ldp."Type" AS transaction_type,
                ldp."Liquidation Amount" AS liquidation_amount,
                ldp."Closed Loan" AS closed_loan,
                ldp."Down Payment" AS down_payment,
                ldp."Loan" AS loan,
                cmp."Price in Stable" AS liquidation_price
            FROM
                Liquidation_With_DP ldp
                LEFT JOIN Current_Market_Prices cmp 
                    ON DATE_TRUNC('minute', ldp."Timestamp") = cmp."Truncated Timestamp"
                    AND ldp."Ticker" = cmp."MP_asset_symbol"
            ORDER BY
                ldp."Timestamp" DESC
            "#,
            time_condition
        );

        let mut query_builder = sqlx::query_as::<_, LiquidationData>(&query);

        if let Some(from_ts) = from {
            query_builder = query_builder.bind(from_ts);
        }

        let data = query_builder.persistent(false).fetch_all(&self.pool).await?;

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
                        WHEN lso."LS_loan_pool_id" = 'nolus1jufcaqm6657xmfltdezzz85quz92rmtd88jk5x0hq9zqseem32ysjdm990' THEN 'ST_ATOM (Short)'
                        WHEN lso."LS_loan_pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN 'ALL_BTC (Short)'
                        WHEN lso."LS_loan_pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN 'ALL_SOL (Short)'
                        WHEN lso."LS_loan_pool_id" = 'nolus1lxr7f5xe02jq6cce4puk6540mtu9sg36at2dms5sk69wdtzdrg9qq0t67z' THEN 'AKT (Short)'
                        ELSE lso."LS_asset_symbol"
                    END AS "Asset",
                    lso."LS_loan_amnt_asset" / 1000000 AS "Loan",
                    CASE
                        WHEN lso."LS_asset_symbol" IN ('ALL_BTC', 'WBTC', 'CRO') THEN lsl."LS_amnt_stable" / 100000000
                        WHEN lso."LS_asset_symbol" IN ('ALL_SOL') THEN lsl."LS_amnt_stable" / 1000000000
                        WHEN lso."LS_asset_symbol" IN ('PICA') THEN lsl."LS_amnt_stable" / 1000000000000
                        WHEN lso."LS_asset_symbol" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS', 'ALL_ETH') THEN lsl."LS_amnt_stable" / 1000000000000000000
                        ELSE lsl."LS_amnt_stable" / 1000000
                    END AS "Liquidation Amount"
                FROM
                    "LS_Opening" lso
                    LEFT JOIN "LS_Liquidation" lsl ON lso."LS_contract_id" = lsl."LS_contract_id"
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
        .persistent(false)
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
                        WHEN lso."LS_loan_pool_id" = 'nolus1jufcaqm6657xmfltdezzz85quz92rmtd88jk5x0hq9zqseem32ysjdm990' THEN 'ST_ATOM (Short)'
                        WHEN lso."LS_loan_pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN 'ALL_BTC (Short)'
                        WHEN lso."LS_loan_pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN 'ALL_SOL (Short)'
                        WHEN lso."LS_loan_pool_id" = 'nolus1lxr7f5xe02jq6cce4puk6540mtu9sg36at2dms5sk69wdtzdrg9qq0t67z' THEN 'AKT (Short)'
                        ELSE lso."LS_asset_symbol"
                    END AS "Asset",
                    lso."LS_loan_amnt_asset" / 1000000 AS "Loan",
                    CASE
                        WHEN lso."LS_asset_symbol" IN ('ALL_BTC', 'WBTC', 'CRO') THEN lsl."LS_amnt_stable" / 100000000
                        WHEN lso."LS_asset_symbol" IN ('ALL_SOL') THEN lsl."LS_amnt_stable" / 1000000000
                        WHEN lso."LS_asset_symbol" IN ('PICA') THEN lsl."LS_amnt_stable" / 1000000000000
                        WHEN lso."LS_asset_symbol" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS', 'ALL_ETH') THEN lsl."LS_amnt_stable" / 1000000000000000000
                        ELSE lsl."LS_amnt_stable" / 1000000
                    END AS "Liquidation Amount"
                FROM
                    "LS_Opening" lso
                    LEFT JOIN "LS_Liquidation" lsl ON lso."LS_contract_id" = lsl."LS_contract_id"
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

        let mut query_builder = sqlx::query_as::<_, HistoricallyLiquidated>(&query);

        if let Some(from_ts) = from {
            query_builder = query_builder.bind(from_ts);
        }

        let data = query_builder.persistent(false).fetch_all(&self.pool).await?;

        Ok(data)
    }
}
