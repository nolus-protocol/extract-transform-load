use std::str::FromStr as _;

use bigdecimal::BigDecimal;
use sqlx::{Error, Transaction};

use crate::model::{LS_Loan_Closing, Pnl_Result, Table};

use super::{DataBase, QueryResult};

impl Table<LS_Loan_Closing> {
    /// Inserts a record if it doesn't already exist, using ON CONFLICT DO NOTHING.
    pub async fn insert_if_not_exists(
        &self,
        data: LS_Loan_Closing,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<QueryResult, Error> {
        sqlx::query(
            r#"
            INSERT INTO "LS_Loan_Closing" (
                "LS_contract_id",
                "LS_amnt",
                "LS_amnt_stable",
                "LS_pnl",
                "LS_timestamp",
                "Type",
                "Block",
                "Active"
            )
            VALUES($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT ("LS_contract_id") DO NOTHING
        "#,
        )
        .bind(&data.LS_contract_id)
        .bind(&data.LS_amnt)
        .bind(&data.LS_amnt_stable)
        .bind(&data.LS_pnl)
        .bind(data.LS_timestamp)
        .bind(&data.Type)
        .bind(data.Block)
        .bind(data.Active)
        .persistent(true)
        .execute(&mut **transaction)
        .await
    }

    pub async fn get_lease_amount(
        &self,
        contract_id: String,
    ) -> Result<BigDecimal, crate::error::Error> {
        let value: (Option<BigDecimal>,) = sqlx::query_as(
            r#"
                SELECT SUM("Amount") as "Total" FROM (
                    SELECT
                    SUM("LS_loan_amnt") as "Amount"
                    FROM "LS_Opening"
                    WHERE "LS_contract_id" = $1
                UNION ALL
                    SELECT
                    -SUM("LS_amnt") as "Amount"
                    FROM "LS_Close_Position"
                    WHERE "LS_contract_id" = $1
                UNION ALL
                    SELECT
                    -SUM("LS_amnt") as "Amount"
                    FROM "LS_Liquidation"
                    WHERE "LS_contract_id" = $1
                ) AS combined_data
            "#,
        )
        .bind(contract_id)
        .persistent(true)
        .fetch_one(&self.pool)
        .await?;
        let (amnt,) = value;
        let amnt = amnt.unwrap_or(BigDecimal::from_str("0")?);

        Ok(amnt)
    }

    pub async fn get_realized_pnl_stats(
        &self,
    ) -> Result<BigDecimal, crate::error::Error> {
        let value: (Option<BigDecimal>,) = sqlx::query_as(
            r#"
                SELECT
                SUM(c."LS_pnl" / POWER(10, cr_asset.decimal_digits)::NUMERIC) AS "Total Adjusted Stable Amount"
                FROM
                "LS_Loan_Closing" c
                LEFT JOIN
                "LS_Opening" o
                ON
                c."LS_contract_id" = o."LS_contract_id"
                INNER JOIN currency_registry cr_asset ON cr_asset.ticker = o."LS_asset_symbol"
                WHERE
                c."LS_timestamp" >= '2025-01-01'
            "#,
        )
        .persistent(true)
        .fetch_one(&self.pool)
        .await?;
        let (amnt,) = value;
        let amnt = amnt.unwrap_or(BigDecimal::from_str("0")?);

        Ok(amnt)
    }

    pub async fn update(
        &self,
        data: LS_Loan_Closing,
    ) -> Result<QueryResult, Error> {
        sqlx::query(
            r#"
            UPDATE
                "LS_Loan_Closing"
            SET
                "LS_amnt" = $1,
                "LS_amnt_stable" = $2,
                "LS_pnl" = $3,
                "Active" = $4
            WHERE
                "LS_contract_id" = $5

        "#,
        )
        .bind(&data.LS_amnt)
        .bind(&data.LS_amnt_stable)
        .bind(&data.LS_pnl)
        .bind(data.Active)
        .bind(&data.LS_contract_id)
        .persistent(true)
        .execute(&self.pool)
        .await
    }

    pub async fn get_leases_to_proceed(
        &self,
    ) -> Result<Vec<LS_Loan_Closing>, Error> {
        let data = sqlx::query_as(
            r#"
            SELECT * FROM "LS_Loan_Closing" WHERE "Active" = false;
            "#,
        )
        .persistent(true)
        .fetch_all(&self.pool)
        .await?;
        Ok(data)
    }

    pub async fn get_leases(
        &self,
        address: String,
        skip: i64,
        limit: i64,
    ) -> Result<Vec<Pnl_Result>, Error> {
        let data = sqlx::query_as(
            r#"
                WITH
                openings AS (
                SELECT
                    o."LS_contract_id",
                    o."LS_asset_symbol",
                    o."LS_cltr_symbol",
                    o."LS_cltr_amnt_stable",
                    o."LS_loan_pool_id"
                FROM "LS_Opening" o
                WHERE o."LS_address_id" = $1
                ),

                repayments AS (
                SELECT
                    r."LS_contract_id",
                    SUM(r."LS_payment_amnt_stable" / POWER(10, cr_pay.decimal_digits)::NUMERIC) AS total_repaid_usdc
                FROM "LS_Repayment" r
                INNER JOIN openings o ON o."LS_contract_id" = r."LS_contract_id"
                INNER JOIN currency_registry cr_pay ON cr_pay.ticker = r."LS_payment_symbol"
                GROUP BY r."LS_contract_id"
                ),

                collects AS (
                SELECT
                    lc."LS_contract_id",
                    SUM(lc."LS_amount_stable" / POWER(10, cr_col.decimal_digits)::NUMERIC)::numeric(38,8) AS total_collected_usdc
                FROM "LS_Loan_Collect" lc
                INNER JOIN openings o ON o."LS_contract_id" = lc."LS_contract_id"
                INNER JOIN currency_registry cr_col ON cr_col.ticker = lc."LS_symbol"
                GROUP BY lc."LS_contract_id"
                )

                SELECT
                o."LS_contract_id"                                                        AS "Position ID",
                o."LS_asset_symbol",
                o."LS_loan_pool_id",
                ct."Type",
                ct."LS_timestamp",
                to_char(ct."LS_timestamp", 'YYYY-MM-DD HH24:MI UTC')                      AS "Close Date UTC",
                (
                    (o."LS_cltr_amnt_stable" / POWER(10, cr_cltr.decimal_digits)::NUMERIC)::numeric(38,8)
                    + COALESCE(r.total_repaid_usdc, 0::numeric(38,8))
                )::double precision                                                        AS "Sent (USDC, Opening)",
                COALESCE(c.total_collected_usdc, 0::numeric(38,8))::double precision        AS "Received (USDC, Closing)",
                (
                    COALESCE(c.total_collected_usdc, 0::numeric(38,8))
                    - (
                        (o."LS_cltr_amnt_stable" / POWER(10, cr_cltr.decimal_digits)::NUMERIC)::numeric(38,8)
                        + COALESCE(r.total_repaid_usdc, 0::numeric(38,8))
                    )
                )::double precision                                                        AS "Realized PnL (USDC)"
                FROM openings o
                INNER JOIN currency_registry cr_cltr ON cr_cltr.ticker = o."LS_cltr_symbol"
                LEFT JOIN repayments r ON r."LS_contract_id" = o."LS_contract_id"
                LEFT JOIN collects c ON c."LS_contract_id" = o."LS_contract_id"
                INNER JOIN "LS_Loan_Closing" ct ON ct."LS_contract_id" = o."LS_contract_id"
                ORDER BY ct."LS_timestamp" DESC
                OFFSET
                    $2
                LIMIT
                    $3
            "#,
        )
        .bind(address)
        .bind(skip)
        .bind(limit)
        .persistent(true)
        .fetch_all(&self.pool)
        .await?;
        Ok(data)
    }

    pub async fn get_realized_pnl(
        &self,
        address: String,
    ) -> Result<f64, crate::error::Error> {
        let value: (Option<f64>,) = sqlx::query_as(
            r#"
                WITH
                openings AS (
                SELECT
                    o."LS_contract_id",
                    o."LS_cltr_symbol",
                    o."LS_cltr_amnt_stable"
                FROM "LS_Opening" o
                WHERE o."LS_address_id" = $1
                ),

                repayments AS (
                SELECT
                    r."LS_contract_id",
                    SUM(r."LS_payment_amnt_stable" / POWER(10, cr_pay.decimal_digits)::NUMERIC) AS total_repaid_usdc
                FROM "LS_Repayment" r
                INNER JOIN openings o ON o."LS_contract_id" = r."LS_contract_id"
                INNER JOIN currency_registry cr_pay ON cr_pay.ticker = r."LS_payment_symbol"
                GROUP BY r."LS_contract_id"
                ),

                collects AS (
                SELECT
                    lc."LS_contract_id",
                    SUM(lc."LS_amount_stable" / POWER(10, cr_col.decimal_digits)::NUMERIC)::numeric(38,8) AS total_collected_usdc
                FROM "LS_Loan_Collect" lc
                INNER JOIN openings o ON o."LS_contract_id" = lc."LS_contract_id"
                INNER JOIN currency_registry cr_col ON cr_col.ticker = lc."LS_symbol"
                GROUP BY lc."LS_contract_id"
                ),

                position_flows AS (
                SELECT
                    o."LS_contract_id"                            AS position_id,
                    (
                    (o."LS_cltr_amnt_stable" / POWER(10, cr_cltr.decimal_digits)::NUMERIC)::numeric(38,8)
                    + COALESCE(r.total_repaid_usdc, 0::numeric(38,8))
                    )                                               AS sent_open_usdc,
                    COALESCE(c.total_collected_usdc, 0::numeric(38,8)) AS received_close_usdc
                FROM openings o
                INNER JOIN currency_registry cr_cltr ON cr_cltr.ticker = o."LS_cltr_symbol"
                LEFT JOIN repayments r ON r."LS_contract_id" = o."LS_contract_id"
                LEFT JOIN collects c ON c."LS_contract_id" = o."LS_contract_id"
                INNER JOIN "LS_Loan_Closing" ct ON ct."LS_contract_id" = o."LS_contract_id"
                )

                SELECT
                (SUM(received_close_usdc) - SUM(sent_open_usdc))::double precision
                    AS "Total Realized PnL (USDC)"
                FROM position_flows
            "#,
        )
        .bind(address)
        .persistent(true)
        .fetch_one(&self.pool)
        .await?;

        let (amnt,) = value;
        let amnt = amnt.unwrap_or(0.0);

        Ok(amnt)
    }

    pub async fn get_all(&self) -> Result<Vec<LS_Loan_Closing>, Error> {
        sqlx::query_as(
            r#"SELECT * FROM "LS_Loan_Closing" WHERE "Block" <= 3785599"#, //<= 3785599
        )
        .persistent(true)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn get(
        &self,
        contract: String,
    ) -> Result<LS_Loan_Closing, Error> {
        sqlx::query_as(
            r#"SELECT * FROM "LS_Loan_Closing" WHERE "LS_contract_id" = $1"#,
        )
        .bind(contract)
        .persistent(true)
        .fetch_one(&self.pool)
        .await
    }
}
