use bigdecimal::{BigDecimal, Zero as _};
use sqlx::{Error, Transaction};

use crate::model::{LS_Loan_Closing, Pnl_Result, Realized_Pnl_Result, Table};

use super::DataBase;

impl Table<LS_Loan_Closing> {
    // FIXME Pass data by reference, as separate arguments or as a dedicated
    //  structure. Avoid the need for owned data.
    pub async fn isExists(&self, contract: String) -> Result<bool, Error> {
        const SQL: &str = r#"
        SELECT EXISTS(
            SELECT
            FROM "LS_Loan_Closing"
            WHERE "LS_contract_id" = $1
        )
        "#;

        sqlx::query_as(SQL)
            .bind(contract)
            .fetch_one(&self.pool)
            .await
            .map(|(result,)| result)
    }

    // FIXME Pass data by reference, as separate arguments or as a dedicated
    //  structure. Avoid the need for owned data.
    pub async fn insert(
        &self,
        data: LS_Loan_Closing,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<(), Error> {
        const SQL: &str = r#"
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
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        "#;

        sqlx::query(SQL)
            .bind(&data.LS_contract_id)
            .bind(&data.LS_amnt)
            .bind(&data.LS_amnt_stable)
            .bind(&data.LS_pnl)
            .bind(data.LS_timestamp)
            .bind(&data.Type)
            .bind(data.Block)
            .bind(data.Active)
            .execute(transaction.as_mut())
            .await
            .map(drop)
    }

    // FIXME Pass argument by reference.
    pub async fn get_lease_amount(
        &self,
        contract_id: String,
    ) -> Result<BigDecimal, Error> {
        const SQL: &str = r#"
        SELECT (
            (
                SELECT
                    SUM("LS_loan_amnt")
                FROM "LS_Opening"
                WHERE "LS_contract_id" = $1
            ) - (
                SELECT
                    SUM("LS_amnt")
                FROM "LS_Close_Position"
                WHERE "LS_contract_id" = $1
            ) - (
                SELECT
                    SUM("LS_amnt")
                FROM "LS_Liquidation"
                WHERE "LS_contract_id" = $1
            )
        )
        "#;

        sqlx::query_as(SQL)
            .bind(contract_id)
            .fetch_optional(&self.pool)
            .await
            .map(|maybe_row| {
                maybe_row.map_or_else(BigDecimal::zero, |(result,)| result)
            })
    }

    pub async fn get_realized_pnl_stats(&self) -> Result<BigDecimal, Error> {
        // FIXME Find a way to describe currencies' decimal places dynamically.
        const SQL: &str = r#"
        SELECT
            (
                "c"."LS_pnl" / SUM(
                    CASE
                        WHEN "o"."LS_asset_symbol" IN ('ALL_BTC', 'WBTC', 'CRO') THEN
                            100000000
                        WHEN "o"."LS_asset_symbol" IN ('ALL_SOL') THEN
                            1000000000
                        WHEN "o"."LS_asset_symbol" IN ('PICA') THEN
                            1000000000000
                        WHEN "o"."LS_asset_symbol" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS') THEN
                            1000000000000000000
                        ELSE
                            1000000
                    END
                )
            ) AS "Total Adjusted Stable Amount"
        FROM "LS_Loan_Closing" AS "c"
        LEFT JOIN "LS_Opening" AS "o" ON "c"."LS_contract_id" = "o"."LS_contract_id"
        WHERE "c"."LS_timestamp" >= '2025-01-01'
        "#;

        sqlx::query_as(SQL)
            .fetch_optional(&self.pool)
            .await
            .map(|maybe_row| {
                maybe_row.map_or_else(BigDecimal::zero, |(result,)| result)
            })
    }

    // FIXME Pass data by reference, as separate arguments or as a dedicated
    //  structure. Avoid the need for owned data.
    pub async fn update(&self, data: LS_Loan_Closing) -> Result<(), Error> {
        const SQL: &str = r#"
        UPDATE "LS_Loan_Closing"
        SET
            "LS_amnt" = $1,
            "LS_amnt_stable" = $2,
            "LS_pnl" = $3,
            "Active" = $4
        WHERE "LS_contract_id" = $5
        "#;

        sqlx::query(SQL)
            .bind(&data.LS_amnt)
            .bind(&data.LS_amnt_stable)
            .bind(&data.LS_pnl)
            .bind(data.Active)
            .bind(&data.LS_contract_id)
            .execute(&self.pool)
            .await
            .map(drop)
    }

    // FIXME Driver might limit number of returned rows.
    pub async fn get_leases_to_proceed(
        &self,
    ) -> Result<Vec<LS_Loan_Closing>, Error> {
        const SQL: &str = r#"
        SELECT *
        FROM "LS_Loan_Closing"
        WHERE "Active" = false
        "#;

        sqlx::query_as(SQL).fetch_all(&self.pool).await
    }

    // FIXME Pass argument by reference.
    // FIXME Use `UInt63` instead.
    // FIXME Avoid using `OFFSET` in SQL query. It requires evaluating rows
    //  eagerly before they can be filtered out.
    // FIXME Driver might limit number of returned rows.
    pub async fn get_leases(
        &self,
        address: String,
        skip: i64,
        limit: i64,
    ) -> Result<Vec<Pnl_Result>, Error> {
        const SQL: &str = r#"
        SELECT
            "loan_closing"."LS_contract_id",
            "loan_closing"."LS_amnt",
            "loan_closing"."LS_amnt_stable",
            "loan_closing"."LS_pnl",
            "loan_closing"."LS_timestamp",
            "loan_closing"."Type",
            "loan_closing"."Block",
            "opening"."LS_asset_symbol",
            "opening"."LS_loan_pool_id"
        FROM "LS_Loan_Closing" AS "loan_closing"
        JOIN "LS_Opening" AS "opening" ON "loan_closing"."LS_contract_id" = "opening"."LS_contract_id"
        WHERE
            "loan_closing"."Active" = true AND
            "opening"."LS_address_id" = $1
        ORDER BY "closing"."LS_timestamp" DESC
        OFFSET $2
        LIMIT $3
        "#;

        sqlx::query_as(SQL)
            .bind(address)
            .bind(skip)
            .bind(limit)
            .fetch_all(&self.pool)
            .await
    }

    // FIXME Pass argument by reference.
    // FIXME Driver might limit number of returned rows.
    pub async fn get_realized_pnl(
        &self,
        address: String,
    ) -> Result<Vec<Realized_Pnl_Result>, Error> {
        const SQL: &str = r#"
        SELECT
            "loan_closing"."LS_pnl",
            "opening"."LS_loan_pool_id",
            "opening"."LS_asset_symbol"
        FROM "LS_Loan_Closing" AS "loan_closing"
        JOIN "LS_Opening" AS "opening" ON "loan_closing"."LS_contract_id" = "opening"."LS_contract_id"
        WHERE "opening"."LS_address_id" = $1
        "#;

        sqlx::query_as(SQL)
            .bind(address)
            .fetch_all(&self.pool)
            .await
    }
}
