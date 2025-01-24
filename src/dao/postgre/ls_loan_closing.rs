use bigdecimal::{BigDecimal, Zero as _};
use sqlx::{Error, Transaction};

use crate::model::{LS_Loan_Closing, Pnl_Result, Realized_Pnl_Result, Table};

use super::DataBase;

impl Table<LS_Loan_Closing> {
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
            .execute(&mut **transaction)
            .await
            .map(drop)
    }

    pub async fn get_lease_amount(
        &self,
        contract_id: String,
    ) -> Result<BigDecimal, Error> {
        const SQL: &str = r#"
        SELECT SUM("Amount") as "Total"
        FROM (
            SELECT SUM("LS_loan_amnt") as "Amount"
            FROM "LS_Opening"
            WHERE "LS_contract_id" = $1
            UNION ALL
            SELECT -SUM("LS_amnt") as "Amount"
            FROM "LS_Close_Position"
            WHERE "LS_contract_id" = $1
            UNION ALL
            SELECT -SUM("LS_amnt") as "Amount"
            FROM "LS_Liquidation"
            WHERE "LS_contract_id" = $1
        )
        "#;

        sqlx::query_as(SQL)
            .bind(contract_id)
            .fetch_optional(&self.pool)
            .await
            .map(|f| f.map_or_else(BigDecimal::zero, |(result,)| result))
    }

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

    pub async fn get_leases(
        &self,
        address: String,
        skip: i64,
        limit: i64,
    ) -> Result<Vec<Pnl_Result>, Error> {
        const SQL: &str = r#"
        SELECT
            "closing"."LS_contract_id",
            "closing"."LS_amnt",
            "closing"."LS_amnt_stable",
            "closing"."LS_pnl",
            "closing"."LS_timestamp",
            "closing"."Type",
            "closing"."Block",
            "opening"."LS_asset_symbol",
            "opening"."LS_loan_pool_id"
        FROM "LS_Loan_Closing" AS "closing"
        INNER JOIN "LS_Opening" AS "opening" ON "closing"."LS_contract_id" = "opening"."LS_contract_id"
        WHERE
            "closing"."Active" = true AND
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

    pub async fn get_realized_pnl(
        &self,
        address: String,
    ) -> Result<Vec<Realized_Pnl_Result>, Error> {
        const SQL: &str = r#"
        SELECT
            "s"."LS_pnl",
            "o"."LS_loan_pool_id",
            "o"."LS_asset_symbol"
        FROM "LS_Loan_Closing" AS "s"
        LEFT JOIN "LS_Opening" AS "o" ON "o"."LS_contract_id" = "s"."LS_contract_id"
        WHERE "s"."LS_contract_id" IN (
            SELECT "LS_contract_id"
            FROM "LS_Opening"
            WHERE "LS_address_id" = $1
        )
        "#;

        sqlx::query_as(SQL)
            .bind(address)
            .fetch_all(&self.pool)
            .await
    }
}
