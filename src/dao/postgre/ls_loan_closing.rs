use std::borrow::Borrow;

use bigdecimal::BigDecimal;
use sqlx::{error::Error, Transaction};

use crate::{
    custom_uint::UInt63,
    model::{LS_Loan_Closing, Pnl_Result, Realized_Pnl_Result, Table},
};

use super::DataBase;

impl<Str, Decimal> Table<LS_Loan_Closing<Str, Decimal>>
where
    Str: Borrow<str>,
    Decimal: Borrow<BigDecimal>,
{
    pub async fn isExists(&self, contract: &str) -> Result<bool, Error> {
        const SQL: &str = r#"
        SELECT COUNT(1) > 0
        FROM "LS_Loan_Closing"
        WHERE "LS_contract_id" = $1
        "#;

        sqlx::query_as(SQL)
            .bind(contract)
            .fetch_one(&self.pool)
            .await
            .map(|(result,)| result)
    }

    pub async fn insert(
        &self,
        &LS_Loan_Closing {
            ref LS_contract_id,
            ref LS_amnt,
            ref LS_amnt_stable,
            ref LS_pnl,
            LS_timestamp,
            ref Type,
            Block,
            Active,
        }: &LS_Loan_Closing<Str, Decimal>,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<(), Error>
    where
        Str: Borrow<str>,
        Decimal: Borrow<BigDecimal>,
    {
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
            .bind(LS_contract_id.borrow())
            .bind(LS_amnt.borrow())
            .bind(LS_amnt_stable.borrow())
            .bind(LS_pnl.borrow())
            .bind(LS_timestamp)
            .bind(Type.borrow())
            .bind(Block)
            .bind(Active)
            .execute(&mut **transaction)
            .await
            .map(drop)
    }

    pub async fn get_lease_amount(
        &self,
        contract_id: &str,
    ) -> Result<BigDecimal, Error> {
        const SQL: &str = r#"
        SELECT COALESCE(SUM("Amount"), 0) AS "Total"
        FROM (
            SELECT SUM("LS_loan_amnt") AS "Amount"
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
        ) AS "combined_data"
        "#;

        sqlx::query_as(SQL)
            .bind(contract_id)
            .fetch_one(&self.pool)
            .await
            .map(|(amnt,)| amnt)
    }

    pub async fn update(
        &self,
        &LS_Loan_Closing {
            ref LS_contract_id,
            ref LS_amnt,
            ref LS_amnt_stable,
            ref LS_pnl,
            Active,
            ..
        }: &LS_Loan_Closing<Str, Decimal>,
    ) -> Result<(), Error> {
        const SQL: &str = r#"
        UPDATE "LS_Loan_Closing"
        WHERE "LS_contract_id" = $1
        SET
            "LS_amnt" = $2,
            "LS_amnt_stable" = $3,
            "LS_pnl" = $4,
            "Active" = $5
        "#;

        sqlx::query(SQL)
            .bind(LS_contract_id.borrow())
            .bind(LS_amnt.borrow())
            .bind(LS_amnt_stable.borrow())
            .bind(LS_pnl.borrow())
            .bind(Active)
            .execute(&self.pool)
            .await
            .map(drop)
    }

    pub async fn get_leases_to_proceed(
        &self,
    ) -> Result<Vec<LS_Loan_Closing<String, BigDecimal>>, Error> {
        const SQL: &str = r#"
        SELECT *
        FROM "LS_Loan_Closing"
        WHERE "Active" = false
        "#;

        sqlx::query_as(SQL).fetch_all(&self.pool).await
    }

    pub async fn get_leases(
        &self,
        address: &str,
        skip: UInt63,
        limit: UInt63,
    ) -> Result<Vec<Pnl_Result>, Error> {
        const SQL: &str = r#"
        SELECT
            "LS_Loan_Closing"."LS_contract_id",
            "LS_Loan_Closing"."LS_amnt",
            "LS_Loan_Closing"."LS_amnt_stable",
            "LS_Loan_Closing"."LS_pnl",
            "LS_Loan_Closing"."LS_timestamp",
            "LS_Loan_Closing"."Type",
            "LS_Loan_Closing"."Block",
            "LS_Opening"."LS_asset_symbol",
            "LS_Opening"."LS_loan_pool_id"
        FROM "LS_Loan_Closing"
        INNER JOIN "LS_Opening" ON "LS_Loan_Closing"."LS_contract_id" = "LS_Opening"."LS_contract_id"
        WHERE
            "LS_Loan_Closing"."Active" = true AND
            "LS_Opening"."LS_address_id" = $1
        ORDER BY "LS_Loan_Closing"."LS_timestamp" DESC
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
        address: &str,
    ) -> Result<Vec<Realized_Pnl_Result>, Error> {
        const SQL: &str = r#"
        SELECT
            "s"."LS_pnl",
            "o"."LS_loan_pool_id",
            "o"."LS_asset_symbol"
        FROM "LS_Loan_Closing" AS "s"
        LEFT JOIN "LS_Opening" AS "o" ON o."LS_contract_id" = "s"."LS_contract_id"
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
