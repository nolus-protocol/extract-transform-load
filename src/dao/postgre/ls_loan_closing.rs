use std::str::FromStr as _;

use bigdecimal::BigDecimal;
use sqlx::{Error, Transaction};

use crate::model::{LS_Loan_Closing, Pnl_Result, Realized_Pnl_Result, Table};

use super::{DataBase, QueryResult};

impl Table<LS_Loan_Closing> {
    pub async fn isExists(
        &self,
        contract: String,
    ) -> Result<bool, crate::error::Error> {
        let (value,): (i64,) = sqlx::query_as(
            r#"
            SELECT 
                COUNT(*)
            FROM "LS_Loan_Closing" 
            WHERE 
                "LS_contract_id" = $1
            "#,
        )
        .bind(contract)
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
        "#,
        )
        .bind(&data.LS_contract_id)
        .bind(&data.LS_amnt)
        .bind(&data.LS_amnt_stable)
        .bind(&data.LS_pnl)
        .bind(&data.LS_timestamp)
        .bind(&data.Type)
        .bind(&data.Block)
        .bind(&data.Active)
        .persistent(false)
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
        .persistent(false)
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
                SUM(
                    CASE
                    WHEN o."LS_asset_symbol" IN ('ALL_BTC', 'WBTC', 'CRO') THEN c."LS_pnl" / 100000000
                    WHEN o."LS_asset_symbol" IN ('ALL_SOL') THEN c."LS_pnl" / 1000000000
                    WHEN o."LS_asset_symbol" IN ('PICA') THEN c."LS_pnl" / 1000000000000
                    WHEN o."LS_asset_symbol" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS', 'ALL_ETH') THEN c."LS_pnl" / 1000000000000000000
                    ELSE c."LS_pnl" / 1000000
                    END
                ) AS "Total Adjusted Stable Amount"
                FROM 
                "LS_Loan_Closing" c
                LEFT JOIN 
                "LS_Opening" o 
                ON 
                c."LS_contract_id" = o."LS_contract_id"
                WHERE 
                c."LS_timestamp" >= '2025-01-01';
            "#,
        )
        .persistent(false)
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
        .bind(&data.Active)
        .bind(&data.LS_contract_id)
        .persistent(false)
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
        .persistent(false)
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
            SELECT 
                "LS_Loan_Closing"."LS_contract_id",
                "LS_Loan_Closing"."LS_amnt",
                "LS_Loan_Closing"."LS_amnt_stable",
                "LS_Loan_Closing"."LS_pnl",
                "LS_Loan_Closing"."LS_timestamp",
                "LS_Loan_Closing"."Type",
                "LS_Loan_Closing"."Block",
                "LS_Opening"."LS_asset_symbol",
                "LS_Opening"."LS_loan_pool_id",
                "LS_Auto_Close_Position"."LS_Close_Strategy",
                "LS_Auto_Close_Position"."LS_Close_Strategy_Ltv"
            FROM 
                "LS_Loan_Closing" 

            INNER JOIN 
                "LS_Opening" 
            ON 
                "LS_Loan_Closing"."LS_contract_id" = "LS_Opening"."LS_contract_id"
            
            LEFT JOIN 
                "LS_Auto_Close_Position" 
            ON 
                "LS_Loan_Closing"."LS_contract_id" = "LS_Auto_Close_Position"."LS_contract_id"  

            WHERE 
                "LS_Loan_Closing"."Active" = true
            AND
                "LS_Opening"."LS_address_id" = $1
            ORDER BY 
               "LS_Loan_Closing"."LS_timestamp" 
            DESC 
            OFFSET 
                $2 
            LIMIT 
                $3;
            "#,
        )
        .bind(address)
        .bind(skip)
        .bind(limit)
        .persistent(false)
        .fetch_all(&self.pool)
        .await?;
        Ok(data)
    }

    pub async fn get_realized_pnl(
        &self,
        address: String,
    ) -> Result<Vec<Realized_Pnl_Result>, crate::error::Error> {
        let data= sqlx::query_as(
            r#"
                SELECT
                    s."LS_pnl", o."LS_loan_pool_id", o."LS_asset_symbol"
                FROM "LS_Loan_Closing"  s
                LEFT JOIN "LS_Opening" o ON o."LS_contract_id" = s."LS_contract_id"
                WHERE s."LS_contract_id" IN 
                (
                    SELECT
                        "LS_contract_id"
                    FROM "LS_Opening"
                    WHERE
                        "LS_address_id" = $1
                )
            "#,
        )
        .bind(address)
        .persistent(false)
        .fetch_all(&self.pool)
        .await?;

        Ok(data)
    }

    pub async fn get_all(&self) -> Result<Vec<LS_Loan_Closing>, Error> {
        sqlx::query_as(r#"SELECT * FROM "LS_Loan_Closing""#)
            .persistent(false)
            .fetch_all(&self.pool)
            .await
    }
}
