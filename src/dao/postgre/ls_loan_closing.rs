use std::str::FromStr;

use super::{DataBase, QueryResult};
use crate::model::{LS_Loan_Closing, Table};
use bigdecimal::BigDecimal;
use sqlx::{error::Error, Transaction};

impl Table<LS_Loan_Closing> {
    pub async fn isExists(
        &self,
        ls_loan_closing: &LS_Loan_Closing,
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
        .bind(&ls_loan_closing.LS_contract_id)
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
                "LS_symbol",
                "LS_amnt_stable",
                "LS_timestamp",
                "Type"
            )
            VALUES($1, $2, $3, $4, $5)
        "#,
        )
        .bind(&data.LS_contract_id)
        .bind(&data.LS_symbol)
        .bind(&data.LS_amnt_stable)
        .bind(&data.LS_timestamp)
        .bind(&data.Type)
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
                WITH Opened_Leases AS (
                    SELECT
                    CASE
                    WHEN 
                        "LS_cltr_symbol" IN ('WBTC', 'CRO') THEN "LS_cltr_amnt_stable" / 100000000 
                    WHEN 
                        "LS_cltr_symbol" IN ('PICA') THEN "LS_cltr_amnt_stable" / 1000000000000 
                    WHEN 
                        "LS_cltr_symbol" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS') THEN "LS_cltr_amnt_stable" / 1000000000000000000
                    ELSE 
                        "LS_cltr_amnt_stable" / 1000000
                    END AS 
                        "Downpayment",
                    "LS_loan_amnt_asset" / 1000000 AS "Loan"
                    FROM "LS_Opening"
                    WHERE "LS_contract_id" = $1
                )
                SELECT
                SUM ("Volume") AS "Amount"
                FROM (
                    SELECT ("Downpayment" + "Loan") AS "Volume" FROM Opened_Leases
                )

                UNION ALL

                    SELECT
                    -CASE
                    WHEN 
                        "LS_payment_symbol" IN ('WBTC', 'CRO') THEN "LS_payment_amnt_stable" / 100000000 
                    WHEN 
                        "LS_payment_symbol" IN ('PICA') THEN "LS_payment_amnt_stable" / 1000000000000 
                    WHEN 
                        "LS_payment_symbol" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS') THEN "LS_payment_amnt_stable" / 1000000000000000000
                    ELSE 
                        "LS_payment_amnt_stable" / 1000000
                    END AS 
                        "Amount"

                    FROM "LS_Close_Position"
                    WHERE "LS_contract_id" = $1

                UNION ALL

                    SELECT
                    -CASE
                    WHEN 
                        "LS_symbol" IN ('WBTC', 'CRO') THEN "LS_amnt_stable" / 100000000 
                    WHEN 
                        "LS_symbol" IN ('PICA') THEN "LS_amnt_stable" / 1000000000000 
                    WHEN 
                        "LS_symbol" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS') THEN "LS_amnt_stable" / 1000000000000000000
                    ELSE 
                        "LS_amnt_stable" / 1000000
                    END AS 
                        "Amount"

                    FROM "LS_Liquidation"
                    WHERE "LS_contract_id" = $1
            )
            "#,
        )
        .bind(contract_id)
        .fetch_one(&self.pool)
        .await?;
        let (amnt,) = value;
        let amnt = amnt.unwrap_or(BigDecimal::from_str("0")?);

        Ok(amnt)
    }
}
