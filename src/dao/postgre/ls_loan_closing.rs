use std::str::FromStr;

use super::{DataBase, QueryResult};
use crate::model::{LS_Loan_Closing, Table};
use bigdecimal::BigDecimal;
use sqlx::{error::Error, Transaction};

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
                "LS_amnt",
                "LS_amnt_stable",
                "LS_pnl",
                "LS_timestamp",
                "Type"
            )
            VALUES($1, $2, $3, $4, $5, $6, $7)
        "#,
        )
        .bind(&data.LS_contract_id)
        .bind(&data.LS_symbol)
        .bind(&data.LS_amnt)
        .bind(&data.LS_amnt_stable)
        .bind(&data.LS_pnl)
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
