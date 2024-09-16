use super::{DataBase, QueryResult};
use crate::model::{LS_Loan_Closing, Table};
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
                "LS_amnt",
                "LS_amnt_stable",
                "LS_timestamp",
                "Type"
            )
            VALUES($1, $2, $3, $4, $5, $6)
        "#,
        )
        .bind(&data.LS_contract_id)
        .bind(&data.LS_symbol)
        .bind(&data.LS_amnt)
        .bind(&data.LS_amnt_stable)
        .bind(&data.LS_timestamp)
        .bind(&data.Type)
        .execute(&mut **transaction)
        .await
    }
}
