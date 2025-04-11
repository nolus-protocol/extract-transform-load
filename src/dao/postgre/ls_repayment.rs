use std::str::FromStr as _;

use chrono::{DateTime, Utc};
use sqlx::{types::BigDecimal, Error, QueryBuilder, Transaction};

use crate::model::{LS_Repayment, Table};

use super::{DataBase, QueryResult};

type OptionDecimal = Option<BigDecimal>;

impl Table<LS_Repayment> {
    pub async fn isExists(
        &self,
        ls_repayment: &LS_Repayment,
    ) -> Result<bool, crate::error::Error> {
        let (value,): (i64,) = sqlx::query_as(
            r#"
            SELECT 
                COUNT(*)
            FROM "LS_Repayment" 
            WHERE 
                "LS_repayment_height" = $1 AND
                "LS_contract_id" = $2 AND
                "LS_timestamp" = $3
            "#,
        )
        .bind(ls_repayment.LS_repayment_height)
        .bind(&ls_repayment.LS_contract_id)
        .bind(ls_repayment.LS_timestamp)
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
        data: LS_Repayment,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<QueryResult, Error> {
        sqlx::query(
            r#"
            INSERT INTO "LS_Repayment" (
                "LS_repayment_height",
                "LS_contract_id",
                "LS_payment_symbol",
                "LS_payment_amnt",
                "LS_payment_amnt_stable",
                "LS_timestamp",
                "LS_loan_close",
                "LS_prev_margin_stable",
                "LS_prev_interest_stable",
                "LS_current_margin_stable",
                "LS_current_interest_stable",
                "LS_principal_stable",
                "Tx_Hash"
            )
            VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
        "#,
        )
        .bind(data.LS_repayment_height)
        .bind(&data.LS_contract_id)
        .bind(&data.LS_payment_symbol)
        .bind(&data.LS_payment_amnt)
        .bind(&data.LS_payment_amnt_stable)
        .bind(data.LS_timestamp)
        .bind(data.LS_loan_close)
        .bind(&data.LS_prev_margin_stable)
        .bind(&data.LS_prev_interest_stable)
        .bind(&data.LS_current_margin_stable)
        .bind(&data.LS_current_interest_stable)
        .bind(&data.LS_principal_stable)
        .bind(&data.Tx_Hash)
        .persistent(false)
        .execute(&mut **transaction)
        .await
    }

    pub async fn insert_many(
        &self,
        data: &Vec<LS_Repayment>,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<(), Error> {
        if data.is_empty() {
            return Ok(());
        }

        let mut query_builder: QueryBuilder<DataBase> = QueryBuilder::new(
            r#"
            INSERT INTO "LS_Repayment" (
                "LS_repayment_height",
                "LS_contract_id",
                "LS_payment_symbol",
                "LS_payment_amnt",
                "LS_payment_amnt_stable",
                "LS_timestamp",
                "LS_loan_close",
                "LS_prev_margin_stable",
                "LS_prev_interest_stable",
                "LS_current_margin_stable",
                "LS_current_interest_stable",
                "LS_principal_stable",
                "Tx_Hash"
            )"#,
        );

        query_builder.push_values(data, |mut b, ls| {
            b.push_bind(ls.LS_repayment_height)
                .push_bind(&ls.LS_contract_id)
                .push_bind(&ls.LS_payment_symbol)
                .push_bind(&ls.LS_payment_amnt)
                .push_bind(&ls.LS_payment_amnt_stable)
                .push_bind(ls.LS_timestamp)
                .push_bind(ls.LS_loan_close)
                .push_bind(&ls.LS_prev_margin_stable)
                .push_bind(&ls.LS_prev_interest_stable)
                .push_bind(&ls.LS_current_margin_stable)
                .push_bind(&ls.LS_current_interest_stable)
                .push_bind(&ls.LS_principal_stable)
                .push_bind(&ls.Tx_Hash);
        });

        let query = query_builder.build().persistent(false);
        query.execute(&mut **transaction).await?;
        Ok(())
    }

    pub async fn get_sum(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<
        (BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal),
        crate::error::Error,
    > {
        let value: (
            OptionDecimal,
            OptionDecimal,
            OptionDecimal,
            OptionDecimal,
            OptionDecimal,
        ) = sqlx::query_as(
            r#"
            SELECT 
                SUM("LS_prev_margin_stable"),
                SUM("LS_prev_interest_stable"),
                SUM("LS_current_margin_stable"),
                SUM("LS_current_interest_stable"),
                SUM("LS_principal_stable")
            FROM "LS_Repayment" WHERE "LS_timestamp" > $1 AND "LS_timestamp" < $2
            "#,
        )
        .bind(from)
        .bind(to)
        .persistent(false)
        .fetch_one(&self.pool)
        .await?;
        let (
            prev_margin_stable,
            prev_interest_stable,
            current_margin_stable,
            current_interest_stable,
            prinicap_stable,
        ) = value;

        let prev_margin_stable =
            prev_margin_stable.unwrap_or(BigDecimal::from_str("0")?);
        let prev_interest_stable =
            prev_interest_stable.unwrap_or(BigDecimal::from_str("0")?);
        let current_margin_stable =
            current_margin_stable.unwrap_or(BigDecimal::from_str("0")?);
        let current_interest_stable =
            current_interest_stable.unwrap_or(BigDecimal::from_str("0")?);
        let prinicap_stable =
            prinicap_stable.unwrap_or(BigDecimal::from_str("0")?);

        Ok((
            prev_margin_stable,
            prev_interest_stable,
            current_margin_stable,
            current_interest_stable,
            prinicap_stable,
        ))
    }

    pub async fn get_by_contract(
        &self,
        contract: String,
    ) -> Result<Vec<LS_Repayment>, Error> {
        let data = sqlx::query_as(
            r#"
                SELECT
                    "LS_repayment_height",
                    "LS_repayment_idx",
                    "LS_contract_id",
                    "LS_payment_symbol",
                    "LS_payment_amnt",
                    "LS_payment_amnt_stable",
                    "LS_timestamp",
                    "LS_loan_close",
                    "LS_prev_margin_stable",
                    "LS_prev_interest_stable",
                    "LS_current_margin_stable",
                    "LS_current_interest_stable",
                    "LS_principal_stable",
                    "Tx_Hash"
                FROM
                    "LS_Repayment" as a
                WHERE
                    a."LS_contract_id" = $1
            "#,
        )
        .bind(contract)
        .persistent(false)
        .fetch_all(&self.pool)
        .await?;
        Ok(data)
    }
}
