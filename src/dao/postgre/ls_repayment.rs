use bigdecimal::Zero as _;
use chrono::{DateTime, Utc};
use sqlx::{types::BigDecimal, Error, QueryBuilder, Transaction};

use crate::model::{LS_Repayment, Table};

use super::DataBase;

impl Table<LS_Repayment> {
    // FIXME Pass data by reference, as separate arguments or as a dedicated
    //  structure. Avoid the need for owned data.
    pub async fn isExists(
        &self,
        ls_repayment: &LS_Repayment,
    ) -> Result<bool, Error> {
        const SQL: &str = r#"
        SELECT EXISTS(
            SELECT
            FROM "LS_Repayment"
            WHERE
                "LS_repayment_height" = $1 AND
                "LS_contract_id" = $2 AND
                "LS_timestamp" = $3
        )
        "#;

        sqlx::query_as(SQL)
            .bind(ls_repayment.LS_repayment_height)
            .bind(&ls_repayment.LS_contract_id)
            .bind(ls_repayment.LS_timestamp)
            .fetch_one(&self.pool)
            .await
            .map(|(result,)| result)
    }

    // FIXME Pass data by reference, as separate arguments or as a dedicated
    //  structure. Avoid the need for owned data.
    pub async fn insert(
        &self,
        data: LS_Repayment,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<(), Error> {
        const SQL: &str = r#"
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
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
        "#;

        sqlx::query(SQL)
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
            .execute(&mut **transaction)
            .await
            .map(drop)
    }

    // FIXME Pass data by reference, as separate arguments or as a dedicated
    //  structure. Avoid the need for owned data.
    // FIXME Use iterators instead.
    pub async fn insert_many(
        &self,
        data: &Vec<LS_Repayment>,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<(), Error> {
        const SQL: &str = r#"
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
        "#;

        if data.is_empty() {
            return Ok(());
        }

        QueryBuilder::new(SQL)
            .push_values(data, |mut b, ls| {
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
            })
            .build()
            .persistent(false)
            .execute(&mut **transaction)
            .await
            .map(drop)
    }

    // FIXME Return data in a dedicated structure instead of as a tuple.
    pub async fn get_sum(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<
        (BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal),
        Error,
    > {
        const SQL: &str = r#"
        SELECT
            SUM("LS_prev_margin_stable"),
            SUM("LS_prev_interest_stable"),
            SUM("LS_current_margin_stable"),
            SUM("LS_current_interest_stable"),
            SUM("LS_principal_stable")
        FROM "LS_Repayment"
        WHERE
            "LS_timestamp" > $1 AND
            "LS_timestamp" < $2
        "#;

        sqlx::query_as(SQL)
            .bind(from)
            .bind(to)
            .fetch_optional(&self.pool)
            .await
            .map(|result| {
                result.unwrap_or_else(|| {
                    (
                        BigDecimal::zero(),
                        BigDecimal::zero(),
                        BigDecimal::zero(),
                        BigDecimal::zero(),
                        BigDecimal::zero(),
                    )
                })
            })
    }

    // FIXME Pass argument by reference.
    // FIXME Driver might limit number of returned rows.
    pub async fn get_by_contract(
        &self,
        contract: String,
    ) -> Result<Vec<LS_Repayment>, Error> {
        const SQL: &str = r#"
        SELECT *
        FROM "LS_Repayment"
        WHERE "LS_contract_id" = $1
        "#;

        sqlx::query_as(SQL)
            .bind(contract)
            .fetch_all(&self.pool)
            .await
    }
}
