use std::iter;

use chrono::{DateTime, Utc};
use sqlx::{error::Error, types::BigDecimal, QueryBuilder, Transaction};

use crate::model::{LS_Repayment, Table};

use super::DataBase;

impl Table<LS_Repayment> {
    pub async fn isExists(
        &self,
        &LS_Repayment {
            LS_repayment_height,
            ref LS_contract_id,
            LS_timestamp,
            ..
        }: &LS_Repayment,
    ) -> Result<bool, Error> {
        const SQL: &str = r#"
        SELECT COUNT(1)
        FROM "LS_Repayment"
        WHERE
            "LS_repayment_height" = $1 AND
            "LS_contract_id" = $2 AND
            "LS_timestamp" = $3
        "#;

        sqlx::query_as(SQL)
            .bind(LS_repayment_height)
            .bind(LS_contract_id)
            .bind(LS_timestamp)
            .fetch_one(&self.pool)
            .await
            .map(|(result,)| result)
    }

    pub async fn insert(
        &self,
        &LS_Repayment {
            LS_repayment_height,
            LS_repayment_idx: _,
            ref LS_contract_id,
            ref LS_payment_symbol,
            ref LS_payment_amnt,
            ref LS_payment_amnt_stable,
            LS_timestamp,
            LS_loan_close,
            ref LS_prev_margin_stable,
            ref LS_prev_interest_stable,
            ref LS_current_margin_stable,
            ref LS_current_interest_stable,
            ref LS_principal_stable,
            ref Tx_Hash,
        }: &LS_Repayment,
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
        VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
        "#;

        sqlx::query(SQL)
            .bind(LS_repayment_height)
            .bind(LS_contract_id)
            .bind(LS_payment_symbol)
            .bind(LS_payment_amnt)
            .bind(LS_payment_amnt_stable)
            .bind(LS_timestamp)
            .bind(LS_loan_close)
            .bind(LS_prev_margin_stable)
            .bind(LS_prev_interest_stable)
            .bind(LS_current_margin_stable)
            .bind(LS_current_interest_stable)
            .bind(LS_principal_stable)
            .bind(Tx_Hash)
            .execute(&mut **transaction)
            .await
            .map(drop)
    }

    pub async fn insert_many<'r, T>(
        &self,
        data: T,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<(), Error>
    where
        T: IntoIterator<Item = &'r LS_Repayment>,
    {
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

        let mut iter = data.into_iter();

        let Some(first) = iter.next() else {
            return Ok(());
        };

        QueryBuilder::new(SQL)
            .push_values(
                iter::once(first).chain(iter),
                |mut b,
                 &LS_Repayment {
                     LS_repayment_height,
                     LS_repayment_idx,
                     ref LS_contract_id,
                     ref LS_payment_symbol,
                     ref LS_payment_amnt,
                     ref LS_payment_amnt_stable,
                     LS_timestamp,
                     LS_loan_close,
                     ref LS_prev_margin_stable,
                     ref LS_prev_interest_stable,
                     ref LS_current_margin_stable,
                     ref LS_current_interest_stable,
                     ref LS_principal_stable,
                     ref Tx_Hash,
                 }| {
                    b.push_bind(LS_repayment_height)
                        .push_bind(LS_contract_id)
                        .push_bind(LS_payment_symbol)
                        .push_bind(LS_payment_amnt)
                        .push_bind(LS_payment_amnt_stable)
                        .push_bind(LS_timestamp)
                        .push_bind(LS_loan_close)
                        .push_bind(LS_prev_margin_stable)
                        .push_bind(LS_prev_interest_stable)
                        .push_bind(LS_current_margin_stable)
                        .push_bind(LS_current_interest_stable)
                        .push_bind(LS_principal_stable)
                        .push_bind(Tx_Hash);
                },
            )
            .build()
            .execute(&mut **transaction)
            .await
            .map(drop)
    }

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
            COALESCE(SUM("LS_prev_margin_stable"), 0),
            COALESCE(SUM("LS_prev_interest_stable"), 0),
            COALESCE(SUM("LS_current_margin_stable"), 0),
            COALESCE(SUM("LS_current_interest_stable"), 0),
            COALESCE(SUM("LS_principal_stable", 0))
        FROM "LS_Repayment"
        WHERE
            "LS_timestamp" > $1 AND
            "LS_timestamp" < $2
        "#;

        sqlx::query_as(SQL)
            .bind(from)
            .bind(to)
            .fetch_one(&self.pool)
            .await
    }

    pub async fn get_by_contract(
        &self,
        contract: &str,
    ) -> Result<Vec<LS_Repayment>, Error> {
        const SQL: &str = r#"
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
        FROM "LS_Repayment"
        WHERE "LS_contract_id" = $1
        "#;

        sqlx::query_as(SQL)
            .bind(contract)
            .fetch_all(&self.pool)
            .await
    }
}
