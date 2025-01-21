use std::iter;

use sqlx::{error::Error, QueryBuilder, Transaction};

use crate::model::{LS_Close_Position, Table};

use super::DataBase;

impl Table<LS_Close_Position> {
    pub async fn isExists(
        &self,
        &LS_Close_Position {
            LS_position_height,
            ref LS_contract_id,
            ..
        }: &LS_Close_Position,
    ) -> Result<bool, Error> {
        const SQL: &str = r#"
        SELECT COUNT(1) > 0
        FROM "LS_Close_Position"
        WHERE
            "LS_position_height" = $1 AND
            "LS_contract_id" = $2
        "#;

        sqlx::query_as(SQL)
            .bind(LS_position_height)
            .bind(LS_contract_id)
            .fetch_one(&self.pool)
            .await
            .map(|(result,)| result)
    }

    pub async fn insert(
        &self,
        &LS_Close_Position {
            ref Tx_Hash,
            LS_position_height,
            LS_position_idx: _,
            ref LS_contract_id,
            ref LS_change,
            ref LS_amnt,
            ref LS_amnt_symbol,
            ref LS_amnt_stable,
            ref LS_payment_amnt,
            ref LS_payment_symbol,
            ref LS_payment_amnt_stable,
            LS_timestamp,
            LS_loan_close,
            ref LS_prev_margin_stable,
            ref LS_prev_interest_stable,
            ref LS_current_margin_stable,
            ref LS_current_interest_stable,
            ref LS_principal_stable,
        }: &LS_Close_Position,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<(), Error> {
        const SQL: &str = r#"
        INSERT INTO "LS_Close_Position" (
            "Tx_Hash",
            "LS_position_height",
            "LS_contract_id",
            "LS_change",
            "LS_amnt",
            "LS_amnt_symbol",
            "LS_amnt_stable",
            "LS_payment_amnt",
            "LS_payment_symbol",
            "LS_payment_amnt_stable",
            "LS_timestamp",
            "LS_loan_close",
            "LS_prev_margin_stable",
            "LS_prev_interest_stable",
            "LS_current_margin_stable",
            "LS_current_interest_stable",
            "LS_principal_stable"
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
        "#;

        sqlx::query(SQL)
            .bind(Tx_Hash)
            .bind(LS_position_height)
            .bind(LS_contract_id)
            .bind(LS_change)
            .bind(LS_amnt)
            .bind(LS_amnt_symbol)
            .bind(LS_amnt_stable)
            .bind(LS_payment_amnt)
            .bind(LS_payment_symbol)
            .bind(LS_payment_amnt_stable)
            .bind(LS_timestamp)
            .bind(LS_loan_close)
            .bind(LS_prev_margin_stable)
            .bind(LS_prev_interest_stable)
            .bind(LS_current_margin_stable)
            .bind(LS_current_interest_stable)
            .bind(LS_principal_stable)
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
        T: IntoIterator<Item = &'r LS_Close_Position>,
    {
        const SQL: &str = r#"
        INSERT INTO "LS_Close_Position" (
            "Tx_Hash",
            "LS_position_height",
            "LS_contract_id",
            "LS_change",
            "LS_amnt",
            "LS_amnt_symbol",
            "LS_amnt_stable",
            "LS_payment_amnt",
            "LS_payment_symbol",
            "LS_payment_amnt_stable",
            "LS_timestamp",
            "LS_loan_close",
            "LS_prev_margin_stable",
            "LS_prev_interest_stable",
            "LS_current_margin_stable",
            "LS_current_interest_stable",
            "LS_principal_stable"
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
                 &LS_Close_Position {
                     ref Tx_Hash,
                     LS_position_height,
                     LS_position_idx,
                     ref LS_contract_id,
                     ref LS_change,
                     ref LS_amnt,
                     ref LS_amnt_symbol,
                     ref LS_amnt_stable,
                     ref LS_payment_amnt,
                     ref LS_payment_symbol,
                     ref LS_payment_amnt_stable,
                     LS_timestamp,
                     LS_loan_close,
                     ref LS_prev_margin_stable,
                     ref LS_prev_interest_stable,
                     ref LS_current_margin_stable,
                     ref LS_current_interest_stable,
                     ref LS_principal_stable,
                 }| {
                    b.push_bind(Tx_Hash)
                        .push_bind(LS_position_height)
                        .push_bind(LS_contract_id)
                        .push_bind(LS_change)
                        .push_bind(LS_amnt)
                        .push_bind(LS_amnt_symbol)
                        .push_bind(LS_amnt_stable)
                        .push_bind(LS_payment_amnt)
                        .push_bind(LS_payment_symbol)
                        .push_bind(LS_payment_amnt_stable)
                        .push_bind(LS_timestamp)
                        .push_bind(LS_loan_close)
                        .push_bind(LS_prev_margin_stable)
                        .push_bind(LS_prev_interest_stable)
                        .push_bind(LS_current_margin_stable)
                        .push_bind(LS_current_interest_stable)
                        .push_bind(LS_principal_stable);
                },
            )
            .build()
            .execute(&mut **transaction)
            .await
            .map(drop)
    }

    pub async fn get_by_contract(
        &self,
        contract: &str,
    ) -> Result<Vec<LS_Close_Position>, Error> {
        const SQL: &str = r#"
        SELECT *
        FROM "LS_Close_Position"
        WHERE "LS_contract_id" = $1
        "#;

        sqlx::query_as(SQL)
            .bind(&contract)
            .fetch_all(&self.pool)
            .await
    }
}
