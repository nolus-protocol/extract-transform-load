use sqlx::{Error, QueryBuilder, Transaction};

use crate::model::{LS_Liquidation, Table};

use super::DataBase;

impl Table<LS_Liquidation> {
    pub async fn isExists(
        &self,
        ls_liquidatiion: &LS_Liquidation,
    ) -> Result<bool, Error> {
        const SQL: &str = r#"
        SELECT EXISTS(
            SELECT
            FROM "LS_Liquidation"
            WHERE
                "LS_liquidation_height" = $1 AND
                "LS_contract_id" = $2
        )
        "#;

        sqlx::query_as(SQL)
            .bind(ls_liquidatiion.LS_liquidation_height)
            .bind(&ls_liquidatiion.LS_contract_id)
            .fetch_one(&self.pool)
            .await
            .map(|(result,)| result)
    }

    pub async fn insert(
        &self,
        data: &LS_Liquidation,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<(), Error> {
        const SQL: &str = r#"
        INSERT INTO "LS_Liquidation" (
            "LS_liquidation_height",
            "LS_contract_id",
            "LS_amnt_symbol",
            "LS_timestamp",
            "LS_amnt_stable",
            "LS_transaction_type",
            "LS_prev_margin_stable",
            "LS_prev_interest_stable",
            "LS_current_margin_stable",
            "LS_current_interest_stable",
            "LS_principal_stable",
            "Tx_Hash",
            "LS_amnt",
            "LS_payment_symbol",
            "LS_payment_amnt",
            "LS_payment_amnt_stable",
            "LS_loan_close"
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
        "#;

        sqlx::query(SQL)
            .bind(data.LS_liquidation_height)
            .bind(&data.LS_contract_id)
            .bind(&data.LS_amnt_symbol)
            .bind(data.LS_timestamp)
            .bind(&data.LS_amnt_stable)
            .bind(&data.LS_transaction_type)
            .bind(&data.LS_prev_margin_stable)
            .bind(&data.LS_prev_interest_stable)
            .bind(&data.LS_current_margin_stable)
            .bind(&data.LS_current_interest_stable)
            .bind(&data.LS_principal_stable)
            .bind(&data.Tx_Hash)
            .bind(&data.LS_amnt)
            .bind(&data.LS_payment_symbol)
            .bind(&data.LS_payment_amnt)
            .bind(&data.LS_payment_amnt_stable)
            .bind(data.LS_loan_close)
            .execute(transaction.as_mut())
            .await
            .map(drop)
    }

    pub async fn insert_many(
        &self,
        data: &Vec<LS_Liquidation>,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<(), Error> {
        const SQL: &str = r#"
        INSERT INTO "LS_Liquidation" (
            "LS_liquidation_height",
            "LS_contract_id",
            "LS_amnt_symbol",
            "LS_timestamp",
            "LS_amnt_stable",
            "LS_transaction_type",
            "LS_prev_margin_stable",
            "LS_prev_interest_stable",
            "LS_current_margin_stable",
            "LS_current_interest_stable",
            "LS_principal_stable",
            "Tx_Hash",
            "LS_amnt",
            "LS_payment_symbol",
            "LS_payment_amnt",
            "LS_payment_amnt_stable",
            "LS_loan_close"
        )
        "#;

        if data.is_empty() {
            return Ok(());
        }

        QueryBuilder::new(SQL)
            .push_values(data, |mut b, ls| {
                b.push_bind(ls.LS_liquidation_height)
                    .push_bind(&ls.LS_contract_id)
                    .push_bind(&ls.LS_amnt_symbol)
                    .push_bind(ls.LS_timestamp)
                    .push_bind(&ls.LS_amnt_stable)
                    .push_bind(&ls.LS_transaction_type)
                    .push_bind(&ls.LS_prev_margin_stable)
                    .push_bind(&ls.LS_prev_interest_stable)
                    .push_bind(&ls.LS_current_margin_stable)
                    .push_bind(&ls.LS_current_interest_stable)
                    .push_bind(&ls.LS_principal_stable)
                    .push_bind(&ls.Tx_Hash)
                    .push_bind(&ls.LS_amnt)
                    .push_bind(&ls.LS_payment_symbol)
                    .push_bind(&ls.LS_payment_amnt)
                    .push_bind(&ls.LS_payment_amnt_stable)
                    .push_bind(ls.LS_loan_close);
            })
            .build()
            .persistent(false)
            .execute(transaction.as_mut())
            .await
            .map(drop)
    }

    pub async fn get_by_contract(
        &self,
        contract: String,
    ) -> Result<Vec<LS_Liquidation>, Error> {
        const SQL: &str = r#"
        SELECT *
        FROM "LS_Liquidation"
        WHERE "LS_contract_id" = $1
        "#;

        sqlx::query_as(SQL)
            .bind(&contract)
            .fetch_all(&self.pool)
            .await
    }
}
