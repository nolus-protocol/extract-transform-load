use sqlx::{Error, QueryBuilder, Transaction};

use crate::model::{LS_Close_Position, Table};

use super::DataBase;

impl Table<LS_Close_Position> {
    // FIXME Pass data by reference, as separate arguments or as a dedicated
    //  structure. Avoid the need for owned data.
    pub async fn isExists(
        &self,
        ls_close_position: &LS_Close_Position,
    ) -> Result<bool, Error> {
        const SQL: &str = r#"
        SELECT EXISTS (
            SELECT
            FROM "LS_Close_Position"
            WHERE
                "LS_position_height" = $1 AND
                "LS_contract_id" = $2
        )
        "#;

        sqlx::query_as(SQL)
            .bind(ls_close_position.LS_position_height)
            .bind(&ls_close_position.LS_contract_id)
            .fetch_one(&self.pool)
            .await
            .map(|(result,)| result)
    }

    // FIXME Pass data by reference, as separate arguments or as a dedicated
    //  structure. Avoid the need for owned data.
    pub async fn insert(
        &self,
        data: &LS_Close_Position,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<(), Error> {
        const SQL: &str = r#"
        INSERT INTO "LS_Close_Position" (
            "LS_position_height",
            "LS_contract_id",
            "LS_payment_amnt_stable",
            "LS_change",
            "LS_amnt",
            "LS_amnt_symbol",
            "LS_timestamp",
            "LS_loan_close",
            "LS_prev_margin_stable",
            "LS_prev_interest_stable",
            "LS_current_margin_stable",
            "LS_current_interest_stable",
            "LS_principal_stable",
            "Tx_Hash",
            "LS_amnt_stable",
            "LS_payment_amnt",
            "LS_payment_symbol"
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
        "#;

        sqlx::query(SQL)
            .bind(data.LS_position_height)
            .bind(&data.LS_contract_id)
            .bind(&data.LS_payment_amnt_stable)
            .bind(&data.LS_change)
            .bind(&data.LS_amnt)
            .bind(&data.LS_amnt_symbol)
            .bind(data.LS_timestamp)
            .bind(data.LS_loan_close)
            .bind(&data.LS_prev_margin_stable)
            .bind(&data.LS_prev_interest_stable)
            .bind(&data.LS_current_margin_stable)
            .bind(&data.LS_current_interest_stable)
            .bind(&data.LS_principal_stable)
            .bind(&data.Tx_Hash)
            .bind(&data.LS_amnt_stable)
            .bind(&data.LS_payment_amnt)
            .bind(&data.LS_payment_symbol)
            .execute(transaction.as_mut())
            .await
            .map(drop)
    }

    // FIXME Pass data by reference, as separate arguments or as a dedicated
    //  structure. Avoid the need for owned data.
    // FIXME Use iterators instead.
    pub async fn insert_many(
        &self,
        data: &Vec<LS_Close_Position>,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<(), Error> {
        const SQL: &str = r#"
        INSERT INTO "LS_Close_Position" (
            "LS_position_height",
            "LS_contract_id",
            "LS_payment_amnt_stable",
            "LS_change",
            "LS_amnt" ,
            "LS_amnt_symbol",
            "LS_timestamp",
            "LS_loan_close",
            "LS_prev_margin_stable",
            "LS_prev_interest_stable",
            "LS_current_margin_stable",
            "LS_current_interest_stable",
            "LS_principal_stable",
            "Tx_Hash",
            "LS_amnt_stable",
            "LS_payment_amnt",
            "LS_payment_symbol"
        )
        "#;

        if data.is_empty() {
            return Ok(());
        }

        QueryBuilder::new(SQL)
            .push_values(data, |mut b, ls| {
                b.push_bind(ls.LS_position_height)
                    .push_bind(&ls.LS_contract_id)
                    .push_bind(&ls.LS_payment_amnt_stable)
                    .push_bind(&ls.LS_change)
                    .push_bind(&ls.LS_amnt)
                    .push_bind(&ls.LS_amnt_symbol)
                    .push_bind(ls.LS_timestamp)
                    .push_bind(ls.LS_loan_close)
                    .push_bind(&ls.LS_prev_margin_stable)
                    .push_bind(&ls.LS_prev_interest_stable)
                    .push_bind(&ls.LS_current_margin_stable)
                    .push_bind(&ls.LS_current_interest_stable)
                    .push_bind(&ls.LS_principal_stable)
                    .push_bind(&ls.Tx_Hash)
                    .push_bind(&ls.LS_amnt_stable)
                    .push_bind(&ls.LS_payment_amnt)
                    .push_bind(&ls.LS_payment_symbol);
            })
            .build()
            .persistent(false)
            .execute(transaction.as_mut())
            .await
            .map(drop)
    }

    // FIXME Pass argument by reference.
    // FIXME Driver might limit number of returned rows.
    pub async fn get_by_contract(
        &self,
        contract: String,
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
