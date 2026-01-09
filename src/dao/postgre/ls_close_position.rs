use sqlx::{Error, QueryBuilder, Transaction};

use crate::model::{LS_Close_Position, Table};

use super::{DataBase, QueryResult};

impl Table<LS_Close_Position> {
    pub async fn isExists(
        &self,
        ls_close_position: &LS_Close_Position,
    ) -> Result<bool, crate::error::Error> {
        let (value,): (i64,) = sqlx::query_as(
            r#"
            SELECT
                COUNT(*)
            FROM "LS_Close_Position"
            WHERE
                "LS_position_height" = $1 AND
                "LS_contract_id" = $2
            "#,
        )
        .bind(ls_close_position.LS_position_height)
        .bind(&ls_close_position.LS_contract_id)
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
        data: &LS_Close_Position,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<QueryResult, Error> {
        sqlx::query(
            r#"
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
            VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
        "#,
        )
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
        .persistent(false)
        .execute(&mut **transaction)
        .await
    }

    pub async fn insert_many(
        &self,
        data: &Vec<LS_Close_Position>,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<(), Error> {
        if data.is_empty() {
            return Ok(());
        }

        let mut query_builder: QueryBuilder<DataBase> = QueryBuilder::new(
            r#"
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
            )"#,
        );

        query_builder.push_values(data, |mut b, ls| {
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
        });

        let query = query_builder.build().persistent(false);
        query.execute(&mut **transaction).await?;
        Ok(())
    }

    pub async fn get_by_contract(
        &self,
        contract: String,
    ) -> Result<Vec<LS_Close_Position>, Error> {
        let data = sqlx::query_as(
            r#"
            SELECT * FROM "LS_Close_Position" WHERE "LS_contract_id" = $1;
            "#,
        )
        .bind(&contract)
        .persistent(false)
        .fetch_all(&self.pool)
        .await?;
        Ok(data)
    }

    pub async fn get_closed_by_contract(
        &self,
        contract: String,
    ) -> Result<LS_Close_Position, Error> {
        sqlx::query_as(
            r#"
            SELECT * FROM "LS_Close_Position" WHERE "LS_contract_id" = $1 AND "LS_loan_close" = true;
            "#,
        )
        .bind(&contract)
        .persistent(false)
        .fetch_one(&self.pool)
        .await
    }
}
