use super::{DataBase, QueryResult};
use crate::model::{LS_Close_Position, Table};
use sqlx::{error::Error, QueryBuilder, Transaction};

impl Table<LS_Close_Position> {
    pub async fn insert(
        &self,
        data: LS_Close_Position,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<QueryResult, Error> {
        sqlx::query(
            r#"
            INSERT INTO "LS_Close_Position" (
                "LS_position_height",
                "LS_contract_id",
                "LS_symbol",
                "LS_amnt_stable",
                "LS_change",
                "LS_amount_amount" ,
                "LS_amount_symbol",
                "LS_timestamp",
                "LS_loan_close",
                "LS_prev_margin_stable",
                "LS_prev_interest_stable",
                "LS_current_margin_stable",
                "LS_current_interest_stable",
                "LS_principal_stable"
            )
            VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
        "#,
        )
        .bind(data.LS_position_height)
        .bind(&data.LS_contract_id)
        .bind(&data.LS_symbol)
        .bind(&data.LS_amnt_stable)
        .bind(&data.LS_change)
        .bind(&data.LS_amount_amount)
        .bind(&data.LS_amount_symbol)
        .bind(data.LS_timestamp)
        .bind(data.LS_loan_close)
        .bind(&data.LS_prev_margin_stable)
        .bind(&data.LS_prev_interest_stable)
        .bind(&data.LS_current_margin_stable)
        .bind(&data.LS_current_interest_stable)
        .bind(&data.LS_principal_stable)
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
            INSERT INTO "LS_Repayment" (
                "LS_position_height",
                "LS_contract_id",
                "LS_symbol",
                "LS_amnt_stable",
                "LS_change",
                "LS_amount_amount" ,
                "LS_amount_symbol",
                "LS_timestamp",
                "LS_loan_close",
                "LS_prev_margin_stable",
                "LS_prev_interest_stable",
                "LS_current_margin_stable",
                "LS_current_interest_stable",
                "LS_principal_stable"
            )"#,
        );

        query_builder.push_values(data, |mut b, ls| {
            b.push_bind(ls.LS_position_height)
                .push_bind(&ls.LS_contract_id)
                .push_bind(&ls.LS_symbol)
                .push_bind(&ls.LS_amnt_stable)
                .push_bind(&ls.LS_change)
                .push_bind(&ls.LS_amount_amount)
                .push_bind(&ls.LS_amount_symbol)
                .push_bind(ls.LS_timestamp)
                .push_bind(ls.LS_loan_close)
                .push_bind(&ls.LS_prev_margin_stable)
                .push_bind(&ls.LS_prev_interest_stable)
                .push_bind(&ls.LS_current_margin_stable)
                .push_bind(&ls.LS_current_interest_stable)
                .push_bind(&ls.LS_principal_stable);
        });

        let query = query_builder.build();
        query.execute(&mut **transaction).await?;
        Ok(())
    }
}
