use super::{DataBase, QueryResult};
use crate::model::{LS_Close_Position, Table};
use sqlx::{error::Error, QueryBuilder, Transaction};

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
        .fetch_one(&self.pool)
        .await?;

        if value > 0 {
            sqlx::query(
                r#"
                    UPDATE 
                        "LS_Close_Position" 
                    SET 
                        "Tx_Hash" = $1,
                        "LS_amnt_stable" = $2,
                        "LS_payment_amnt" = $3,
                        "LS_payment_symbol" = $4
                    WHERE 
                         "LS_position_height" = $5 AND
                         "LS_contract_id" = $6
                "#,
            )
            .bind(&ls_close_position.Tx_Hash)
            .bind(&ls_close_position.LS_amnt_stable)
            .bind(&ls_close_position.LS_payment_amnt)
            .bind(&ls_close_position.LS_payment_symbol)
            .bind(ls_close_position.LS_position_height)
            .bind(&ls_close_position.LS_contract_id)
            .execute(&self.pool)
            .await?;

            return Ok(true);
        }

        Ok(false)
    }

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
                "LS_payment_amnt_stable",
                "LS_change",
                "LS_amount_amount" ,
                "LS_amount_symbol",
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
        .bind(&data.LS_amount_amount)
        .bind(&data.LS_amount_symbol)
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
                "LS_payment_amnt_stable",
                "LS_change",
                "LS_amount_amount" ,
                "LS_amount_symbol",
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
                .push_bind(&ls.LS_amount_amount)
                .push_bind(&ls.LS_amount_symbol)
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

        let query = query_builder.build();
        query.execute(&mut **transaction).await?;
        Ok(())
    }
}
