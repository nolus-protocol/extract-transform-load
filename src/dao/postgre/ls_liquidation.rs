use super::{DataBase, QueryResult};
use crate::model::{LS_Liquidation, Table};
use sqlx::{error::Error, QueryBuilder, Transaction};

impl Table<LS_Liquidation> {
    pub async fn isExists(
        &self,
        ls_liquidatiion: &LS_Liquidation,
    ) -> Result<bool, crate::error::Error> {
        let (value,): (i64,) = sqlx::query_as(
            r#"
            SELECT 
                COUNT(*)
            FROM "LS_Liquidation" 
            WHERE 
                "LS_liquidation_height" = $1 AND
                "LS_contract_id" = $2
            "#,
        )
        .bind(ls_liquidatiion.LS_liquidation_height)
        .bind(&ls_liquidatiion.LS_contract_id)
        .fetch_one(&self.pool)
        .await?;

        if value > 0 {
            return Ok(true);
        }

        Ok(false)
    }

    pub async fn insert(
        &self,
        data: &LS_Liquidation,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<QueryResult, Error> {
        sqlx::query(
            r#"
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
            VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
        "#,
        )
        .bind(&data.LS_liquidation_height)
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
        .bind(&data.LS_loan_close)
        .execute(&mut **transaction)
        .await
    }

    pub async fn insert_many(
        &self,
        data: &Vec<LS_Liquidation>,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<(), Error> {
        if data.is_empty() {
            return Ok(());
        }

        let mut query_builder: QueryBuilder<DataBase> = QueryBuilder::new(
            r#"
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
            )"#,
        );

        query_builder.push_values(data, |mut b, ls| {
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
                .push_bind(&ls.LS_loan_close);
        });

        let query = query_builder.build().persistent(false);
        query.execute(&mut **transaction).await?;
        Ok(())
    }

    pub async fn get_by_contract(
        &self,
        contract: String,
    ) -> Result<Vec<LS_Liquidation>, Error> {
        let data = sqlx::query_as(
            r#"
            SELECT * FROM "LS_Liquidation" WHERE "LS_contract_id" = $1;
            "#,
        )
        .bind(&contract)
        .fetch_all(&self.pool)
        .await?;
        Ok(data)
    }
}
