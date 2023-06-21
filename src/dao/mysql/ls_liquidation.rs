use super::{DataBase, QueryResult};
use crate::model::{LS_Liquidation, Table};
use sqlx::{error::Error, QueryBuilder, Transaction};

impl Table<LS_Liquidation> {
    pub async fn insert(
        &self,
        data: LS_Liquidation,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<QueryResult, Error> {
        sqlx::query(
            r#"
            INSERT INTO `LS_Liquidation` (
                `LS_liquidation_height`,
                `LS_contract_id`,
                `LS_symbol`,
                `LS_timestamp`,
                `LS_amnt_stable`,
                `LS_transaction_type`,
                `LS_prev_margin_stable`,
                `LS_prev_interest_stable`,
                `LS_current_margin_stable`,
                `LS_current_interest_stable`,
                `LS_principal_stable`
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#,
        )
        .bind(&data.LS_liquidation_height)
        .bind(&data.LS_contract_id)
        .bind(&data.LS_symbol)
        .bind(&data.LS_timestamp)
        .bind(&data.LS_amnt_stable)
        .bind(&data.LS_transaction_type)
        .bind(&data.LS_prev_margin_stable)
        .bind(&data.LS_prev_interest_stable)
        .bind(&data.LS_current_margin_stable)
        .bind(&data.LS_current_interest_stable)
        .bind(&data.LS_principal_stable)
        .execute(transaction)
        .await
    }

    pub async fn insert_many(
        &self,
        data: &Vec<LS_Liquidation>,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<(), Error> {
        if data.len() == 0 {
            return Ok(());
        }

        let mut query_builder: QueryBuilder<DataBase> = QueryBuilder::new(
            r#"
            INSERT INTO `LS_Liquidation` (
                `LS_liquidation_height`,
                `LS_contract_id`,
                `LS_symbol`,
                `LS_timestamp`,
                `LS_amnt_stable`,
                `LS_transaction_type`,
                `LS_prev_margin_stable`,
                `LS_prev_interest_stable`,
                `LS_current_margin_stable`,
                `LS_current_interest_stable`,
                `LS_principal_stable`
            )"#,
        );

        query_builder.push_values(data, |mut b, ls| {
            b.push_bind(&ls.LS_liquidation_height)
                .push_bind(&ls.LS_contract_id)
                .push_bind(&ls.LS_symbol)
                .push_bind(&ls.LS_timestamp)
                .push_bind(&ls.LS_amnt_stable)
                .push_bind(&ls.LS_transaction_type)
                .push_bind(&ls.LS_prev_margin_stable)
                .push_bind(&ls.LS_prev_interest_stable)
                .push_bind(&ls.LS_current_margin_stable)
                .push_bind(&ls.LS_current_interest_stable)
                .push_bind(&ls.LS_principal_stable);
        });

        let query = query_builder.build();
        query.execute(transaction).await?;
        Ok(())
    }
}
