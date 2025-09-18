use super::{DataBase, QueryResult};
use crate::model::{LS_Loan_Collect, Table};
use sqlx::{Error, QueryBuilder, Transaction};

impl Table<LS_Loan_Collect> {
    pub async fn isExists(
        &self,
        contract: String,
        symbol: String,
    ) -> Result<bool, crate::error::Error> {
        let (value,): (i64,) = sqlx::query_as(
            r#"
            SELECT 
                COUNT(*)
            FROM "LS_Loan_Collect" 
            WHERE 
                "LS_contract_id" = $1 AND
                "LS_symbol" = $2
            "#,
        )
        .bind(contract)
        .bind(symbol)
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
        data: LS_Loan_Collect,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<QueryResult, Error> {
        sqlx::query(
            r#"
            INSERT INTO "LS_Loan_Collect" (
                "LS_contract_id",
                "LS_symbol",
                "LS_amount"
            )
            VALUES($1, $2, $3)
        "#,
        )
        .bind(&data.LS_contract_id)
        .bind(&data.LS_symbol)
        .bind(&data.LS_amount)
        .persistent(false)
        .execute(&mut **transaction)
        .await
    }

    pub async fn insert_many_transaction(
        &self,
        data: &Vec<LS_Loan_Collect>,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<(), Error> {
        if data.is_empty() {
            return Ok(());
        }

        let mut query_builder: QueryBuilder<DataBase> = QueryBuilder::new(
            r#"
            INSERT INTO "LS_Loan_Collect" (
                "LS_contract_id",
                "LS_symbol",
                "LS_amount"
            )"#,
        );

        query_builder.push_values(data, |mut b, data| {
            b.push_bind(&data.LS_contract_id)
                .push_bind(&data.LS_symbol)
                .push_bind(&data.LS_amount);
        });

        let query = query_builder.build().persistent(false);
        query.execute(&mut **transaction).await?;
        Ok(())
    }

    pub async fn insert_many(
        &self,
        data: &Vec<LS_Loan_Collect>,
    ) -> Result<(), Error> {
        if data.is_empty() {
            return Ok(());
        }

        let mut query_builder: QueryBuilder<DataBase> = QueryBuilder::new(
            r#"
            INSERT INTO "LS_Loan_Collect" (
                "LS_contract_id",
                "LS_symbol",
                "LS_amount"
            )"#,
        );

        query_builder.push_values(data, |mut b, data| {
            b.push_bind(&data.LS_contract_id)
                .push_bind(&data.LS_symbol)
                .push_bind(&data.LS_amount);
        });

        let query = query_builder.build().persistent(false);
        query.execute(&self.pool).await?;
        Ok(())
    }
}
