use super::{DataBase, QueryResult};
use crate::model::{LS_Opening, LS_State, Table};
use chrono::{DateTime, Utc};
use sqlx::{error::Error, QueryBuilder};

impl Table<LS_State> {
    pub async fn insert(&self, data: LS_State) -> Result<QueryResult, Error> {
        sqlx::query(
            r#"
            INSERT INTO `LS_State` (
                `LS_contract_id`,
                `LS_timestamp`,
                `LS_amnt_stable`,
                `LS_prev_margin_stable`,
                `LS_prev_interest_stable`,
                `LS_current_margin_stable`,
                `LS_current_interest_stable`,
                `LS_principal_stable`
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(&data.LS_contract_id)
        .bind(&data.LS_timestamp)
        .bind(&data.LS_amnt_stable)
        .bind(&data.LS_prev_margin_stable)
        .bind(&data.LS_prev_interest_stable)
        .bind(&data.LS_current_margin_stable)
        .bind(&data.LS_current_interest_stable)
        .bind(&data.LS_principal_stable)
        .execute(&self.pool)
        .await
    }

    pub async fn get_active_states(&self) -> Result<Vec<LS_Opening>, Error> {
        sqlx::query_as(
            r#"
                SELECT 
                    a.`LS_contract_id`,
                    a.`LS_address_id`,
                    a.`LS_asset_symbol`,
                    a.`LS_interest`,
                    a.`LS_timestamp`,
                    a.`LS_loan_pool_id`,
                    a.`LS_loan_amnt_stable`,
                    a.`LS_loan_amnt_asset`,
                    a.`LS_cltr_symbol`,
                    a.`LS_cltr_amnt_stable`,
                    a.`LS_cltr_amnt_asset`,
                    a.`LS_native_amnt_stable`,
                    a.`LS_native_amnt_nolus`
                FROM `LS_Opening` as a 
                LEFT JOIN `LS_Closing` as b 
                ON a.`LS_contract_id` = b.`LS_contract_id` 
                WHERE b.`LS_contract_id` IS NULL
            "#,
        )
        .fetch_all(&self.pool)
        .await
    }

    pub async fn insert_many(&self, data: &Vec<LS_State>) -> Result<(), Error> {
        if data.len() == 0 {
            return Ok(());
        }

        let mut query_builder: QueryBuilder<DataBase> = QueryBuilder::new(
            r#"
            INSERT INTO `LS_State` (
                `LS_contract_id`,
                `LS_timestamp`,
                `LS_amnt_stable`,
                `LS_prev_margin_stable`,
                `LS_prev_interest_stable`,
                `LS_current_margin_stable`,
                `LS_current_interest_stable`,
                `LS_principal_stable`
            )"#,
        );

        query_builder.push_values(data, |mut b, data| {
            b.push_bind(&data.LS_contract_id)
                .push_bind(&data.LS_timestamp)
                .push_bind(&data.LS_amnt_stable)
                .push_bind(&data.LS_prev_margin_stable)
                .push_bind(&data.LS_prev_interest_stable)
                .push_bind(&data.LS_current_margin_stable)
                .push_bind(&data.LS_current_interest_stable)
                .push_bind(&data.LS_principal_stable);
        });

        let query = query_builder.build();
        query.execute(&self.pool).await?;
        Ok(())
    }

    pub async fn count(&self, timestamp: DateTime<Utc>) -> Result<i64, crate::error::Error> {
        let (value,): (i64,) = sqlx::query_as(
            r#"
            SELECT 
                COUNT(*)
            FROM `LS_State` WHERE `LS_timestamp` = ?
            "#,
        )
        .bind(timestamp)
        .fetch_one(&self.pool)
        .await?;
        Ok(value)
    }
}
