use sqlx::{Error, Transaction};

use crate::model::{LS_Auto_Close_Position, Table};

use super::{DataBase, QueryResult};

impl Table<LS_Auto_Close_Position> {
    pub async fn insert_if_not_exists(
        &self,
        data: LS_Auto_Close_Position,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<QueryResult, Error> {
        sqlx::query(
            r#"
            INSERT INTO "LS_Auto_Close_Position" (
                "Tx_Hash",
                "LS_contract_id",
                "LS_Close_Strategy",
                "LS_Close_Strategy_Ltv",
                "LS_timestamp"
            )
            VALUES($1, $2, $3, $4, $5)
            ON CONFLICT ("Tx_Hash", "LS_contract_id", "LS_timestamp") DO NOTHING
        "#,
        )
        .bind(&data.Tx_Hash)
        .bind(&data.LS_contract_id)
        .bind(&data.LS_Close_Strategy)
        .bind(data.LS_Close_Strategy_Ltv)
        .bind(data.LS_timestamp)
        .persistent(true)
        .execute(&mut **transaction)
        .await
    }
}
