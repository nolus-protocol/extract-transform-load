use sqlx::{Error, Transaction};

use crate::model::{LS_Auto_Close_Position, Table};

use super::{DataBase, QueryResult};

impl Table<LS_Auto_Close_Position> {
    pub async fn isExists(
        &self,
        ls_auto_close_position: &LS_Auto_Close_Position,
    ) -> Result<bool, crate::error::Error> {
        let (value,): (i64,) = sqlx::query_as(
            r#"
            SELECT
                COUNT(*)
            FROM "LS_Auto_Close_Position"
            WHERE
                "Tx_Hash" = $1 AND
                "LS_contract_id" = $2 AND
                "LS_timestamp" = $3
            "#,
        )
        .bind(&ls_auto_close_position.Tx_Hash)
        .bind(&ls_auto_close_position.LS_contract_id)
        .bind(ls_auto_close_position.LS_timestamp)
        .fetch_one(&self.pool)
        .await?;

        if value > 0 {
            return Ok(true);
        }

        Ok(false)
    }

    pub async fn insert(
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
        "#,
        )
        .bind(&data.Tx_Hash)
        .bind(&data.LS_contract_id)
        .bind(&data.LS_Close_Strategy)
        .bind(data.LS_Close_Strategy_Ltv)
        .bind(data.LS_timestamp)
        .execute(&mut **transaction)
        .await
    }

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
