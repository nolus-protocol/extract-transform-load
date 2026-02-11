use sqlx::{Error, Transaction};

use crate::model::{Reserve_Cover_Loss, Table};

use super::{DataBase, QueryResult};

impl Table<Reserve_Cover_Loss> {
    pub async fn insert_if_not_exists(
        &self,
        data: Reserve_Cover_Loss,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<QueryResult, Error> {
        sqlx::query(
            r#"
            INSERT INTO "Reserve_Cover_Loss" (
                "Tx_Hash",
                "LS_contract_id",
                "LS_symbol",
                "LS_amnt",
                "Event_Block_Index",
                "LS_timestamp"
            )
            VALUES($1, $2, $3, $4, $5, $6)
            ON CONFLICT ("LS_contract_id", "Event_Block_Index", "Tx_Hash") DO NOTHING
        "#,
        )
        .bind(&data.Tx_Hash)
        .bind(&data.LS_contract_id)
        .bind(&data.LS_symbol)
        .bind(&data.LS_amnt)
        .bind(data.Event_Block_Index)
        .bind(data.LS_timestamp)
        .persistent(true)
        .execute(&mut **transaction)
        .await
    }
}
