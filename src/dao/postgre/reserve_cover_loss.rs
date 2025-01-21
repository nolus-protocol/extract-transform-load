use sqlx::{error::Error, Transaction};

use crate::model::{Reserve_Cover_Loss, Table};

use super::DataBase;

impl Table<Reserve_Cover_Loss> {
    pub async fn isExists(
        &self,
        &Reserve_Cover_Loss {
            ref LS_contract_id,
            ref Tx_Hash,
            Event_Block_Index,
            ..
        }: &Reserve_Cover_Loss,
    ) -> Result<bool, Error> {
        const SQL: &str = r#"
        SELECT COUNT(1) > 0
        FROM "Reserve_Cover_Loss"
        WHERE
            "LS_contract_id" = $1 AND
            "Tx_Hash" = $2 AND
            "Event_Block_Index" = $3
        "#;

        sqlx::query_as(SQL)
            .bind(LS_contract_id)
            .bind(Tx_Hash)
            .bind(Event_Block_Index)
            .fetch_one(&self.pool)
            .await
            .map(|(result,)| result)
    }

    pub async fn insert(
        &self,
        &Reserve_Cover_Loss {
            ref LS_contract_id,
            ref Tx_Hash,
            ref LS_symbol,
            ref LS_amnt,
            LS_timestamp,
            Event_Block_Index,
        }: &Reserve_Cover_Loss,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<(), Error> {
        const SQL: &str = r#"
        INSERT INTO "Reserve_Cover_Loss" (
            "LS_contract_id",
            "Tx_Hash",
            "LS_symbol",
            "LS_amnt",
            "LS_timestamp",
            "Event_Block_Index"
        )
        VALUES ($1, $2, $3, $4, $5, $6)
        "#;

        sqlx::query(SQL)
            .bind(LS_contract_id)
            .bind(Tx_Hash)
            .bind(LS_symbol)
            .bind(LS_amnt)
            .bind(LS_timestamp)
            .bind(Event_Block_Index)
            .execute(&mut **transaction)
            .await
            .map(drop)
    }
}
