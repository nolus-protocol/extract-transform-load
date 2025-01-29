use sqlx::{Error, Transaction};

use crate::model::{Reserve_Cover_Loss, Table};

use super::DataBase;

impl Table<Reserve_Cover_Loss> {
    pub async fn isExists(
        &self,
        reserve_cover_loss: &Reserve_Cover_Loss,
    ) -> Result<bool, Error> {
        const SQL: &str = r#"
        SELECT EXISTS(
            SELECT
            FROM "Reserve_Cover_Loss"
            WHERE
                "LS_contract_id" = $1 AND
                "Event_Block_Index" = $2 AND
                "Tx_Hash" = $3
        )
        "#;

        sqlx::query_as(SQL)
            .bind(&reserve_cover_loss.LS_contract_id)
            .bind(reserve_cover_loss.Event_Block_Index)
            .bind(&reserve_cover_loss.Tx_Hash)
            .fetch_one(&self.pool)
            .await
            .map(|(result,)| result)
    }

    pub async fn insert(
        &self,
        data: Reserve_Cover_Loss,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<(), Error> {
        const SQL: &str = r#"
        INSERT INTO "Reserve_Cover_Loss" (
            "Tx_Hash",
            "LS_contract_id",
            "LS_symbol",
            "LS_amnt",
            "Event_Block_Index",
            "LS_timestamp"
        )
        VALUES ($1, $2, $3, $4, $5, $6)
        "#;

        sqlx::query(SQL)
            .bind(&data.Tx_Hash)
            .bind(&data.LS_contract_id)
            .bind(&data.LS_symbol)
            .bind(&data.LS_amnt)
            .bind(data.Event_Block_Index)
            .bind(data.LS_timestamp)
            .execute(&mut **transaction)
            .await
            .map(drop)
    }
}
