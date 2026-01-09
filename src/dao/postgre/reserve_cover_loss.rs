use sqlx::{Error, Transaction};

use crate::model::{Reserve_Cover_Loss, Table};

use super::{DataBase, QueryResult};

impl Table<Reserve_Cover_Loss> {
    pub async fn isExists(
        &self,
        reserve_cover_loss: &Reserve_Cover_Loss,
    ) -> Result<bool, crate::error::Error> {
        let (value,): (i64,) = sqlx::query_as(
            r#"
            SELECT
                COUNT(*)
            FROM "Reserve_Cover_Loss"
            WHERE
                "LS_contract_id" = $1 AND
                "Event_Block_Index" = $2 AND
                "Tx_Hash" = $3
            "#,
        )
        .bind(&reserve_cover_loss.LS_contract_id)
        .bind(reserve_cover_loss.Event_Block_Index)
        .bind(&reserve_cover_loss.Tx_Hash)
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
        "#,
        )
        .bind(&data.Tx_Hash)
        .bind(&data.LS_contract_id)
        .bind(&data.LS_symbol)
        .bind(&data.LS_amnt)
        .bind(data.Event_Block_Index)
        .bind(data.LS_timestamp)
        .persistent(false)
        .execute(&mut **transaction)
        .await
    }
}
