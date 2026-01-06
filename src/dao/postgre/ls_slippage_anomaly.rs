use sqlx::{Error, Transaction};

use crate::model::{LS_Slippage_Anomaly, Table};

use super::{DataBase, QueryResult};

impl Table<LS_Slippage_Anomaly> {
    pub async fn isExists(
        &self,
        ls_slippage_anomaly: &LS_Slippage_Anomaly,
    ) -> Result<bool, crate::error::Error> {
        let (value,): (i64,) = sqlx::query_as(
            r#"
            SELECT
                COUNT(*)
            FROM "LS_Slippage_Anomaly"
            WHERE
                "Tx_Hash" = $1 AND
                "LS_contract_id" = $2 AND
                "LS_timestamp" = $3
            "#,
        )
        .bind(&ls_slippage_anomaly.Tx_Hash)
        .bind(&ls_slippage_anomaly.LS_contract_id)
        .bind(&ls_slippage_anomaly.LS_timestamp)
        .persistent(true)
        .fetch_one(&self.pool)
        .await?;

        if value > 0 {
            return Ok(true);
        }

        Ok(false)
    }

    pub async fn insert(
        &self,
        data: LS_Slippage_Anomaly,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<QueryResult, Error> {
        sqlx::query(
            r#"
            INSERT INTO "LS_Slippage_Anomaly" (
                "Tx_Hash",
                "LS_contract_id",
                "LS_address_id",
                "LS_asset_symbol",
                "LS_max_slipagge",
                "LS_timestamp"
            )
            VALUES($1, $2, $3, $4, $5, $6)
        "#,
        )
        .bind(&data.Tx_Hash)
        .bind(&data.LS_contract_id)
        .bind(&data.LS_address_id)
        .bind(&data.LS_asset_symbol)
        .bind(&data.LS_max_slipagge)
        .bind(&data.LS_timestamp)
        .persistent(true)
        .execute(&mut **transaction)
        .await
    }

    pub async fn insert_if_not_exists(
        &self,
        data: LS_Slippage_Anomaly,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<QueryResult, Error> {
        sqlx::query(
            r#"
            INSERT INTO "LS_Slippage_Anomaly" (
                "Tx_Hash",
                "LS_contract_id",
                "LS_address_id",
                "LS_asset_symbol",
                "LS_max_slipagge",
                "LS_timestamp"
            )
            VALUES($1, $2, $3, $4, $5, $6)
            ON CONFLICT ("Tx_Hash", "LS_contract_id", "LS_timestamp") DO NOTHING
        "#,
        )
        .bind(&data.Tx_Hash)
        .bind(&data.LS_contract_id)
        .bind(&data.LS_address_id)
        .bind(&data.LS_asset_symbol)
        .bind(&data.LS_max_slipagge)
        .bind(&data.LS_timestamp)
        .persistent(true)
        .execute(&mut **transaction)
        .await
    }
}
