use sqlx::{error::Error, Transaction};

use crate::model::{LS_Liquidation_Warning, Table};

use super::DataBase;

impl Table<LS_Liquidation_Warning> {
    pub async fn isExists(
        &self,
        &LS_Liquidation_Warning {
            ref Tx_Hash,
            ref LS_contract_id,
            LS_timestamp,
            ..
        }: &LS_Liquidation_Warning,
    ) -> Result<bool, Error> {
        const SQL: &str = r#"
        SELECT COUNT(1) > 0
        FROM "LS_Liquidation_Warning"
        WHERE
            "Tx_Hash" = $1 AND
            "LS_contract_id" = $2 AND
            "LS_timestamp" = $3
        "#;

        sqlx::query_as(SQL)
            .bind(Tx_Hash.as_deref())
            .bind(LS_contract_id)
            .bind(LS_timestamp)
            .fetch_one(&self.pool)
            .await
            .map(|(result,)| result)
    }

    pub async fn insert(
        &self,
        &LS_Liquidation_Warning {
            ref Tx_Hash,
            ref LS_contract_id,
            ref LS_address_id,
            ref LS_asset_symbol,
            LS_level,
            LS_ltv,
            LS_timestamp,
        }: &LS_Liquidation_Warning,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<(), Error> {
        const SQL: &str = r#"
        INSERT INTO "LS_Liquidation_Warning" (
            "Tx_Hash",
            "LS_contract_id",
            "LS_address_id",
            "LS_asset_symbol",
            "LS_level",
            "LS_ltv",
            "LS_timestamp"
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        "#;

        sqlx::query(SQL)
            .bind(Tx_Hash)
            .bind(LS_contract_id)
            .bind(LS_address_id)
            .bind(LS_asset_symbol)
            .bind(LS_level)
            .bind(LS_ltv)
            .bind(LS_timestamp)
            .execute(&mut **transaction)
            .await
            .map(drop)
    }
}
