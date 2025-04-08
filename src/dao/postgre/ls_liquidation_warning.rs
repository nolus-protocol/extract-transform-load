use sqlx::{Error, Transaction};

use crate::model::{LS_Liquidation_Warning, Table};

use super::DataBase;

impl Table<LS_Liquidation_Warning> {
    pub async fn isExists(
        &self,
        ls_liquidatiion_warning: &LS_Liquidation_Warning,
    ) -> Result<bool, Error> {
        const SQL: &str = r#"
        SELECT EXISTS(
            SELECT
            FROM "LS_Liquidation_Warning"
            WHERE
                "Tx_Hash" = $1 AND
                "LS_contract_id" = $2 AND
                "LS_timestamp" = $3
        )
        "#;

        sqlx::query_as(SQL)
            .bind(&ls_liquidatiion_warning.Tx_Hash)
            .bind(&ls_liquidatiion_warning.LS_contract_id)
            .bind(ls_liquidatiion_warning.LS_timestamp)
            .fetch_one(&self.pool)
            .await
            .map(|(result,)| result)
    }

    pub async fn insert(
        &self,
        data: LS_Liquidation_Warning,
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
            .bind(&data.Tx_Hash)
            .bind(&data.LS_contract_id)
            .bind(&data.LS_address_id)
            .bind(&data.LS_asset_symbol)
            .bind(data.LS_level)
            .bind(data.LS_ltv)
            .bind(data.LS_timestamp)
            .execute(transaction.as_mut())
            .await
            .map(drop)
    }
}
