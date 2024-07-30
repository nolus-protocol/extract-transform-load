use super::QueryResult;
use crate::model::{MP_Asset_Mapping, Table};
use sqlx::error::Error;

impl Table<MP_Asset_Mapping> {
    pub async fn insert(
        &self,
        data: MP_Asset_Mapping,
    ) -> Result<QueryResult, Error> {
        sqlx::query(
            r#"
            INSERT INTO "MP_Asset_Mapping" ("MP_asset_symbol", "MP_asset_symbol_coingecko")
            VALUES($1, $2)
            "#,
        )
        .bind(&data.MP_asset_symbol)
        .bind(&data.MP_asset_symbol_coingecko)
        .execute(&self.pool)
        .await
    }

    pub async fn get_one(
        &self,
        asset_symbol: String,
    ) -> Result<Option<MP_Asset_Mapping>, Error> {
        sqlx::query_as(
            r#"
             SELECT * FROM "MP_Asset_Mapping" WHERE "MP_asset_symbol" = $1 LIMIT 1
            "#,
        )
        .bind(asset_symbol)
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn get_all(&self) -> Result<Vec<MP_Asset_Mapping>, Error> {
        sqlx::query_as(
            r#"
             SELECT * FROM "MP_Asset_Mapping"
            "#,
        )
        .fetch_all(&self.pool)
        .await
    }
}
