use super::QueryResult;
use crate::model::{MP_Asset_State, Table};
use sqlx::error::Error;

impl Table<MP_Asset_State> {
    pub async fn insert(&self, data: MP_Asset_State) -> Result<QueryResult, Error> {
        sqlx::query(
            r#"
            INSERT INTO `MP_Asset_State` (
                `MP_asset_symbol`,
                `MP_timestamp`,
                `MP_price_open`,
                `MP_price_high`,
                `MP_price_low`,
                `MP_price_close`,
                `MP_volume`,
                `MP_marketcap`
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(&data.MP_asset_symbol)
        .bind(&data.MP_timestamp)
        .bind(&data.MP_price_open)
        .bind(&data.MP_price_high)
        .bind(&data.MP_price_low)
        .bind(&data.MP_price_close)
        .bind(&data.MP_volume)
        .bind(&data.MP_marketcap)
        .execute(&self.pool)
        .await
    }
}
