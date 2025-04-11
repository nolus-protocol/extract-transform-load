use super::QueryResult;
use crate::model::{MP_Yield, Table};
use sqlx::error::Error;

impl Table<MP_Yield> {
    pub async fn insert(&self, data: MP_Yield) -> Result<QueryResult, Error> {
        sqlx::query(
            r#"
            INSERT INTO "MP_Yield" (
                "MP_yield_symbol",
                "MP_yield_timestamp",
                "MP_apy_permilles"
            )
            VALUES($1, $2, $3)
        "#,
        )
        .bind(&data.MP_yield_symbol)
        .bind(data.MP_yield_timestamp)
        .bind(data.MP_apy_permilles)
        .persistent(false)
        .execute(&self.pool)
        .await
    }
}
