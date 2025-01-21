use sqlx::error::Error;

use crate::model::{MP_Yield, Table};

impl Table<MP_Yield> {
    pub async fn insert(
        &self,
        &MP_Yield {
            ref MP_yield_symbol,
            MP_yield_timestamp,
            MP_apy_permilles,
        }: &MP_Yield,
    ) -> Result<(), Error> {
        const SQL: &str = r#"
        INSERT INTO "MP_Yield" (
            "MP_yield_symbol",
            "MP_yield_timestamp",
            "MP_apy_permilles"
        )
        VALUES ($1, $2, $3)
        "#;

        sqlx::query(SQL)
            .bind(MP_yield_symbol)
            .bind(MP_yield_timestamp)
            .bind(MP_apy_permilles)
            .execute(&self.pool)
            .await
            .map(drop)
    }
}
