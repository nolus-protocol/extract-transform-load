use sqlx::Error;

use crate::model::{MP_Yield, Table};

impl Table<MP_Yield> {
    pub async fn insert(&self, data: MP_Yield) -> Result<(), Error> {
        const SQL: &str = r#"
        INSERT INTO "MP_Yield" (
            "MP_yield_symbol",
            "MP_yield_timestamp",
            "MP_apy_permilles"
        )
        VALUES ($1, $2, $3)
        "#;

        sqlx::query(SQL)
            .bind(&data.MP_yield_symbol)
            .bind(data.MP_yield_timestamp)
            .bind(data.MP_apy_permilles)
            .execute(&self.pool)
            .await
            .map(drop)
    }
}
