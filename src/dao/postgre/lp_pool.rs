use sqlx::Error;

use crate::model::{LP_Pool, Table};

impl Table<LP_Pool> {
    pub async fn insert(&self, data: LP_Pool) -> Result<(), Error> {
        const SQL: &str = r#"
        INSERT INTO "LP_Pool" (
            "LP_Pool_id",
            "LP_symbol"
        )
        VALUES ($1, $2)
        ON CONFLICT DO NOTHING
        "#;

        sqlx::query(SQL)
            .bind(&data.LP_Pool_id)
            .bind(&data.LP_symbol)
            .execute(&self.pool)
            .await
            .map(drop)
    }

    pub async fn get_all(&self) -> Result<Vec<LP_Pool>, Error> {
        const SQL: &str = r#"
        SELECT *
        FROM "LP_Pool"
        "#;

        sqlx::query_as(SQL).fetch_all(&self.pool).await
    }
}
