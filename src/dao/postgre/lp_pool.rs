use sqlx::Error;

use crate::model::{LP_Pool, Table};

use super::QueryResult;

impl Table<LP_Pool> {
    pub async fn insert(&self, data: LP_Pool) -> Result<QueryResult, Error> {
        sqlx::query(
            r#"
            INSERT INTO "LP_Pool" ("LP_Pool_id", "LP_symbol", "LP_status")
            VALUES($1, $2, $3)
            ON CONFLICT ("LP_Pool_id") DO NOTHING
        "#,
        )
        .bind(&data.LP_Pool_id)
        .bind(&data.LP_symbol)
        .bind(data.LP_status)
        .persistent(true)
        .execute(&self.pool)
        .await
    }

    pub async fn get_all(&self) -> Result<Vec<LP_Pool>, Error> {
        sqlx::query_as(r#"SELECT * FROM "LP_Pool""#)
            .persistent(true)
            .fetch_all(&self.pool)
            .await
    }
}
