use sqlx::Error;

use crate::model::{LP_Pool, Table};

use super::QueryResult;

impl Table<LP_Pool> {
    pub async fn insert(&self, data: LP_Pool) -> Result<QueryResult, Error> {
        sqlx::query(
            r#"
            INSERT INTO "LP_Pool" ("LP_Pool_id", "LP_symbol")
            SELECT * FROM (SELECT $1 as "LP_Pool_id", $2 as "LP_symbol") AS tmp
            WHERE NOT EXISTS (
                SELECT "LP_Pool_id" FROM "LP_Pool" WHERE "LP_Pool_id" = $3
            ) LIMIT 1
        "#,
        )
        .bind(&data.LP_Pool_id)
        .bind(&data.LP_symbol)
        .bind(&data.LP_Pool_id)
        .persistent(false)
        .execute(&self.pool)
        .await
    }

    pub async fn get_all(&self) -> Result<Vec<LP_Pool>, Error> {
        sqlx::query_as(r#"SELECT * FROM "LP_Pool""#)
            .persistent(false)
            .fetch_all(&self.pool)
            .await
    }
}
