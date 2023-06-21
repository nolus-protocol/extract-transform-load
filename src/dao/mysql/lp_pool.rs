use super::QueryResult;
use crate::model::{LP_Pool, Table};
use sqlx::error::Error;

impl Table<LP_Pool> {
    pub async fn insert(&self, data: LP_Pool) -> Result<QueryResult, Error> {
        sqlx::query(
            r#"
            INSERT INTO `LP_Pool` (`LP_Pool_id`, `LP_symbol`)
            SELECT * FROM (SELECT ? as `LP_Pool_id`, ? as `LP_symbol`) AS tmp
            WHERE NOT EXISTS (
                SELECT `LP_Pool_id` FROM `LP_Pool` WHERE `LP_Pool_id` = ?
            ) LIMIT 1
        "#,
        )
        .bind(&data.LP_Pool_id)
        .bind(&data.LP_symbol)
        .bind(&data.LP_Pool_id)
        .execute(&self.pool)
        .await
    }

    pub async fn get_all(&self) -> Result<Vec<LP_Pool>, Error> {
        sqlx::query_as(r#"SELECT * FROM `LP_Pool`"#)
            .fetch_all(&self.pool)
            .await
    }
}
