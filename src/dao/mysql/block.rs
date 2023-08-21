use super::{DataBase, QueryResult};
use crate::model::{Block, Table};
use sqlx::{error::Error, Transaction};

impl Table<Block> {
    pub async fn insert(
        &self,
        block: Block,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<QueryResult, Error> {
        sqlx::query(
            r#"
            INSERT INTO block (id)
            VALUES(?)
            "#,
        )
        .bind(block.id)
        .execute(&mut **transaction)
        .await
    }
    pub async fn get_missing_blocks(&self) -> Result<Vec<(i64, i64)>, Error> {
        sqlx::query_as(
            r#"
            WITH gaps AS
            (
                SELECT
                    LAG(id, 1, 0) OVER(ORDER BY id) AS gap_begin,
                    id AS gap_end,
                    id - LAG(id, 1, 0) OVER(ORDER BY id) AS gap
                FROM block
            )
            SELECT
                gap_begin,
                gap_end
            FROM gaps
            WHERE gap > 1
            "#,
        )
        .fetch_all(&self.pool)
        .await
    }

    pub async fn get_first_block(&self) -> Result<(i64,), Error> {
        sqlx::query_as(
            r#"
            SELECT id FROM block ORDER BY id ASC
            "#,
        )
        .fetch_one(&self.pool)
        .await
    }

    pub async fn get_last_block(&self) -> Result<(i64,), Error> {
        sqlx::query_as(
            r#"
            SELECT id FROM block ORDER BY id DESC
            "#,
        )
        .fetch_one(&self.pool)
        .await
    }

    pub async fn get_one(
        &self,
        id: i64
    ) -> Result<Option<Block>, Error> {
        sqlx::query_as(
            r#"
             SELECT * FROM "block" WHERE id = ?
            "#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
    }
}
