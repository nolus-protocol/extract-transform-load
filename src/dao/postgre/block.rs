use sqlx::{Error, Transaction};

use crate::model::{Block, Table};

use super::{DataBase, QueryResult};

impl Table<Block> {
    pub async fn insert(
        &self,
        block: Block,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<QueryResult, Error> {
        sqlx::query(
            r#"
            INSERT INTO block (id)
            VALUES($1)
            "#,
        )
        .bind(block.id)
        .persistent(true)
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
        .persistent(true)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn get_first_block(&self) -> Result<(i64,), Error> {
        sqlx::query_as(
            r#"
            SELECT id FROM block ORDER BY id ASC
            "#,
        )
        .persistent(true)
        .fetch_one(&self.pool)
        .await
    }

    pub async fn get_last_block(&self) -> Result<(i64,), Error> {
        sqlx::query_as(
            r#"
            SELECT id FROM block ORDER BY id DESC
            "#,
        )
        .persistent(true)
        .fetch_one(&self.pool)
        .await
    }

    pub async fn get_one(&self, id: i64) -> Result<Option<Block>, Error> {
        sqlx::query_as(
            r#"
             SELECT * FROM "block" WHERE id = $1
            "#,
        )
        .bind(id)
        .persistent(true)
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn count(&self) -> Result<i64, Error> {
        let (count,) = sqlx::query_as(
            r#"
             SELECT COUNT(1) FROM "block"
            "#,
        )
        .persistent(true)
        .fetch_one(&self.pool)
        .await?;
        Ok(count)
    }

    pub async fn is_synced_to_block(&self, block: i64) -> Result<bool, Error> {
        let (count,): (i64,) = sqlx::query_as(
            r#"
             SELECT COUNT(1) FROM "block" WHERE id <= $1
            "#,
        )
        .bind(block)
        .persistent(true)
        .fetch_one(&self.pool)
        .await?;

        if block == count {
            return Ok(true);
        }

        Ok(false)
    }
}
