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

    /// Full gap scan - scans entire block table for gaps.
    /// Use on startup to catch any historical gaps.
    /// Uses window function LEAD() to efficiently find gaps without correlated subqueries.
    pub async fn get_all_missing_blocks(&self) -> Result<Vec<(i64, i64)>, Error> {
        sqlx::query_as(
            r#"
            WITH with_next AS (
                SELECT id, LEAD(id) OVER (ORDER BY id) AS next_id
                FROM block
            )
            SELECT id AS gap_begin, next_id AS gap_end
            FROM with_next
            WHERE next_id IS NOT NULL AND next_id > id + 1
            "#,
        )
        .persistent(true)
        .fetch_all(&self.pool)
        .await
    }

    /// Recent gap scan - only checks last 100k blocks for gaps.
    /// Use during runtime for fast gap detection.
    /// Uses window function LEAD() to efficiently find gaps without correlated subqueries.
    pub async fn get_recent_missing_blocks(&self) -> Result<Vec<(i64, i64)>, Error> {
        sqlx::query_as(
            r#"
            WITH recent_blocks AS (
                SELECT id
                FROM block
                WHERE id > (SELECT MAX(id) - 100000 FROM block)
                ORDER BY id
            ),
            with_next AS (
                SELECT id, LEAD(id) OVER (ORDER BY id) AS next_id
                FROM recent_blocks
            )
            SELECT id AS gap_begin, next_id AS gap_end
            FROM with_next
            WHERE next_id IS NOT NULL AND next_id > id + 1
            "#,
        )
        .persistent(true)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn get_first_block(&self) -> Result<(i64,), Error> {
        sqlx::query_as(
            r#"
            SELECT id FROM block ORDER BY id ASC LIMIT 1
            "#,
        )
        .persistent(true)
        .fetch_one(&self.pool)
        .await
    }

    pub async fn get_last_block(&self) -> Result<(i64,), Error> {
        sqlx::query_as(
            r#"
            SELECT id FROM block ORDER BY id DESC LIMIT 1
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
