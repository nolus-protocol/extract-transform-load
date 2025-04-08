use sqlx::{Error, Transaction};

use crate::model::{Block, Table};

use super::DataBase;

impl Table<Block> {
    pub async fn insert(
        &self,
        block: Block,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<(), Error> {
        const SQL: &str = r#"
        INSERT INTO "block" ("id")
        VALUES ($1)
        "#;

        sqlx::query(SQL)
            .bind(block.id)
            .execute(transaction.as_mut())
            .await
            .map(drop)
    }

    pub async fn get_missing_blocks(&self) -> Result<Vec<(i64, i64)>, Error> {
        const SQL: &str = r#"
        SELECT *
        FROM (
            SELECT
                (
                    LAG("id", 1, 0)
                    OVER(ORDER BY "id")
                ) AS "gap_begin",
                "id" AS "gap_end"
            FROM "block"
        )
        WHERE "gap_end" - "gap_begin" > 1
        "#;

        sqlx::query_as(SQL).fetch_all(&self.pool).await
    }

    pub async fn get_first_block(&self) -> Result<(i64,), Error> {
        const SQL: &str = r#"
        SELECT "id"
        FROM "block"
        ORDER BY "id" ASC
        LIMIT 1
        "#;

        sqlx::query_as(SQL).fetch_one(&self.pool).await
    }

    pub async fn get_last_block(&self) -> Result<(i64,), Error> {
        const SQL: &str = r#"
        SELECT "id"
        FROM "block"
        ORDER BY "id" DESC
        LIMIT 1
        "#;

        sqlx::query_as(SQL).fetch_one(&self.pool).await
    }

    pub async fn get_one(&self, id: i64) -> Result<Option<Block>, Error> {
        const SQL: &str = r#"
        SELECT *
        FROM "block"
        WHERE "id" = $1
        "#;

        sqlx::query_as(SQL)
            .bind(id)
            .fetch_optional(&self.pool)
            .await
    }

    pub async fn count(&self) -> Result<i64, Error> {
        const SQL: &str = r#"
        SELECT
            COUNT(*)
        FROM "block"
        "#;

        sqlx::query_as(SQL)
            .fetch_one(&self.pool)
            .await
            .map(|(val,)| val)
    }

    pub async fn is_synced_to_block(&self, block: i64) -> Result<bool, Error> {
        const SQL: &str = r#"
        SELECT
            COUNT(*) = $1
        FROM "block"
        WHERE id <= $1
        "#;

        sqlx::query_as(SQL)
            .bind(block)
            .fetch_one(&self.pool)
            .await
            .map(|(result,)| result)
    }
}
