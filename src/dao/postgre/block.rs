use sqlx::{FromRow, Transaction};

use crate::{
    custom_uint::UInt63,
    error::Error,
    model::{Block, Table},
};

use super::DataBase;

type Result<T, E = sqlx::Error> = std::result::Result<T, E>;

#[derive(Debug, Clone, Copy, FromRow)]
struct Gap {
    start: UInt63,
    end: UInt63,
}

impl Table<Block> {
    pub async fn insert(
        &self,
        Block { id }: Block,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<()> {
        const SQL: &str = r#"
        INSERT INTO "block" ("id")
        VALUES ($1)
        "#;

        sqlx::query(SQL)
            .bind(id)
            .execute(&mut **transaction)
            .await
            .map(drop)
    }

    pub async fn get_missing_blocks(&self) -> Result<Vec<Gap>> {
        const SQL: &str = r#"
        SELECT *
        FROM (
            SELECT
                (
                    LAG("id", 1, 0)
                    OVER(ORDER BY "id")
                ) AS "start",
                "id" AS "end"
            FROM "block"
        )
        WHERE "end" - "start" > 1
        "#;

        sqlx::query_as(SQL).fetch_all(&self.pool).await
    }

    pub async fn get_first_block(&self) -> Result<UInt63> {
        const SQL: &str = r#"
        SELECT "id"
        FROM "block"
        ORDER BY "id" ASC
        "#;

        sqlx::query_as(SQL)
            .fetch_one(&self.pool)
            .await
            .map(|(result,)| result)
    }

    pub async fn get_last_block(&self) -> Result<UInt63> {
        const SQL: &str = r#"
        SELECT "id"
        FROM "block"
        ORDER BY "id" DESC
        LIMIT 1
        "#;

        sqlx::query_as(SQL)
            .fetch_one(&self.pool)
            .await
            .map(|(result,)| result)
    }

    pub async fn get_one(&self, id: UInt63) -> Result<Option<Block>> {
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

    pub async fn count(&self) -> Result<UInt63, Error> {
        const SQL: &str = r#"
        SELECT COUNT(1)
        FROM "block"
        "#;

        sqlx::query_as(SQL)
            .fetch_one(&self.pool)
            .await
            .map_err(From::from)
            .map(|(result,)| result)
    }

    pub async fn is_synced_to_block(&self, block: UInt63) -> Result<bool> {
        const SQL: &str = r#"
        SELECT COUNT(1) = $1
        FROM "block"
        WHERE id <= $1
        "#;

        sqlx::query_as(SQL)
            .bind(block.get_signed())
            .fetch_one(&self.pool)
            .await
            .map(|(result,)| result)
    }
}
