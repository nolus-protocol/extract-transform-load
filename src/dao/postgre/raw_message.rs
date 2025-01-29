use sqlx::{Error, Transaction};

use crate::model::{Raw_Message, Table};

use super::DataBase;

impl Table<Raw_Message> {
    pub async fn insert(
        &self,
        data: Raw_Message,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<(), Error> {
        const SQL: &str = r#"
        INSERT INTO "raw_message" (
            "index",
            "from",
            "to",
            "tx_hash",
            "type",
            "value",
            "block",
            "fee_amount",
            "fee_denom",
            "memo",
            "timestamp",
            "rewards"
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        "#;

        sqlx::query(SQL)
            .bind(data.index)
            .bind(&data.from)
            .bind(&data.to)
            .bind(&data.tx_hash)
            .bind(&data.r#type)
            .bind(&data.value)
            .bind(data.block)
            .bind(&data.fee_amount)
            .bind(&data.fee_denom)
            .bind(&data.memo)
            .bind(data.timestamp)
            .bind(&data.rewards)
            .execute(&mut **transaction)
            .await
            .map(drop)
    }

    pub async fn isExists(&self, data: &Raw_Message) -> Result<bool, Error> {
        const SQL: &str = r#"
        SELECT EXISTS(
            SELECT
            FROM "raw_message"
            WHERE
                "index" = $1 AND
                "tx_hash" = $2
        )
        "#;

        sqlx::query_as(SQL)
            .bind(data.index)
            .bind(&data.tx_hash)
            .fetch_one(&self.pool)
            .await
            .map(|(result,)| result)
    }

    pub async fn get(
        &self,
        address: String,
        skip: i64,
        limit: i64,
    ) -> Result<Vec<Raw_Message>, Error> {
        const SQL: &str = r#"
        SELECT *
        FROM "raw_message"
        WHERE
            "from" = $1 OR
            "to" = $1
        ORDER BY "timestamp" DESC
        OFFSET $2
        LIMIT $3
        "#;

        sqlx::query_as(SQL)
            .bind(&address)
            .bind(skip)
            .bind(limit)
            .fetch_all(&self.pool)
            .await
    }
}
