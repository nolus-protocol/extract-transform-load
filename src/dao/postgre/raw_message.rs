use sqlx::{error::Error, Transaction};

use crate::model::{Raw_Message, Table};

use super::DataBase;

impl Table<Raw_Message> {
    pub async fn insert(
        &self,
        &Raw_Message {
            index,
            ref from,
            ref to,
            ref r#type,
            ref value,
            ref tx_hash,
            block,
            ref fee_amount,
            ref fee_denom,
            ref memo,
            timestamp,
            ref rewards,
        }: &Raw_Message,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<(), Error> {
        const SQL: &'static str = r#"
        INSERT INTO "raw_message" (
            "index",
            "from",
            "to",
            "type",
            "value",
            "tx_hash",
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
            .bind(index)
            .bind(from)
            .bind(to)
            .bind(r#type)
            .bind(value)
            .bind(tx_hash)
            .bind(block)
            .bind(fee_amount)
            .bind(fee_denom)
            .bind(memo)
            .bind(timestamp)
            .bind(rewards)
            .execute(&mut **transaction)
            .await
            .map(drop)
    }

    pub async fn isExists(
        &self,
        &Raw_Message {
            index, ref tx_hash, ..
        }: &Raw_Message,
    ) -> Result<bool, Error> {
        const SQL: &str = r#"
        SELECT COUNT(1) > 0
        FROM "raw_message"
        WHERE
            "index" = $1 AND
            "tx_hash" = $2
        "#;

        sqlx::query_as(SQL)
            .bind(index)
            .bind(tx_hash)
            .fetch_one(&self.pool)
            .await
            .map(|(result,)| result)
    }

    pub async fn get(
        &self,
        address: &str,
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
            .bind(address)
            .bind(skip)
            .bind(limit)
            .fetch_all(&self.pool)
            .await
    }
}
