use sqlx::{Error, Transaction};

use crate::model::{Raw_Message, Table};

use super::{DataBase, QueryResult};

impl Table<Raw_Message> {
    pub async fn insert(
        &self,
        data: Raw_Message,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<QueryResult, Error> {
        sqlx::query(
            r#"
            INSERT INTO "raw_message" ("index", "from", "to", "tx_hash", "type", "value", "block", "fee_amount", "fee_denom", "memo", "timestamp", "rewards")
            VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            "#,
        )
        .bind(data.index)
        .bind(&data.from)
        .bind(&data.to)
        .bind(&data.tx_hash)
        .bind(&data.r#type)
        .bind(&data.value)
        .bind(&data.block)
        .bind(&data.fee_amount)
        .bind(&data.fee_denom)
        .bind(&data.memo)
        .bind(&data.timestamp)
        .bind(&data.rewards)
        .persistent(false)
        .execute(&mut **transaction)
        .await
    }

    pub async fn isExists(
        &self,
        data: &Raw_Message,
    ) -> Result<bool, crate::error::Error> {
        let (value,): (i64,) = sqlx::query_as(
            r#"
            SELECT 
                COUNT(*)
            FROM "raw_message" 
            WHERE 
                "index" = $1 AND
                "tx_hash" = $2
            "#,
        )
        .bind(data.index)
        .bind(&data.tx_hash)
        .persistent(false)
        .fetch_one(&self.pool)
        .await?;

        if value > 0 {
            return Ok(true);
        }

        Ok(false)
    }

    pub async fn get(
        &self,
        address: String,
        skip: i64,
        limit: i64,
    ) -> Result<Vec<Raw_Message>, Error> {
        let data = sqlx::query_as(
            r#"
            SELECT * FROM "raw_message" WHERE "from" = $1 OR "to" = $2 ORDER BY "timestamp" DESC OFFSET $3 LIMIT $4
            "#,
        )
        .bind(&address)
        .bind(&address)
        .bind(skip)
        .bind(limit)
        .persistent(false)
        .fetch_all(&self.pool)
        .await?;
        Ok(data)
    }
}
