use chrono::{DateTime, Utc};
use sqlx::{Error, QueryBuilder, Transaction};

use crate::model::{LS_Closing, Table};

use super::{DataBase, QueryResult};

impl Table<LS_Closing> {
    /// Inserts a record if it doesn't already exist, using ON CONFLICT DO NOTHING.
    pub async fn insert_if_not_exists(
        &self,
        data: LS_Closing,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<QueryResult, Error> {
        sqlx::query(
            r#"
            INSERT INTO "LS_Closing" ("LS_contract_id", "LS_timestamp", "Tx_Hash")
            VALUES($1, $2, $3)
            ON CONFLICT ("LS_contract_id") DO NOTHING
        "#,
        )
        .bind(&data.LS_contract_id)
        .bind(data.LS_timestamp)
        .bind(data.Tx_Hash)
        .persistent(true)
        .execute(&mut **transaction)
        .await
    }

    pub async fn insert_many(
        &self,
        data: &Vec<LS_Closing>,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<(), Error> {
        if data.is_empty() {
            return Ok(());
        }

        let mut query_builder: QueryBuilder<DataBase> = QueryBuilder::new(
            r#"
            INSERT INTO "LS_Closing" (
                "LS_contract_id", "LS_timestamp", "Tx_Hash"
            )"#,
        );

        query_builder.push_values(data, |mut b, ls| {
            b.push_bind(&ls.LS_contract_id)
                .push_bind(ls.LS_timestamp)
                .push_bind(&ls.Tx_Hash);
        });

        let query = query_builder.build().persistent(true);
        query.execute(&mut **transaction).await?;
        Ok(())
    }

    pub async fn count(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<i64, crate::error::Error> {
        let (value,): (i64,) = sqlx::query_as(
            r#"
            SELECT
                COUNT(*)
            FROM "LS_Closing" WHERE "LS_timestamp" > $1 AND "LS_timestamp" <= $2
            "#,
        )
        .bind(from)
        .bind(to)
        .persistent(true)
        .fetch_one(&self.pool)
        .await?;
        Ok(value)
    }
}
