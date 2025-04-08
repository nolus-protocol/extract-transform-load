use chrono::{DateTime, Utc};
use sqlx::{Error, QueryBuilder, Transaction};

use crate::model::{LS_Closing, Table};

use super::DataBase;

impl Table<LS_Closing> {
    // FIXME Pass data by reference, as separate arguments or as a dedicated
    //  structure. Avoid the need for owned data.
    pub async fn isExists(
        &self,
        ls_closing: &LS_Closing,
    ) -> Result<bool, Error> {
        const SQL: &str = r#"
        SELECT EXISTS(
            SELECT
            FROM "LS_Closing"
            WHERE "LS_contract_id" = $1
        )
        "#;

        sqlx::query_as(SQL)
            .bind(&ls_closing.LS_contract_id)
            .fetch_one(&self.pool)
            .await
            .map(|(result,)| result)
    }

    // FIXME Pass data by reference, as separate arguments or as a dedicated
    //  structure. Avoid the need for owned data.
    pub async fn insert(
        &self,
        data: LS_Closing,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<(), Error> {
        const SQL: &str = r#"
        INSERT INTO "LS_Closing" (
            "LS_contract_id",
            "LS_timestamp",
            "Tx_Hash"
        )
        VALUES ($1, $2, $3)
        "#;

        sqlx::query(SQL)
            .bind(&data.LS_contract_id)
            .bind(data.LS_timestamp)
            .bind(data.Tx_Hash)
            .execute(transaction.as_mut())
            .await
            .map(drop)
    }

    // FIXME Pass data by reference, as separate arguments or as a dedicated
    //  structure. Avoid the need for owned data.
    // FIXME Use iterators instead.
    pub async fn insert_many(
        &self,
        data: &Vec<LS_Closing>,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<(), Error> {
        const SQL: &str = r#"
        INSERT INTO "LS_Closing" (
            "LS_contract_id", "LS_timestamp", "Tx_Hash"
        )
        "#;

        if data.is_empty() {
            return Ok(());
        }

        QueryBuilder::new(SQL)
            .push_values(data, |mut b, ls| {
                b.push_bind(&ls.LS_contract_id)
                    .push_bind(ls.LS_timestamp)
                    .push_bind(&ls.Tx_Hash);
            })
            .build()
            .persistent(false)
            .execute(transaction.as_mut())
            .await
            .map(drop)
    }

    // FIXME Use `UInt63` instead.
    pub async fn count(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<i64, Error> {
        const SQL: &str = r#"
        SELECT
            COUNT(*)
        FROM "LS_Closing"
        WHERE
            "LS_timestamp" > $1 AND
            "LS_timestamp" <= $2
        "#;

        sqlx::query_as(SQL)
            .bind(from)
            .bind(to)
            .fetch_one(&self.pool)
            .await
            .map(|(result,)| result)
    }
}
