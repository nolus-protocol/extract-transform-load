use std::iter;

use chrono::{DateTime, Utc};
use sqlx::{error::Error, QueryBuilder, Transaction};

use crate::{
    custom_uint::UInt63,
    model::{LS_Closing, Table},
};

use super::DataBase;

impl Table<LS_Closing> {
    pub async fn isExists(&self, contract_id: &str) -> Result<bool, Error> {
        const SQL: &str = r#"
        SELECT COUNT(1) > 0
        FROM "LS_Closing"
        WHERE "LS_contract_id" = $1
        "#;

        sqlx::query_as(SQL)
            .bind(contract_id)
            .fetch_one(&self.pool)
            .await
            .map(|(result,)| result)
    }

    pub async fn insert(
        &self,
        &LS_Closing {
            ref Tx_Hash,
            ref LS_contract_id,
            LS_timestamp,
        }: &LS_Closing,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<(), Error> {
        const SQL: &str = r#"
        INSERT INTO "LS_Closing" (
            "Tx_Hash",
            "LS_contract_id",
            "LS_timestamp"
        )
        VALUES ($1, $2, $3)
        "#;

        sqlx::query_as(SQL)
            .bind(Tx_Hash)
            .bind(LS_contract_id)
            .bind(LS_timestamp)
            .execute(&mut **transaction)
            .await
    }

    pub async fn insert_many<'r, T>(
        &self,
        data: T,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<(), Error>
    where
        T: Iterator<Item = &'r LS_Closing>,
    {
        const SQL: &str = r#"
        INSERT INTO "LS_Closing" (
            "Tx_Hash",
            "LS_contract_id",
            "LS_timestamp"
        )
        "#;

        let mut iter = data.iter();

        let Some(first) = iter.next() else {
            return Ok(());
        };

        QueryBuilder::new(SQL)
            .push_values(
                iter::once(first).chain(iter),
                |mut b,
                 &LS_Closing {
                     ref Tx_Hash,
                     ref LS_contract_id,
                     LS_timestamp,
                 }| {
                    b.push_bind(Tx_Hash)
                        .push_bind(LS_contract_id)
                        .push_bind(LS_timestamp);
                },
            )
            .build()
            .execute(&mut **transaction)
            .await
            .map(drop)
    }

    pub async fn count(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<UInt63, Error> {
        const SQL: &'static str = r#"
        SELECT COUNT(1)
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
