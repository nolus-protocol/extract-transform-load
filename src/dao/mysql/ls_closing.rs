use super::{DataBase, QueryResult};
use crate::model::{LS_Closing, Table};
use chrono::{DateTime, Utc};
use sqlx::{error::Error, QueryBuilder, Transaction};

impl Table<LS_Closing> {
    pub async fn insert(
        &self,
        data: LS_Closing,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<QueryResult, Error> {
        sqlx::query(
            r#"
            INSERT INTO `LS_Closing` (`LS_contract_id`, `LS_timestamp`)
            VALUES(?, ?)
        "#,
        )
        .bind(&data.LS_contract_id)
        .bind(&data.LS_timestamp)
        .execute(transaction)
        .await
    }

    pub async fn insert_many(
        &self,
        data: &Vec<LS_Closing>,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<(), Error> {
        if data.len() == 0 {
            return Ok(());
        }

        let mut query_builder: QueryBuilder<DataBase> = QueryBuilder::new(
            r#"
            INSERT INTO `LS_Closing` (
                `LS_contract_id`, `LS_timestamp`
            )"#,
        );

        query_builder.push_values(data, |mut b, ls| {
            b.push_bind(&ls.LS_contract_id).push_bind(&ls.LS_timestamp);
        });

        let query = query_builder.build();
        query.execute(transaction).await?;
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
            FROM `LS_Closing` WHERE `LS_timestamp` > ? AND `LS_timestamp` <= ?
            "#,
        )
        .bind(from)
        .bind(to)
        .fetch_one(&self.pool)
        .await?;
        Ok(value)
    }
}
