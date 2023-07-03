use super::{DataBase, QueryResult};
use crate::model::{TR_Profit, Table};
use chrono::{DateTime, Utc};
use sqlx::{error::Error, types::BigDecimal, QueryBuilder, Transaction};
use std::str::FromStr;

impl Table<TR_Profit> {
    pub async fn insert(
        &self,
        data: TR_Profit,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<QueryResult, Error> {
        sqlx::query(
            r#"
            INSERT INTO "TR_Profit" (
                "TR_Profit_height",
                "TR_Profit_timestamp",
                "TR_Profit_amnt_stable",
                "TR_Profit_amnt_nls"
            )
            VALUES($1, $2, $3, $4)
        "#,
        )
        .bind(data.TR_Profit_height)
        .bind(data.TR_Profit_timestamp)
        .bind(&data.TR_Profit_amnt_stable)
        .bind(&data.TR_Profit_amnt_nls)
        .execute(transaction)
        .await
    }

    pub async fn insert_many(
        &self,
        data: &Vec<TR_Profit>,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<(), Error> {
        if data.is_empty() {
            return Ok(());
        }

        let mut query_builder: QueryBuilder<DataBase> = QueryBuilder::new(
            r#"
            INSERT INTO "TR_Profit" (
                "TR_Profit_height",
                "TR_Profit_timestamp",
                "TR_Profit_amnt_stable",
                "TR_Profit_amnt_nls"
            )"#,
        );

        query_builder.push_values(data, |mut b, tr| {
            b.push_bind(tr.TR_Profit_height)
                .push_bind(tr.TR_Profit_timestamp)
                .push_bind(&tr.TR_Profit_amnt_stable)
                .push_bind(&tr.TR_Profit_amnt_nls);
        });

        let query = query_builder.build();
        query.execute(transaction).await?;
        Ok(())
    }

    pub async fn get_amnt_stable(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<(BigDecimal, BigDecimal), crate::error::Error> {
        let value: (Option<BigDecimal>, Option<BigDecimal>) = sqlx::query_as(
            r#"
            SELECT 
                SUM("TR_Profit_amnt_stable"),
                SUM("TR_Profit_amnt_nls")
            FROM "TR_Profit" WHERE "TR_Profit_timestamp" > $1 AND "TR_Profit_timestamp" <= $2
            "#,
        )
        .bind(from)
        .bind(to)
        .fetch_one(&self.pool)
        .await?;
        let (amnt, amnt_nolus) = value;
        let amnt = amnt.unwrap_or(BigDecimal::from_str("0")?);
        let amnt_nolus = amnt_nolus.unwrap_or(BigDecimal::from_str("0")?);

        Ok((amnt, amnt_nolus))
    }
}
