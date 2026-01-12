use std::str::FromStr as _;

use chrono::{DateTime, Utc};
use sqlx::{types::BigDecimal, Error, QueryBuilder, Transaction};

use crate::model::{Buyback, TR_Profit, Table};

use super::{DataBase, QueryResult};

impl Table<TR_Profit> {
    pub async fn isExists(
        &self,
        tr_profit: &TR_Profit,
    ) -> Result<bool, crate::error::Error> {
        let (value,): (i64,) = sqlx::query_as(
            r#"
            SELECT
                COUNT(*)
            FROM "TR_Profit"
            WHERE
                "TR_Profit_height" = $1 AND
                "TR_Profit_timestamp" = $2
            "#,
        )
        .bind(tr_profit.TR_Profit_height)
        .bind(tr_profit.TR_Profit_timestamp)
        .persistent(true)
        .fetch_one(&self.pool)
        .await?;

        if value > 0 {
            return Ok(true);
        }

        Ok(false)
    }

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
                "TR_Profit_amnt_nls",
                "Tx_Hash"
            )
            VALUES($1, $2, $3, $4, $5)
        "#,
        )
        .bind(data.TR_Profit_height)
        .bind(data.TR_Profit_timestamp)
        .bind(&data.TR_Profit_amnt_stable)
        .bind(&data.TR_Profit_amnt_nls)
        .bind(&data.Tx_Hash)
        .persistent(true)
        .execute(&mut **transaction)
        .await
    }

    /// Inserts a record if it doesn't already exist, using ON CONFLICT DO NOTHING.
    /// More efficient than calling isExists() followed by insert().
    pub async fn insert_if_not_exists(
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
                "TR_Profit_amnt_nls",
                "Tx_Hash"
            )
            VALUES($1, $2, $3, $4, $5)
            ON CONFLICT ("TR_Profit_height", "TR_Profit_timestamp") DO NOTHING
        "#,
        )
        .bind(data.TR_Profit_height)
        .bind(data.TR_Profit_timestamp)
        .bind(&data.TR_Profit_amnt_stable)
        .bind(&data.TR_Profit_amnt_nls)
        .bind(&data.Tx_Hash)
        .persistent(true)
        .execute(&mut **transaction)
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
                "TR_Profit_amnt_nls",
                "Tx_Hash"
            )"#,
        );

        query_builder.push_values(data, |mut b, tr| {
            b.push_bind(tr.TR_Profit_height)
                .push_bind(tr.TR_Profit_timestamp)
                .push_bind(&tr.TR_Profit_amnt_stable)
                .push_bind(&tr.TR_Profit_amnt_nls)
                .push_bind(&tr.Tx_Hash);
        });

        let query = query_builder.build().persistent(true);
        query.execute(&mut **transaction).await?;
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
        .persistent(true)
        .fetch_one(&self.pool)
        .await?;
        let (amnt, amnt_nolus) = value;
        let amnt = amnt.unwrap_or(BigDecimal::from_str("0")?);
        let amnt_nolus = amnt_nolus.unwrap_or(BigDecimal::from_str("0")?);

        Ok((amnt, amnt_nolus))
    }

    pub async fn get_buyback(
        &self,
        skip: i64,
        limit: i64,
    ) -> Result<Vec<Buyback>, Error> {
        let data = sqlx::query_as(
            r#"
                SELECT "TR_Profit_timestamp" AS time, (SUM("TR_Profit_amnt_nls" / 1000000) OVER ( Order By "TR_Profit_timestamp")) AS "Bought-back" FROM "TR_Profit" OFFSET $1 LIMIT $2
            "#,
        )
        .bind(skip)
        .bind(limit)
        .persistent(true)
        .fetch_all(&self.pool)
        .await?;
        Ok(data)
    }

    /// Get buyback data with time window filtering
    /// - months: number of months to look back (None = all time)
    /// - from: only return records after this timestamp (exclusive)
    pub async fn get_buyback_with_window(
        &self,
        months: Option<i32>,
        from: Option<DateTime<Utc>>,
    ) -> Result<Vec<Buyback>, Error> {
        // Build time conditions
        let mut conditions = Vec::new();
        if let Some(m) = months {
            conditions.push(format!("\"TR_Profit_timestamp\" >= NOW() - INTERVAL '{} months'", m));
        }
        if from.is_some() {
            conditions.push("\"TR_Profit_timestamp\" > $1".to_string());
        }

        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };

        let query = format!(
            r#"
            SELECT "TR_Profit_timestamp" AS time,
                   (SUM("TR_Profit_amnt_nls" / 1000000) OVER (ORDER BY "TR_Profit_timestamp")) AS "Bought-back"
            FROM "TR_Profit"
            {}
            ORDER BY "TR_Profit_timestamp" ASC
            "#,
            where_clause
        );

        let data = if let Some(from_ts) = from {
            sqlx::query_as(&query)
                .bind(from_ts)
                .persistent(true)
                .fetch_all(&self.pool)
                .await?
        } else {
            sqlx::query_as(&query)
                .persistent(true)
                .fetch_all(&self.pool)
                .await?
        };

        Ok(data)
    }

    pub async fn get_buyback_total(
        &self,
    ) -> Result<BigDecimal, crate::error::Error> {
        let value: (Option<BigDecimal>,) = sqlx::query_as(
            r#"
                SELECT SUM("TR_Profit_amnt_nls") / 1000000 AS "Distributed" FROM "TR_Profit"
            "#,
        )
        .persistent(true)
        .fetch_one(&self.pool)
        .await?;

        let (amnt,) = value;
        let amnt = amnt.unwrap_or(BigDecimal::from_str("0")?);

        Ok(amnt)
    }

    pub async fn get_revenue(&self) -> Result<BigDecimal, crate::error::Error> {
        let value: (Option<BigDecimal>,) = sqlx::query_as(
            r#"
                SELECT SUM("TR_Profit_amnt_stable") / 1000000 AS "Distributed" FROM "TR_Profit"
            "#,
        )
        .persistent(true)
        .fetch_one(&self.pool)
        .await?;

        let (amnt,) = value;
        let amnt = amnt.unwrap_or(BigDecimal::from_str("0")?);

        Ok(amnt)
    }

    pub async fn get_revenue_series(
        &self,
    ) -> Result<Vec<(DateTime<Utc>, BigDecimal, BigDecimal)>, crate::error::Error>
    {
        let data: Vec<(DateTime<Utc>, BigDecimal, BigDecimal)> = sqlx::query_as(
            r#"
            SELECT
                DATE_TRUNC('day', "TR_Profit_timestamp") AS time,
                SUM("TR_Profit_amnt_stable") / 1000000 AS daily,
                SUM(SUM("TR_Profit_amnt_stable")) OVER (ORDER BY DATE_TRUNC('day', "TR_Profit_timestamp")) / 1000000 AS cumulative
            FROM "TR_Profit"
            WHERE "TR_Profit_amnt_stable" < 10000000000
            GROUP BY DATE_TRUNC('day', "TR_Profit_timestamp")
            ORDER BY time ASC
            "#,
        )
        .persistent(true)
        .fetch_all(&self.pool)
        .await?;

        Ok(data)
    }

    pub async fn get_revenue_series_with_window(
        &self,
        months: Option<i32>,
        from: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<Vec<(DateTime<Utc>, BigDecimal, BigDecimal)>, crate::error::Error> {
        let time_filter = match (months, from) {
            (_, Some(from_ts)) => format!(
                r#"AND "TR_Profit_timestamp" > '{}'"#,
                from_ts.format("%Y-%m-%d %H:%M:%S")
            ),
            (Some(m), None) => format!(
                r#"AND "TR_Profit_timestamp" > NOW() - INTERVAL '{} months'"#,
                m
            ),
            (None, None) => String::new(),
        };

        let query_str = format!(
            r#"
            SELECT
                DATE_TRUNC('day', "TR_Profit_timestamp") AS time,
                SUM("TR_Profit_amnt_stable") / 1000000 AS daily,
                SUM(SUM("TR_Profit_amnt_stable")) OVER (ORDER BY DATE_TRUNC('day', "TR_Profit_timestamp")) / 1000000 AS cumulative
            FROM "TR_Profit"
            WHERE "TR_Profit_amnt_stable" < 10000000000
            {}
            GROUP BY DATE_TRUNC('day', "TR_Profit_timestamp")
            ORDER BY time ASC
            "#,
            time_filter
        );

        let data: Vec<(DateTime<Utc>, BigDecimal, BigDecimal)> = sqlx::query_as(&query_str)
            .persistent(true)
            .fetch_all(&self.pool)
            .await?;

        Ok(data)
    }
}
