use super::{DataBase, QueryResult};
use crate::model::{Borrow_APR, LS_Opening, Leased_Asset, Table};
use chrono::{DateTime, Utc};
use sqlx::{error::Error, types::BigDecimal, QueryBuilder, Transaction};
use std::str::FromStr;

impl Table<LS_Opening> {
    pub async fn insert(
        &self,
        data: LS_Opening,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<QueryResult, Error> {
        sqlx::query(
            r#"
            INSERT INTO "LS_Opening" (
                "LS_contract_id",
                "LS_address_id",
                "LS_asset_symbol",
                "LS_interest",
                "LS_timestamp",
                "LS_loan_pool_id",
                "LS_loan_amnt_stable",
                "LS_loan_amnt_asset",
                "LS_cltr_symbol",
                "LS_cltr_amnt_stable",
                "LS_cltr_amnt_asset",
                "LS_native_amnt_stable",
                "LS_native_amnt_nolus"
            )
            VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            "#,
        )
        .bind(&data.LS_contract_id)
        .bind(&data.LS_address_id)
        .bind(&data.LS_asset_symbol)
        .bind(data.LS_interest)
        .bind(data.LS_timestamp)
        .bind(&data.LS_loan_pool_id)
        .bind(&data.LS_loan_amnt_stable)
        .bind(&data.LS_loan_amnt_asset)
        .bind(&data.LS_cltr_symbol)
        .bind(&data.LS_cltr_amnt_stable)
        .bind(&data.LS_cltr_amnt_asset)
        .bind(&data.LS_native_amnt_stable)
        .bind(&data.LS_native_amnt_nolus)
        .execute(&mut **transaction)
        .await
    }

    pub async fn insert_many(
        &self,
        data: &Vec<LS_Opening>,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<(), Error> {
        if data.is_empty() {
            return Ok(());
        }

        let mut query_builder: QueryBuilder<DataBase> = QueryBuilder::new(
            r#"
            INSERT INTO "LS_Opening" (
                "LS_contract_id",
                "LS_address_id",
                "LS_asset_symbol",
                "LS_interest",
                "LS_timestamp",
                "LS_loan_pool_id",
                "LS_loan_amnt_stable",
                "LS_loan_amnt_asset",
                "LS_cltr_symbol",
                "LS_cltr_amnt_stable",
                "LS_cltr_amnt_asset",
                "LS_native_amnt_stable",
                "LS_native_amnt_nolus"
            )"#,
        );

        query_builder.push_values(data, |mut b, ls| {
            b.push_bind(&ls.LS_contract_id)
                .push_bind(&ls.LS_address_id)
                .push_bind(&ls.LS_asset_symbol)
                .push_bind(ls.LS_interest)
                .push_bind(ls.LS_timestamp)
                .push_bind(&ls.LS_loan_pool_id)
                .push_bind(&ls.LS_loan_amnt_stable)
                .push_bind(&ls.LS_loan_amnt_asset)
                .push_bind(&ls.LS_cltr_symbol)
                .push_bind(&ls.LS_cltr_amnt_stable)
                .push_bind(&ls.LS_cltr_amnt_asset)
                .push_bind(&ls.LS_native_amnt_stable)
                .push_bind(&ls.LS_native_amnt_nolus);
        });

        let query = query_builder.build();
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
            FROM "LS_Opening" WHERE "LS_timestamp" > $1 AND "LS_timestamp" <= $2
            "#,
        )
        .bind(from)
        .bind(to)
        .fetch_one(&self.pool)
        .await?;
        Ok(value)
    }

    pub async fn get_cltr_amnt_opened_stable_sum(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<BigDecimal, crate::error::Error> {
        let value: (Option<BigDecimal>,) = sqlx::query_as(
            r#"
            SELECT 
                SUM("LS_cltr_amnt_stable")
            FROM "LS_Opening" WHERE "LS_timestamp" > $1 AND "LS_timestamp" <= $2
            "#,
        )
        .bind(from)
        .bind(to)
        .fetch_one(&self.pool)
        .await?;
        let (amnt,) = value;
        let amnt = amnt.unwrap_or(BigDecimal::from_str("0")?);

        Ok(amnt)
    }

    pub async fn get_loan_amnt_stable_sum(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<BigDecimal, crate::error::Error> {
        let value: (Option<BigDecimal>,) = sqlx::query_as(
            r#"
            SELECT 
                SUM("LS_loan_amnt_stable")
            FROM "LS_Opening" WHERE "LS_timestamp" > $1 AND "LS_timestamp" <= $2
            "#,
        )
        .bind(from)
        .bind(to)
        .fetch_one(&self.pool)
        .await?;
        let (amnt,) = value;
        let amnt = amnt.unwrap_or(BigDecimal::from_str("0")?);

        Ok(amnt)
    }

    pub async fn get_ls_cltr_amnt_stable_sum(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<BigDecimal, crate::error::Error> {
        let value: (Option<BigDecimal>,) = sqlx::query_as(
            r#"
            SELECT 
                SUM("LS_cltr_amnt_stable")
            FROM "LS_Opening"
            LEFT JOIN 
                "LS_Closing"
            ON
                "LS_Opening"."LS_contract_id" = "LS_Closing"."LS_contract_id"
            WHERE "LS_Closing"."LS_timestamp" > $1 AND "LS_Closing"."LS_timestamp" <= $2
            "#,
        )
        .bind(from)
        .bind(to)
        .fetch_one(&self.pool)
        .await?;
        let (amnt,) = value;
        let amnt = amnt.unwrap_or(BigDecimal::from_str("0")?);

        Ok(amnt)
    }

    pub async fn get_ls_amnt_stable_sum(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<BigDecimal, crate::error::Error> {
        let value: (Option<BigDecimal>,) = sqlx::query_as(
            r#"
            SELECT 
                SUM("LS_loan_amnt_stable" + "LS_cltr_amnt_stable")
            FROM "LS_Opening"
            LEFT JOIN 
                "LS_Closing"
            ON
                "LS_Opening"."LS_contract_id" = "LS_Closing"."LS_contract_id"
            WHERE "LS_Closing"."LS_timestamp" > $1 AND "LS_Closing"."LS_timestamp" <= $2
            "#,
        )
        .bind(from)
        .bind(to)
        .fetch_one(&self.pool)
        .await?;
        let (amnt,) = value;
        let amnt = amnt.unwrap_or(BigDecimal::from_str("0")?);

        Ok(amnt)
    }

    pub async fn get_borrow_apr(&self, skip: i64, limit: i64) -> Result<Vec<Borrow_APR>, Error> {
        let data = sqlx::query_as(
            r#"
            SELECT "LS_interest" / 10.0 AS "APR" FROM "LS_Opening" ORDER BY "LS_timestamp" DESC OFFSET $1 LIMIT $2
            "#,
        )
        .bind(skip)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(data)
    }

    pub async fn get_leased_assets(&self) -> Result<Vec<Leased_Asset>, Error> {
        let data = sqlx::query_as(
            r#"
            SELECT "LS_asset_symbol" AS "Asset", SUM("LS_loan_amnt_asset" / 1000000) AS "Loan" FROM "LS_Opening" GROUP BY "Asset"
            "#,
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(data)
    }

    // TODO: add for mysql feature
    pub async fn get_earn_apr(&self) -> Result<BigDecimal, crate::error::Error> {
        let value: Option<(BigDecimal,)> = sqlx::query_as(
            r#"
                WITH DateRange AS (
                    SELECT
                    generate_series(
                        CURRENT_DATE - INTERVAL '30 days',
                        CURRENT_DATE,
                        '1 day'
                    ) :: date AS date
                ),
                DailyInterest AS (
                    SELECT
                    DATE("LS_timestamp") AS date,
                    MAX("LS_interest") AS max_interest
                    FROM
                    "LS_Opening"
                    WHERE
                    "LS_timestamp" >= CURRENT_DATE - INTERVAL '30 days'
                    GROUP BY
                    DATE("LS_timestamp")
                ),
                MaxLSInterest AS (
                    SELECT
                    dr.date,
                    COALESCE(
                        di.max_interest,
                        FIRST_VALUE(di.max_interest) OVER (
                        ORDER BY
                            dr.date ROWS BETWEEN UNBOUNDED PRECEDING
                            AND 1 PRECEDING
                        )
                    ) AS max_interest
                    FROM
                    DateRange dr
                    LEFT JOIN DailyInterest di ON dr.date = di.date
                ),
                MaxLPRatio AS (
                    SELECT
                    DATE("LP_Pool_timestamp") AS date,
                    (
                        "LP_Pool_total_borrowed_stable" / "LP_Pool_total_value_locked_stable"
                    ) AS ratio
                    FROM
                    (
                        SELECT
                        *,
                        RANK() OVER (
                            PARTITION BY DATE("LP_Pool_timestamp")
                            ORDER BY
                            (
                                "LP_Pool_total_borrowed_stable" / "LP_Pool_total_value_locked_stable"
                            ) DESC
                        ) AS rank
                        FROM
                        "LP_Pool_State"
                        WHERE
                        "LP_Pool_timestamp" >= CURRENT_DATE - INTERVAL '30 days'
                    ) ranked
                    WHERE
                    ranked.rank = 1
                ),
                APRCalc AS (
                    SELECT
                    AVG((mli.max_interest - 40) * mlr.ratio) / 10 AS "Earn APR"
                    FROM
                    MaxLSInterest mli
                    JOIN MaxLPRatio mlr ON mli.date = mlr.date
                )
                SELECT
                    (POWER((1 + ("Earn APR" / 100 / 365)), 365) - 1) * 100 AS "Earn APY"
                FROM APRCalc;
              "#,
        )
        .fetch_optional(&self.pool)
        .await?;

        let amnt = value.unwrap_or((BigDecimal::from_str("0")?,));

        Ok(amnt.0)
    }
}
