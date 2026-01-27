use std::str::FromStr as _;

use chrono::{DateTime, Utc};
use sqlx::{types::BigDecimal, Error, FromRow, QueryBuilder, Transaction};

use crate::model::{LS_Repayment, Table};

use super::{DataBase, QueryResult};

type OptionDecimal = Option<BigDecimal>;

#[derive(Debug, Clone, FromRow)]
pub struct InterestRepaymentData {
    pub timestamp: DateTime<Utc>,
    pub contract_id: String,
    pub position_owner: String,
    pub position_type: String,
    pub event_type: String,
    pub loan_interest_repaid: BigDecimal,
    pub margin_interest_repaid: BigDecimal,
}

#[derive(Debug, Clone, FromRow)]
pub struct HistoricallyRepaid {
    pub contract_id: String,
    pub symbol: String,
    pub loan: BigDecimal,
    pub total_repaid: BigDecimal,
    pub close_timestamp: Option<DateTime<Utc>>,
    pub loan_closed: String,
}

impl Table<LS_Repayment> {
    pub async fn insert_if_not_exists(
        &self,
        data: LS_Repayment,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<QueryResult, Error> {
        sqlx::query(
            r#"
            INSERT INTO "LS_Repayment" (
                "LS_repayment_height",
                "LS_contract_id",
                "LS_payment_symbol",
                "LS_payment_amnt",
                "LS_payment_amnt_stable",
                "LS_timestamp",
                "LS_loan_close",
                "LS_prev_margin_stable",
                "LS_prev_interest_stable",
                "LS_current_margin_stable",
                "LS_current_interest_stable",
                "LS_principal_stable",
                "Tx_Hash"
            )
            VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            ON CONFLICT ("LS_repayment_height", "LS_contract_id", "LS_timestamp") DO NOTHING
        "#,
        )
        .bind(data.LS_repayment_height)
        .bind(&data.LS_contract_id)
        .bind(&data.LS_payment_symbol)
        .bind(&data.LS_payment_amnt)
        .bind(&data.LS_payment_amnt_stable)
        .bind(data.LS_timestamp)
        .bind(data.LS_loan_close)
        .bind(&data.LS_prev_margin_stable)
        .bind(&data.LS_prev_interest_stable)
        .bind(&data.LS_current_margin_stable)
        .bind(&data.LS_current_interest_stable)
        .bind(&data.LS_principal_stable)
        .bind(&data.Tx_Hash)
        .persistent(true)
        .execute(&mut **transaction)
        .await
    }

    pub async fn insert_many(
        &self,
        data: &Vec<LS_Repayment>,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<(), Error> {
        if data.is_empty() {
            return Ok(());
        }

        let mut query_builder: QueryBuilder<DataBase> = QueryBuilder::new(
            r#"
            INSERT INTO "LS_Repayment" (
                "LS_repayment_height",
                "LS_contract_id",
                "LS_payment_symbol",
                "LS_payment_amnt",
                "LS_payment_amnt_stable",
                "LS_timestamp",
                "LS_loan_close",
                "LS_prev_margin_stable",
                "LS_prev_interest_stable",
                "LS_current_margin_stable",
                "LS_current_interest_stable",
                "LS_principal_stable",
                "Tx_Hash"
            )"#,
        );

        query_builder.push_values(data, |mut b, ls| {
            b.push_bind(ls.LS_repayment_height)
                .push_bind(&ls.LS_contract_id)
                .push_bind(&ls.LS_payment_symbol)
                .push_bind(&ls.LS_payment_amnt)
                .push_bind(&ls.LS_payment_amnt_stable)
                .push_bind(ls.LS_timestamp)
                .push_bind(ls.LS_loan_close)
                .push_bind(&ls.LS_prev_margin_stable)
                .push_bind(&ls.LS_prev_interest_stable)
                .push_bind(&ls.LS_current_margin_stable)
                .push_bind(&ls.LS_current_interest_stable)
                .push_bind(&ls.LS_principal_stable)
                .push_bind(&ls.Tx_Hash);
        });

        let query = query_builder.build().persistent(true);
        query.execute(&mut **transaction).await?;
        Ok(())
    }

    pub async fn get_sum(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<
        (BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal),
        crate::error::Error,
    > {
        let value: (
            OptionDecimal,
            OptionDecimal,
            OptionDecimal,
            OptionDecimal,
            OptionDecimal,
        ) = sqlx::query_as(
            r#"
            SELECT
                SUM("LS_prev_margin_stable"),
                SUM("LS_prev_interest_stable"),
                SUM("LS_current_margin_stable"),
                SUM("LS_current_interest_stable"),
                SUM("LS_principal_stable")
            FROM "LS_Repayment" WHERE "LS_timestamp" > $1 AND "LS_timestamp" < $2
            "#,
        )
        .bind(from)
        .bind(to)
        .persistent(true)
        .fetch_one(&self.pool)
        .await?;
        let (
            prev_margin_stable,
            prev_interest_stable,
            current_margin_stable,
            current_interest_stable,
            prinicap_stable,
        ) = value;

        let prev_margin_stable =
            prev_margin_stable.unwrap_or(BigDecimal::from_str("0")?);
        let prev_interest_stable =
            prev_interest_stable.unwrap_or(BigDecimal::from_str("0")?);
        let current_margin_stable =
            current_margin_stable.unwrap_or(BigDecimal::from_str("0")?);
        let current_interest_stable =
            current_interest_stable.unwrap_or(BigDecimal::from_str("0")?);
        let prinicap_stable =
            prinicap_stable.unwrap_or(BigDecimal::from_str("0")?);

        Ok((
            prev_margin_stable,
            prev_interest_stable,
            current_margin_stable,
            current_interest_stable,
            prinicap_stable,
        ))
    }

    pub async fn get_by_contract(
        &self,
        contract: String,
    ) -> Result<Vec<LS_Repayment>, Error> {
        let data = sqlx::query_as(
            r#"
                SELECT
                    "LS_repayment_height",
                    "LS_repayment_idx",
                    "LS_contract_id",
                    "LS_payment_symbol",
                    "LS_payment_amnt",
                    "LS_payment_amnt_stable",
                    "LS_timestamp",
                    "LS_loan_close",
                    "LS_prev_margin_stable",
                    "LS_prev_interest_stable",
                    "LS_current_margin_stable",
                    "LS_current_interest_stable",
                    "LS_principal_stable",
                    "Tx_Hash"
                FROM
                    "LS_Repayment" as a
                WHERE
                    a."LS_contract_id" = $1
            "#,
        )
        .bind(contract)
        .persistent(true)
        .fetch_all(&self.pool)
        .await?;
        Ok(data)
    }

    pub async fn get_historically_repaid(
        &self,
    ) -> Result<Vec<HistoricallyRepaid>, crate::error::Error> {
        let data = sqlx::query_as(
            r#"
            WITH Closed_Loans AS (
                SELECT
                    "LS_contract_id",
                    "LS_timestamp",
                    "LS_amnt_stable",
                    "LS_loan_close",
                    "LS_amnt_symbol" AS "Amount Symbol"
                FROM
                    "LS_Close_Position"
                UNION ALL
                SELECT
                    "LS_contract_id",
                    "LS_timestamp",
                    "LS_payment_amnt_stable" AS "LS_amnt_stable",
                    "LS_loan_close",
                    "LS_payment_symbol" AS "Amount Symbol"
                FROM
                    "LS_Repayment"
            ),
            RepaidLeases AS (
                SELECT
                    lso."LS_contract_id" AS "Contract ID",
                    lso."LS_asset_symbol" AS "Symbol",
                    lso."LS_loan_amnt_asset" / COALESCE(pc.lpn_decimals, 1000000)::numeric AS "Loan",
                    COALESCE(
                        SUM(cl."LS_amnt_stable" / POWER(10, COALESCE(cr_amnt.decimal_digits, 6))),
                        0
                    ) AS "Total Repaid",
                    MAX(
                        CASE
                            WHEN cl."LS_loan_close" = true THEN cl."LS_timestamp"
                        END
                    ) AS "Close Timestamp",
                    CASE
                        WHEN SUM(
                            CASE
                                WHEN cl."LS_loan_close" = true THEN 1
                                ELSE 0
                            END
                        ) > 0 THEN 'yes'
                        ELSE 'no'
                    END AS "Loan Closed"
                FROM
                    "LS_Opening" lso
                    LEFT JOIN Closed_Loans cl ON lso."LS_contract_id" = cl."LS_contract_id"
                    LEFT JOIN currency_registry cr_amnt ON cr_amnt.ticker = cl."Amount Symbol"
                    LEFT JOIN pool_config pc ON pc.pool_id = lso."LS_loan_pool_id"
                GROUP BY
                    lso."LS_contract_id",
                    lso."LS_asset_symbol",
                    lso."LS_loan_amnt_asset",
                    pc.lpn_decimals
            )
            SELECT
                rl."Contract ID" AS contract_id,
                rl."Symbol" AS symbol,
                rl."Loan" AS loan,
                rl."Total Repaid" AS total_repaid,
                rl."Close Timestamp" AS close_timestamp,
                rl."Loan Closed" AS loan_closed
            FROM
                RepaidLeases rl
            "#,
        )
        .persistent(true)
        .fetch_all(&self.pool)
        .await?;

        Ok(data)
    }

    /// Get historically repaid positions with optional time window filter
    pub async fn get_historically_repaid_with_window(
        &self,
        months: Option<i32>,
        from: Option<DateTime<Utc>>,
    ) -> Result<Vec<HistoricallyRepaid>, crate::error::Error> {
        // Build time conditions dynamically
        let mut conditions = Vec::new();

        if let Some(m) = months {
            conditions.push(format!(
                "lso.\"LS_timestamp\" >= NOW() - INTERVAL '{} months'",
                m
            ));
        }

        if from.is_some() {
            conditions.push("lso.\"LS_timestamp\" > $1".to_string());
        }

        let time_condition = if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };

        let query = format!(
            r#"
            WITH Closed_Loans AS (
                SELECT
                    "LS_contract_id",
                    "LS_timestamp",
                    "LS_amnt_stable",
                    "LS_loan_close",
                    "LS_amnt_symbol" AS "Amount Symbol"
                FROM
                    "LS_Close_Position"
                UNION ALL
                SELECT
                    "LS_contract_id",
                    "LS_timestamp",
                    "LS_payment_amnt_stable" AS "LS_amnt_stable",
                    "LS_loan_close",
                    "LS_payment_symbol" AS "Amount Symbol"
                FROM
                    "LS_Repayment"
            ),
            RepaidLeases AS (
                SELECT
                    lso."LS_contract_id" AS "Contract ID",
                    lso."LS_asset_symbol" AS "Symbol",
                    lso."LS_loan_amnt_asset" / COALESCE(pc.lpn_decimals, 1000000)::numeric AS "Loan",
                    COALESCE(
                        SUM(cl."LS_amnt_stable" / POWER(10, COALESCE(cr_amnt.decimal_digits, 6))),
                        0
                    ) AS "Total Repaid",
                    MAX(
                        CASE
                            WHEN cl."LS_loan_close" = true THEN cl."LS_timestamp"
                        END
                    ) AS "Close Timestamp",
                    CASE
                        WHEN SUM(
                            CASE
                                WHEN cl."LS_loan_close" = true THEN 1
                                ELSE 0
                            END
                        ) > 0 THEN 'yes'
                        ELSE 'no'
                    END AS "Loan Closed"
                FROM
                    "LS_Opening" lso
                    LEFT JOIN Closed_Loans cl ON lso."LS_contract_id" = cl."LS_contract_id"
                    LEFT JOIN currency_registry cr_amnt ON cr_amnt.ticker = cl."Amount Symbol"
                    LEFT JOIN pool_config pc ON pc.pool_id = lso."LS_loan_pool_id"
                {}
                GROUP BY
                    lso."LS_contract_id",
                    lso."LS_asset_symbol",
                    lso."LS_loan_amnt_asset",
                    pc.lpn_decimals
            )
            SELECT
                rl."Contract ID" AS contract_id,
                rl."Symbol" AS symbol,
                rl."Loan" AS loan,
                rl."Total Repaid" AS total_repaid,
                rl."Close Timestamp" AS close_timestamp,
                rl."Loan Closed" AS loan_closed
            FROM
                RepaidLeases rl
            "#,
            time_condition
        );

        let mut query_builder = sqlx::query_as::<_, HistoricallyRepaid>(&query);

        if let Some(from_ts) = from {
            query_builder = query_builder.bind(from_ts);
        }

        let data = query_builder.persistent(true).fetch_all(&self.pool).await?;

        Ok(data)
    }

    pub async fn get_interest_repayments(
        &self,
        skip: i64,
        limit: i64,
        from: Option<DateTime<Utc>>,
    ) -> Result<Vec<InterestRepaymentData>, crate::error::Error> {
        // Use a very old date as default if 'from' is not provided
        let from_timestamp = from.unwrap_or_else(|| {
            DateTime::parse_from_rfc3339("1970-01-01T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc)
        });

        let data = sqlx::query_as(
            r#"
            WITH ContractInfo AS (
                SELECT
                    o."LS_contract_id",
                    o."LS_address_id" AS position_owner,
                    COALESCE(pc.position_type, 'Long') AS position_type,
                    COALESCE(pc.stable_currency_decimals, 1000000)::numeric AS stable_decimals
                FROM "LS_Opening" o
                LEFT JOIN pool_config pc ON o."LS_loan_pool_id" = pc.pool_id
            ),
            RepaymentEvents AS (
                SELECT
                    r."LS_timestamp" AS timestamp,
                    r."LS_contract_id" AS contract_id,
                    (COALESCE(r."LS_prev_interest_stable", 0) + COALESCE(r."LS_current_interest_stable", 0)) / ci.stable_decimals AS loan_interest_repaid,
                    (COALESCE(r."LS_prev_margin_stable", 0) + COALESCE(r."LS_current_margin_stable", 0)) / ci.stable_decimals AS margin_interest_repaid,
                    'repayment' AS event_type
                FROM "LS_Repayment" r
                JOIN ContractInfo ci ON ci."LS_contract_id" = r."LS_contract_id"
                WHERE r."LS_timestamp" > $3
            ),
            CloseEvents AS (
                SELECT
                    c."LS_timestamp" AS timestamp,
                    c."LS_contract_id" AS contract_id,
                    (COALESCE(c."LS_prev_interest_stable", 0) + COALESCE(c."LS_current_interest_stable", 0)) / ci.stable_decimals AS loan_interest_repaid,
                    (COALESCE(c."LS_prev_margin_stable", 0) + COALESCE(c."LS_current_margin_stable", 0)) / ci.stable_decimals AS margin_interest_repaid,
                    'close' AS event_type
                FROM "LS_Close_Position" c
                JOIN ContractInfo ci ON ci."LS_contract_id" = c."LS_contract_id"
                WHERE c."LS_timestamp" > $3
            ),
            LiquidationEvents AS (
                SELECT
                    l."LS_timestamp" AS timestamp,
                    l."LS_contract_id" AS contract_id,
                    (COALESCE(l."LS_prev_interest_stable", 0) + COALESCE(l."LS_current_interest_stable", 0)) / ci.stable_decimals AS loan_interest_repaid,
                    (COALESCE(l."LS_prev_margin_stable", 0) + COALESCE(l."LS_current_margin_stable", 0)) / ci.stable_decimals AS margin_interest_repaid,
                    'liquidation' AS event_type
                FROM "LS_Liquidation" l
                JOIN ContractInfo ci ON ci."LS_contract_id" = l."LS_contract_id"
                WHERE l."LS_timestamp" > $3
            ),
            AllEvents AS (
                SELECT * FROM RepaymentEvents
                UNION ALL
                SELECT * FROM CloseEvents
                UNION ALL
                SELECT * FROM LiquidationEvents
            )
            SELECT
                e.timestamp,
                e.contract_id,
                ci.position_owner,
                ci.position_type,
                e.event_type,
                e.loan_interest_repaid,
                e.margin_interest_repaid
            FROM AllEvents e
            JOIN ContractInfo ci ON ci."LS_contract_id" = e.contract_id
            ORDER BY e.timestamp DESC
            OFFSET $1 LIMIT $2
            "#,
        )
        .bind(skip)
        .bind(limit)
        .bind(from_timestamp)
        .persistent(true)
        .fetch_all(&self.pool)
        .await?;

        Ok(data)
    }

    /// Get interest repayments with time window filtering
    /// - months: number of months to look back (None = all time)
    /// - from: only return records after this timestamp (exclusive)
    pub async fn get_interest_repayments_with_window(
        &self,
        months: Option<i32>,
        from: Option<DateTime<Utc>>,
    ) -> Result<Vec<InterestRepaymentData>, crate::error::Error> {
        // Build time conditions
        let mut conditions = Vec::new();
        if let Some(m) = months {
            conditions
                .push(format!("timestamp >= NOW() - INTERVAL '{} months'", m));
        }
        if from.is_some() {
            conditions.push("timestamp > $1".to_string());
        }

        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };

        let query = format!(
            r#"
            WITH ContractInfo AS (
                SELECT
                    o."LS_contract_id",
                    o."LS_address_id" AS position_owner,
                    COALESCE(pc.position_type, 'Long') AS position_type,
                    COALESCE(pc.stable_currency_decimals, 1000000)::numeric AS stable_decimals
                FROM "LS_Opening" o
                LEFT JOIN pool_config pc ON o."LS_loan_pool_id" = pc.pool_id
            ),
            AllEvents AS (
                SELECT
                    r."LS_timestamp" AS timestamp,
                    r."LS_contract_id" AS contract_id,
                    (COALESCE(r."LS_prev_interest_stable", 0) + COALESCE(r."LS_current_interest_stable", 0)) / ci.stable_decimals AS loan_interest_repaid,
                    (COALESCE(r."LS_prev_margin_stable", 0) + COALESCE(r."LS_current_margin_stable", 0)) / ci.stable_decimals AS margin_interest_repaid,
                    'repayment' AS event_type
                FROM "LS_Repayment" r
                JOIN ContractInfo ci ON ci."LS_contract_id" = r."LS_contract_id"
                UNION ALL
                SELECT
                    c."LS_timestamp" AS timestamp,
                    c."LS_contract_id" AS contract_id,
                    (COALESCE(c."LS_prev_interest_stable", 0) + COALESCE(c."LS_current_interest_stable", 0)) / ci.stable_decimals AS loan_interest_repaid,
                    (COALESCE(c."LS_prev_margin_stable", 0) + COALESCE(c."LS_current_margin_stable", 0)) / ci.stable_decimals AS margin_interest_repaid,
                    'close' AS event_type
                FROM "LS_Close_Position" c
                JOIN ContractInfo ci ON ci."LS_contract_id" = c."LS_contract_id"
                UNION ALL
                SELECT
                    l."LS_timestamp" AS timestamp,
                    l."LS_contract_id" AS contract_id,
                    (COALESCE(l."LS_prev_interest_stable", 0) + COALESCE(l."LS_current_interest_stable", 0)) / ci.stable_decimals AS loan_interest_repaid,
                    (COALESCE(l."LS_prev_margin_stable", 0) + COALESCE(l."LS_current_margin_stable", 0)) / ci.stable_decimals AS margin_interest_repaid,
                    'liquidation' AS event_type
                FROM "LS_Liquidation" l
                JOIN ContractInfo ci ON ci."LS_contract_id" = l."LS_contract_id"
            ),
            FilteredEvents AS (
                SELECT * FROM AllEvents
                {}
            )
            SELECT
                e.timestamp,
                e.contract_id,
                ci.position_owner,
                ci.position_type,
                e.event_type,
                e.loan_interest_repaid,
                e.margin_interest_repaid
            FROM FilteredEvents e
            JOIN ContractInfo ci ON ci."LS_contract_id" = e.contract_id
            ORDER BY e.timestamp DESC
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
}
