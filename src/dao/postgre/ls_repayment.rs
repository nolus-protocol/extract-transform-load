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
    pub async fn isExists(
        &self,
        ls_repayment: &LS_Repayment,
    ) -> Result<bool, crate::error::Error> {
        let (value,): (i64,) = sqlx::query_as(
            r#"
            SELECT
                COUNT(*)
            FROM "LS_Repayment"
            WHERE
                "LS_repayment_height" = $1 AND
                "LS_contract_id" = $2 AND
                "LS_timestamp" = $3
            "#,
        )
        .bind(ls_repayment.LS_repayment_height)
        .bind(&ls_repayment.LS_contract_id)
        .bind(ls_repayment.LS_timestamp)
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
                    lso."LS_loan_amnt_asset" / 1000000 AS "Loan",
                    COALESCE(
                        SUM(
                            CASE
                                WHEN cl."Amount Symbol" IN ('ALL_BTC', 'WBTC', 'CRO') THEN cl."LS_amnt_stable" / 100000000
                                WHEN cl."Amount Symbol" IN ('ALL_SOL') THEN cl."LS_amnt_stable" / 1000000000
                                WHEN cl."Amount Symbol" IN ('PICA') THEN cl."LS_amnt_stable" / 1000000000000
                                WHEN cl."Amount Symbol" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS', 'ALL_ETH') THEN cl."LS_amnt_stable" / 1000000000000000000
                                ELSE cl."LS_amnt_stable" / 1000000
                            END
                        ),
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
                GROUP BY
                    lso."LS_contract_id",
                    lso."LS_asset_symbol",
                    lso."LS_loan_amnt_asset"
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
                    lso."LS_loan_amnt_asset" / 1000000 AS "Loan",
                    COALESCE(
                        SUM(
                            CASE
                                WHEN cl."Amount Symbol" IN ('ALL_BTC', 'WBTC', 'CRO') THEN cl."LS_amnt_stable" / 100000000
                                WHEN cl."Amount Symbol" IN ('ALL_SOL') THEN cl."LS_amnt_stable" / 1000000000
                                WHEN cl."Amount Symbol" IN ('PICA') THEN cl."LS_amnt_stable" / 1000000000000
                                WHEN cl."Amount Symbol" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS', 'ALL_ETH') THEN cl."LS_amnt_stable" / 1000000000000000000
                                ELSE cl."LS_amnt_stable" / 1000000
                            END
                        ),
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
                {}
                GROUP BY
                    lso."LS_contract_id",
                    lso."LS_asset_symbol",
                    lso."LS_loan_amnt_asset"
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

        let data = query_builder.persistent(false).fetch_all(&self.pool).await?;

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
            WITH Loan_Type_Map AS (
                SELECT * FROM (VALUES
                    ('nolus1jufcaqm6657xmfltdezzz85quz92rmtd88jk5x0hq9zqseem32ysjdm990', 'Short'),
                    ('nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3', 'Short'),
                    ('nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm', 'Short'),
                    ('nolus1lxr7f5xe02jq6cce4puk6540mtu9sg36at2dms5sk69wdtzdrg9qq0t67z', 'Short'),
                    ('nolus1u0zt8x3mkver0447glfupz9lz6wnt62j70p5fhhtu3fr46gcdd9s5dz9l6', 'Short'),
                    ('nolus1py7pxw74qvlgq0n6rfz7mjrhgnls37mh87wasg89n75qt725rams8yr46t', 'Short')
                ) AS t(id, position_type)
            ),
            ContractInfo AS (
                SELECT
                    o."LS_contract_id",
                    o."LS_address_id" AS position_owner,
                    COALESCE(m.position_type, 'Long') AS position_type
                FROM "LS_Opening" o
                LEFT JOIN Loan_Type_Map m ON o."LS_loan_pool_id" = m.id
            ),
            RepaymentEvents AS (
                SELECT
                    r."LS_timestamp" AS timestamp,
                    r."LS_contract_id" AS contract_id,
                    (COALESCE(r."LS_prev_interest_stable", 0) + COALESCE(r."LS_current_interest_stable", 0)) / 1000000.0 AS loan_interest_repaid,
                    (COALESCE(r."LS_prev_margin_stable", 0) + COALESCE(r."LS_current_margin_stable", 0)) / 1000000.0 AS margin_interest_repaid,
                    'repayment' AS event_type
                FROM "LS_Repayment" r
                WHERE r."LS_timestamp" > $3
            ),
            CloseEvents AS (
                SELECT
                    c."LS_timestamp" AS timestamp,
                    c."LS_contract_id" AS contract_id,
                    (COALESCE(c."LS_prev_interest_stable", 0) + COALESCE(c."LS_current_interest_stable", 0)) / 1000000.0 AS loan_interest_repaid,
                    (COALESCE(c."LS_prev_margin_stable", 0) + COALESCE(c."LS_current_margin_stable", 0)) / 1000000.0 AS margin_interest_repaid,
                    'close' AS event_type
                FROM "LS_Close_Position" c
                WHERE c."LS_timestamp" > $3
            ),
            LiquidationEvents AS (
                SELECT
                    l."LS_timestamp" AS timestamp,
                    l."LS_contract_id" AS contract_id,
                    (COALESCE(l."LS_prev_interest_stable", 0) + COALESCE(l."LS_current_interest_stable", 0)) / 1000000.0 AS loan_interest_repaid,
                    (COALESCE(l."LS_prev_margin_stable", 0) + COALESCE(l."LS_current_margin_stable", 0)) / 1000000.0 AS margin_interest_repaid,
                    'liquidation' AS event_type
                FROM "LS_Liquidation" l
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
            conditions.push(format!("timestamp >= NOW() - INTERVAL '{} months'", m));
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
            WITH Loan_Type_Map AS (
                SELECT * FROM (VALUES
                    ('nolus1jufcaqm6657xmfltdezzz85quz92rmtd88jk5x0hq9zqseem32ysjdm990', 'Short'),
                    ('nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3', 'Short'),
                    ('nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm', 'Short'),
                    ('nolus1lxr7f5xe02jq6cce4puk6540mtu9sg36at2dms5sk69wdtzdrg9qq0t67z', 'Short'),
                    ('nolus1u0zt8x3mkver0447glfupz9lz6wnt62j70p5fhhtu3fr46gcdd9s5dz9l6', 'Short'),
                    ('nolus1py7pxw74qvlgq0n6rfz7mjrhgnls37mh87wasg89n75qt725rams8yr46t', 'Short')
                ) AS t(id, position_type)
            ),
            ContractInfo AS (
                SELECT
                    o."LS_contract_id",
                    o."LS_address_id" AS position_owner,
                    COALESCE(m.position_type, 'Long') AS position_type
                FROM "LS_Opening" o
                LEFT JOIN Loan_Type_Map m ON o."LS_loan_pool_id" = m.id
            ),
            AllEvents AS (
                SELECT
                    r."LS_timestamp" AS timestamp,
                    r."LS_contract_id" AS contract_id,
                    (COALESCE(r."LS_prev_interest_stable", 0) + COALESCE(r."LS_current_interest_stable", 0)) / 1000000.0 AS loan_interest_repaid,
                    (COALESCE(r."LS_prev_margin_stable", 0) + COALESCE(r."LS_current_margin_stable", 0)) / 1000000.0 AS margin_interest_repaid,
                    'repayment' AS event_type
                FROM "LS_Repayment" r
                UNION ALL
                SELECT
                    c."LS_timestamp" AS timestamp,
                    c."LS_contract_id" AS contract_id,
                    (COALESCE(c."LS_prev_interest_stable", 0) + COALESCE(c."LS_current_interest_stable", 0)) / 1000000.0 AS loan_interest_repaid,
                    (COALESCE(c."LS_prev_margin_stable", 0) + COALESCE(c."LS_current_margin_stable", 0)) / 1000000.0 AS margin_interest_repaid,
                    'close' AS event_type
                FROM "LS_Close_Position" c
                UNION ALL
                SELECT
                    l."LS_timestamp" AS timestamp,
                    l."LS_contract_id" AS contract_id,
                    (COALESCE(l."LS_prev_interest_stable", 0) + COALESCE(l."LS_current_interest_stable", 0)) / 1000000.0 AS loan_interest_repaid,
                    (COALESCE(l."LS_prev_margin_stable", 0) + COALESCE(l."LS_current_margin_stable", 0)) / 1000000.0 AS margin_interest_repaid,
                    'liquidation' AS event_type
                FROM "LS_Liquidation" l
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
                .persistent(false)
                .fetch_all(&self.pool)
                .await?
        } else {
            sqlx::query_as(&query)
                .persistent(false)
                .fetch_all(&self.pool)
                .await?
        };

        Ok(data)
    }
}
