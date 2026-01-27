use std::str::FromStr as _;

use chrono::{DateTime, Utc};
use sqlx::{types::BigDecimal, Error, QueryBuilder};

use crate::model::{
    LP_Pool_State, Supplied_Borrowed_Series, Table, Utilization_Level,
};

use super::{DataBase, QueryResult};

impl Table<LP_Pool_State> {
    pub async fn insert(
        &self,
        data: LP_Pool_State,
    ) -> Result<QueryResult, Error> {
        sqlx::query(
            r#"
            INSERT INTO "LP_Pool_State" (
                "LP_Pool_id",
                "LP_Pool_timestamp",
                "LP_Pool_total_value_locked_stable",
                "LP_Pool_total_value_locked_asset",
                "LP_Pool_total_issued_receipts",
                "LP_Pool_total_borrowed_stable",
                "LP_Pool_total_borrowed_asset",
                "LP_Pool_total_yield_stable",
                "LP_Pool_total_yield_asset",
                "LP_Pool_min_utilization_threshold"
            )
            VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        "#,
        )
        .bind(&data.LP_Pool_id)
        .bind(data.LP_Pool_timestamp)
        .bind(&data.LP_Pool_total_value_locked_stable)
        .bind(&data.LP_Pool_total_value_locked_asset)
        .bind(&data.LP_Pool_total_issued_receipts)
        .bind(&data.LP_Pool_total_borrowed_stable)
        .bind(&data.LP_Pool_total_borrowed_asset)
        .bind(&data.LP_Pool_total_yield_stable)
        .bind(&data.LP_Pool_total_yield_asset)
        .persistent(true)
        .execute(&self.pool)
        .await
    }

    pub async fn insert_many(
        &self,
        data: &Vec<LP_Pool_State>,
    ) -> Result<(), Error> {
        if data.is_empty() {
            return Ok(());
        }

        let mut query_builder: QueryBuilder<DataBase> = QueryBuilder::new(
            r#"
            INSERT INTO "LP_Pool_State" (
                "LP_Pool_id",
                "LP_Pool_timestamp",
                "LP_Pool_total_value_locked_stable",
                "LP_Pool_total_value_locked_asset",
                "LP_Pool_total_issued_receipts",
                "LP_Pool_total_borrowed_stable",
                "LP_Pool_total_borrowed_asset",
                "LP_Pool_total_yield_stable",
                "LP_Pool_total_yield_asset",
                "LP_Pool_min_utilization_threshold"
            )"#,
        );

        query_builder.push_values(data, |mut b, data| {
            b.push_bind(&data.LP_Pool_id)
                .push_bind(data.LP_Pool_timestamp)
                .push_bind(&data.LP_Pool_total_value_locked_stable)
                .push_bind(&data.LP_Pool_total_value_locked_asset)
                .push_bind(&data.LP_Pool_total_issued_receipts)
                .push_bind(&data.LP_Pool_total_borrowed_stable)
                .push_bind(&data.LP_Pool_total_borrowed_asset)
                .push_bind(&data.LP_Pool_total_yield_stable)
                .push_bind(&data.LP_Pool_total_yield_asset)
                .push_bind(&data.LP_Pool_min_utilization_threshold);
        });

        let query = query_builder.build().persistent(true);
        query.execute(&self.pool).await?;
        Ok(())
    }

    pub async fn get_total_value_locked_stable(
        &self,
        datetime: DateTime<Utc>,
    ) -> Result<(BigDecimal, BigDecimal, BigDecimal), crate::error::Error> {
        let value: (
            Option<BigDecimal>,
            Option<BigDecimal>,
            Option<BigDecimal>,
        ) = sqlx::query_as(
            r#"
            SELECT
                SUM("LP_Pool_total_value_locked_stable"),
                SUM("LP_Pool_total_borrowed_stable"),
                SUM("LP_Pool_total_yield_stable")
            FROM "LP_Pool_State" WHERE "LP_Pool_timestamp" = $1
            "#,
        )
        .bind(datetime)
        .persistent(true)
        .fetch_one(&self.pool)
        .await?;
        let (locked, borrowed, yield_amount) = value;
        let locked = locked.unwrap_or(BigDecimal::from_str("0")?);
        let borrowed = borrowed.unwrap_or(BigDecimal::from_str("0")?);
        let yield_amount = yield_amount.unwrap_or(BigDecimal::from_str("0")?);

        Ok((locked, borrowed, yield_amount))
    }

    pub async fn get_supplied_borrowed_series(
        &self,
        protocol: String,
    ) -> Result<Vec<Supplied_Borrowed_Series>, Error> {
        let data = sqlx::query_as(
            r#"
            SELECT
                lps."LP_Pool_timestamp",
                SUM(lps."LP_Pool_total_value_locked_stable" / pc.lpn_decimals::numeric) AS "Supplied",
                SUM(lps."LP_Pool_total_borrowed_stable" / pc.lpn_decimals::numeric) AS "Borrowed"
            FROM
                "LP_Pool_State" lps
            INNER JOIN pool_config pc ON lps."LP_Pool_id" = pc.pool_id
            WHERE lps."LP_Pool_id" = $1
            GROUP BY
                lps."LP_Pool_timestamp"
            ORDER BY
                lps."LP_Pool_timestamp" DESC
            "#,
        )
        .bind(protocol)
        .persistent(true)
        .fetch_all(&self.pool)
        .await?;
        Ok(data)
    }

    pub async fn get_supplied_borrowed_series_total(
        &self,
        protocols: Vec<String>,
    ) -> Result<Vec<Supplied_Borrowed_Series>, Error> {
        let mut params = String::from("$1");

        for i in 1..protocols.len() {
            params += &format!(", ${}", i + 1);
        }

        let query_str = format!(
            r#"
            SELECT
                lps."LP_Pool_timestamp",
                SUM(lps."LP_Pool_total_value_locked_stable" / pc.lpn_decimals::numeric) AS "Supplied",
                SUM(lps."LP_Pool_total_borrowed_stable" / pc.lpn_decimals::numeric) AS "Borrowed"
            FROM
                "LP_Pool_State" lps
            INNER JOIN pool_config pc ON lps."LP_Pool_id" = pc.pool_id
            WHERE lps."LP_Pool_id" IN ({})
            GROUP BY
                lps."LP_Pool_timestamp"
            ORDER BY
                lps."LP_Pool_timestamp" DESC
            "#,
            params
        );

        let mut query: sqlx::query::QueryAs<'_, _, _, _> =
            sqlx::query_as(&query_str).persistent(true);

        for i in protocols {
            query = query.bind(i);
        }

        let data = query.persistent(true).fetch_all(&self.pool).await?;
        Ok(data)
    }

    pub async fn get_supplied_borrowed_series_with_window(
        &self,
        protocol: String,
        months: Option<i32>,
        from: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<Vec<Supplied_Borrowed_Series>, Error> {
        let time_filter = match (months, from) {
            (_, Some(from_ts)) => format!(
                r#"AND lps."LP_Pool_timestamp" > '{}'"#,
                from_ts.format("%Y-%m-%d %H:%M:%S")
            ),
            (Some(m), None) => format!(
                r#"AND lps."LP_Pool_timestamp" > NOW() - INTERVAL '{} months'"#,
                m
            ),
            (None, None) => String::new(),
        };

        let query_str = format!(
            r#"
            SELECT
                lps."LP_Pool_timestamp",
                SUM(lps."LP_Pool_total_value_locked_stable" / pc.lpn_decimals::numeric) AS "Supplied",
                SUM(lps."LP_Pool_total_borrowed_stable" / pc.lpn_decimals::numeric) AS "Borrowed"
            FROM
                "LP_Pool_State" lps
            INNER JOIN pool_config pc ON lps."LP_Pool_id" = pc.pool_id
            WHERE lps."LP_Pool_id" = $1
            {}
            GROUP BY
                lps."LP_Pool_timestamp"
            ORDER BY
                lps."LP_Pool_timestamp" DESC
            "#,
            time_filter
        );

        let data = sqlx::query_as(&query_str)
            .bind(protocol)
            .persistent(true)
            .fetch_all(&self.pool)
            .await?;
        Ok(data)
    }

    pub async fn get_supplied_borrowed_series_total_with_window(
        &self,
        protocols: Vec<String>,
        months: Option<i32>,
        from: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<Vec<Supplied_Borrowed_Series>, Error> {
        let mut params = String::from("$1");

        for i in 1..protocols.len() {
            params += &format!(", ${}", i + 1);
        }

        let time_filter = match (months, from) {
            (_, Some(from_ts)) => format!(
                r#"AND lps."LP_Pool_timestamp" > '{}'"#,
                from_ts.format("%Y-%m-%d %H:%M:%S")
            ),
            (Some(m), None) => format!(
                r#"AND lps."LP_Pool_timestamp" > NOW() - INTERVAL '{} months'"#,
                m
            ),
            (None, None) => String::new(),
        };

        let query_str = format!(
            r#"
            SELECT
                lps."LP_Pool_timestamp",
                SUM(lps."LP_Pool_total_value_locked_stable" / pc.lpn_decimals::numeric) AS "Supplied",
                SUM(lps."LP_Pool_total_borrowed_stable" / pc.lpn_decimals::numeric) AS "Borrowed"
            FROM
                "LP_Pool_State" lps
            INNER JOIN pool_config pc ON lps."LP_Pool_id" = pc.pool_id
            WHERE lps."LP_Pool_id" IN ({})
            {}
            GROUP BY
                lps."LP_Pool_timestamp"
            ORDER BY
                lps."LP_Pool_timestamp" DESC
            "#,
            params, time_filter
        );

        let mut query: sqlx::query::QueryAs<'_, _, _, _> =
            sqlx::query_as(&query_str).persistent(true);

        for i in protocols {
            query = query.bind(i);
        }

        let data = query.persistent(true).fetch_all(&self.pool).await?;
        Ok(data)
    }

    pub async fn get_utilization_level(
        &self,
        protocol: String,
        skip: i64,
        limit: i64,
    ) -> Result<Vec<Utilization_Level>, Error> {
        let data = sqlx::query_as(
            r#"
                SELECT ("LP_Pool_total_borrowed_stable" / "LP_Pool_total_value_locked_stable")*100 as "Utilization_Level" FROM "LP_Pool_State" WHERE "LP_Pool_id" = $1 ORDER BY "LP_Pool_timestamp" DESC OFFSET $2 LIMIT $3
            "#,
        )
        .bind(protocol)
        .bind(skip)
        .bind(limit)
        .persistent(true)
        .fetch_all(&self.pool)
        .await?;
        Ok(data)
    }

    pub async fn get_utilization_level_old(
        &self,
        skip: i64,
        limit: i64,
    ) -> Result<Vec<Utilization_Level>, Error> {
        let data = sqlx::query_as(
            r#"
                SELECT ("LP_Pool_total_borrowed_stable" / "LP_Pool_total_value_locked_stable")*100 as "Utilization_Level" FROM "LP_Pool_State" ORDER BY "LP_Pool_timestamp" DESC OFFSET $1 LIMIT $2
            "#,
        )
        .bind(skip)
        .bind(limit)
        .persistent(true)
        .fetch_all(&self.pool)
        .await?;
        Ok(data)
    }

    /// Get utilization level with time window filtering
    pub async fn get_utilization_level_with_window(
        &self,
        protocol: String,
        months: Option<i32>,
        from: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<Vec<Utilization_Level>, Error> {
        // Build time conditions dynamically
        let mut conditions = vec![r#""LP_Pool_id" = $1"#.to_string()];

        if let Some(m) = months {
            conditions.push(format!(
                r#""LP_Pool_timestamp" >= NOW() - INTERVAL '{} months'"#,
                m
            ));
        }

        if from.is_some() {
            conditions.push(r#""LP_Pool_timestamp" > $2"#.to_string());
        }

        let where_clause = conditions.join(" AND ");
        let query = format!(
            r#"
            SELECT ("LP_Pool_total_borrowed_stable" / "LP_Pool_total_value_locked_stable")*100 as "Utilization_Level"
            FROM "LP_Pool_State"
            WHERE {}
            ORDER BY "LP_Pool_timestamp" DESC
            "#,
            where_clause
        );

        let mut query_builder =
            sqlx::query_as::<_, Utilization_Level>(&query).bind(&protocol);

        if let Some(from_ts) = from {
            query_builder = query_builder.bind(from_ts);
        }

        let data = query_builder.persistent(true).fetch_all(&self.pool).await?;
        Ok(data)
    }

    /// Fetch utilization levels for all protocols in a single query.
    /// Returns a HashMap mapping pool_id -> Vec<Utilization_Level>.
    pub async fn get_utilization_level_by_protocols(
        &self,
        months: Option<i32>,
    ) -> Result<std::collections::HashMap<String, Vec<Utilization_Level>>, Error>
    {
        let time_condition = if let Some(m) = months {
            format!(
                r#"WHERE "LP_Pool_timestamp" >= NOW() - INTERVAL '{} months'"#,
                m
            )
        } else {
            String::new()
        };

        let query = format!(
            r#"
            SELECT
                "LP_Pool_id" AS pool_id,
                ("LP_Pool_total_borrowed_stable" / "LP_Pool_total_value_locked_stable")*100 as "Utilization_Level"
            FROM "LP_Pool_State"
            {}
            ORDER BY "LP_Pool_id", "LP_Pool_timestamp" DESC
            "#,
            time_condition
        );

        #[derive(sqlx::FromRow)]
        struct UtilizationRow {
            pool_id: String,
            #[sqlx(rename = "Utilization_Level")]
            utilization_level: BigDecimal,
        }

        let rows: Vec<UtilizationRow> = sqlx::query_as(&query)
            .persistent(true)
            .fetch_all(&self.pool)
            .await?;

        let mut result: std::collections::HashMap<
            String,
            Vec<Utilization_Level>,
        > = std::collections::HashMap::new();
        for row in rows {
            result
                .entry(row.pool_id)
                .or_default()
                .push(Utilization_Level {
                    utilization_level: row.utilization_level,
                });
        }
        Ok(result)
    }

    pub async fn get_supplied_funds(
        &self,
    ) -> Result<BigDecimal, crate::error::Error> {
        let value: (Option<BigDecimal>,) = sqlx::query_as(
            r#"
              WITH Latest_Pool_Data AS (
                SELECT
                    lps."LP_Pool_id",
                    lps."LP_Pool_total_value_locked_stable",
                    pc.lpn_decimals,
                    RANK() OVER (PARTITION BY lps."LP_Pool_id" ORDER BY lps."LP_Pool_timestamp" DESC) AS rank
                FROM "LP_Pool_State" lps
                INNER JOIN pool_config pc ON lps."LP_Pool_id" = pc.pool_id
            )
            SELECT
                SUM("LP_Pool_total_value_locked_stable" / lpn_decimals::numeric) AS "Total Supplied"
            FROM Latest_Pool_Data
            WHERE rank = 1
            "#,
        )
        .persistent(true)
        .fetch_one(&self.pool)
        .await?;
        let (amnt,) = value;
        let amnt = amnt.unwrap_or(BigDecimal::from_str("0")?);

        Ok(amnt)
    }

    pub async fn get_earnings(
        &self,
        address: String,
    ) -> Result<BigDecimal, crate::error::Error> {
        let value: (Option<BigDecimal>,) = sqlx::query_as(
            r#"
            WITH 
            -- Get all active pools with their config
            ActivePools AS (
                SELECT pool_id, lpn_symbol, lpn_decimals, position_type, protocol
                FROM pool_config
                WHERE is_active = true
            ),
            -- Get latest prices for Short position assets from Long protocols
            LongProtocols AS (
                SELECT protocol FROM pool_config WHERE position_type = 'Long' AND is_active = true
            ),
            LatestPrices AS (
                SELECT DISTINCT ON (a."MP_asset_symbol")
                    a."MP_asset_symbol",
                    a."MP_price_in_stable"
                FROM "MP_Asset" a
                INNER JOIN LongProtocols lp ON a."Protocol" = lp.protocol
                ORDER BY a."MP_asset_symbol", a."MP_asset_timestamp" DESC
            ),
            -- Get latest lender state per pool for this address
            LatestLenderState AS (
                SELECT DISTINCT ON (ls."LP_Pool_id")
                    ls."LP_Pool_id",
                    ls."LP_Lender_id",
                    ls."LP_Lender_stable",
                    ls."LP_Lender_asset",
                    ls."LP_timestamp"
                FROM "LP_Lender_State" ls
                WHERE ls."LP_Lender_id" = $1
                ORDER BY ls."LP_Pool_id", ls."LP_timestamp" DESC
            ),
            -- Calculate deposits per pool up to the lender state timestamp
            Deposits AS (
                SELECT 
                    d."LP_Pool_id",
                    COALESCE(SUM(d."LP_amnt_stable"), 0)::numeric AS deposited_stable,
                    COALESCE(SUM(d."LP_amnt_asset"), 0)::numeric AS deposited_asset
                FROM "LP_Deposit" d
                INNER JOIN LatestLenderState ls ON d."LP_Pool_id" = ls."LP_Pool_id"
                WHERE d."LP_address_id" = $1
                AND d."LP_timestamp" <= ls."LP_timestamp"
                GROUP BY d."LP_Pool_id"
            ),
            -- Calculate withdrawals per pool up to the lender state timestamp
            Withdrawals AS (
                SELECT 
                    w."LP_Pool_id",
                    COALESCE(SUM(w."LP_amnt_stable"), 0)::numeric AS withdrawn_stable,
                    COALESCE(SUM(w."LP_amnt_asset"), 0)::numeric AS withdrawn_asset
                FROM "LP_Withdraw" w
                INNER JOIN LatestLenderState ls ON w."LP_Pool_id" = ls."LP_Pool_id"
                WHERE w."LP_address_id" = $1
                AND w."LP_timestamp" <= ls."LP_timestamp"
                GROUP BY w."LP_Pool_id"
            ),
            -- Calculate earnings per pool
            PoolEarnings AS (
                SELECT
                    ap.pool_id,
                    ap.position_type,
                    CASE 
                        -- Long pools (USDC-based): use stable values directly
                        WHEN ap.position_type = 'Long' THEN
                            (ls."LP_Lender_stable"::numeric - (COALESCE(dep.deposited_stable, 0) - COALESCE(wdr.withdrawn_stable, 0))) 
                            / ap.lpn_decimals::numeric
                        -- Short pools (asset-based): convert to stable using price
                        WHEN ap.position_type = 'Short' THEN
                            (ls."LP_Lender_asset"::numeric - (COALESCE(dep.deposited_asset, 0) - COALESCE(wdr.withdrawn_asset, 0))) 
                            / ap.lpn_decimals::numeric 
                            * COALESCE(p."MP_price_in_stable", 0)
                        ELSE 0
                    END AS earnings_in_stable
                FROM ActivePools ap
                INNER JOIN LatestLenderState ls ON ap.pool_id = ls."LP_Pool_id"
                LEFT JOIN Deposits dep ON ap.pool_id = dep."LP_Pool_id"
                LEFT JOIN Withdrawals wdr ON ap.pool_id = wdr."LP_Pool_id"
                LEFT JOIN LatestPrices p ON ap.lpn_symbol = p."MP_asset_symbol"
            )
            SELECT
                COALESCE(SUM(GREATEST(earnings_in_stable, 0)), 0)::numeric AS total_earnings_in_stable
            FROM PoolEarnings
            "#,
        )
        .persistent(true)
        .bind(address)
        .fetch_one(&self.pool)
        .await?;
        let (amnt,) = value;
        let amnt = amnt.unwrap_or(BigDecimal::from_str("0")?);

        Ok(amnt)
    }

    pub async fn get_by_date(
        &self,
        protocol: String,
        date_time: &DateTime<Utc>,
    ) -> Result<LP_Pool_State, Error> {
        sqlx::query_as(
            r#"
                    SELECT *
                    FROM "LP_Pool_State"
                    WHERE
                        "LP_Pool_id" = $1
                        AND
                        "LP_Pool_timestamp" >= $2

                    ORDER BY "LP_Pool_timestamp" ASC LIMIT 1
                    "#,
        )
        .bind(protocol)
        .bind(date_time)
        .persistent(true)
        .fetch_one(&self.pool)
        .await
    }

    /// Get utilization levels for all pools in a single query
    /// Returns the latest utilization level for each pool including borrow APR and earn APR
    pub async fn get_all_utilization_levels(
        &self,
    ) -> Result<Vec<PoolUtilizationLevel>, Error> {
        let data = sqlx::query_as(
            r#"
            WITH Latest_Pool_Aggregation AS (
                SELECT MAX("LP_Pool_timestamp") AS max_ts FROM "LP_Pool_State"
            ),
            Latest_LS_Aggregation AS (
                SELECT MAX("LS_timestamp") AS max_ts FROM "LS_State"
            ),
            LatestStates AS (
                SELECT DISTINCT ON (lps."LP_Pool_id")
                    lps."LP_Pool_id",
                    lps."LP_Pool_total_value_locked_stable",
                    lps."LP_Pool_total_borrowed_stable",
                    lps."LP_Pool_timestamp",
                    pc.lpn_decimals,
                    pc.protocol
                FROM "LP_Pool_State" lps
                INNER JOIN pool_config pc ON lps."LP_Pool_id" = pc.pool_id
                WHERE lps."LP_Pool_timestamp" = (SELECT max_ts FROM Latest_Pool_Aggregation)
                ORDER BY lps."LP_Pool_id", lps."LP_Pool_timestamp" DESC
            ),
            LatestBorrowAPR AS (
                SELECT DISTINCT ON ("LS_loan_pool_id")
                    "LS_loan_pool_id",
                    "LS_interest" / 10.0 AS borrow_apr
                FROM "LS_Opening"
                ORDER BY "LS_loan_pool_id", "LS_timestamp" DESC
            ),
            PoolUtilization AS (
                SELECT
                    lps."LP_Pool_id",
                    CASE
                        WHEN lps."LP_Pool_total_value_locked_stable" > 0
                        THEN lps."LP_Pool_total_borrowed_stable"::numeric / lps."LP_Pool_total_value_locked_stable"::numeric
                        ELSE 0
                    END AS utilization_rate
                FROM "LP_Pool_State" lps
                WHERE lps."LP_Pool_timestamp" = (SELECT max_ts FROM Latest_Pool_Aggregation)
            ),
            AvgInterestPerPool AS (
                SELECT
                    o."LS_loan_pool_id",
                    AVG(o."LS_interest") / 10.0 AS avg_interest
                FROM "LS_State" s
                CROSS JOIN Latest_LS_Aggregation la
                INNER JOIN "LS_Opening" o ON s."LS_contract_id" = o."LS_contract_id"
                WHERE s."LS_timestamp" = la.max_ts
                GROUP BY o."LS_loan_pool_id"
            ),
            EarnAPRCalc AS (
                SELECT
                    pc.pool_id,
                    (COALESCE(ai.avg_interest, 0) - CASE
                        WHEN pc.lpn_symbol IN ('ALL_BTC', 'ATOM') THEN 2.5
                        WHEN pc.lpn_symbol = 'ALL_SOL' THEN 4.0
                        WHEN pc.lpn_symbol IN ('ST_ATOM', 'AKT') THEN 2.0
                        ELSE 4.0
                    END) * COALESCE(pu.utilization_rate, 0) AS apr_simple
                FROM pool_config pc
                LEFT JOIN AvgInterestPerPool ai ON pc.pool_id = ai."LS_loan_pool_id"
                LEFT JOIN PoolUtilization pu ON pc.pool_id = pu."LP_Pool_id"
            )
            SELECT
                COALESCE(ls.protocol, ls."LP_Pool_id") AS protocol,
                CASE
                    WHEN ls."LP_Pool_total_value_locked_stable" > 0
                    THEN (ls."LP_Pool_total_borrowed_stable"::numeric / ls."LP_Pool_total_value_locked_stable"::numeric) * 100
                    ELSE 0
                END AS utilization,
                ls."LP_Pool_total_value_locked_stable" / ls.lpn_decimals::numeric AS supplied,
                ls."LP_Pool_total_borrowed_stable" / ls.lpn_decimals::numeric AS borrowed,
                COALESCE(apr.borrow_apr, 0) AS borrow_apr,
                CASE
                    WHEN ea.apr_simple IS NOT NULL AND ea.apr_simple > 0
                    THEN (POWER((1 + (ea.apr_simple / 100 / 365)), 365) - 1) * 100
                    ELSE 0
                END AS earn_apr
            FROM LatestStates ls
            LEFT JOIN LatestBorrowAPR apr ON ls."LP_Pool_id" = apr."LS_loan_pool_id"
            LEFT JOIN EarnAPRCalc ea ON ls."LP_Pool_id" = ea.pool_id
            WHERE ls.protocol IS NOT NULL
            ORDER BY ls.protocol
            "#,
        )
        .persistent(true)
        .fetch_all(&self.pool)
        .await?;

        Ok(data)
    }
}

/// Represents utilization level data for a single pool
#[derive(Debug, Clone, sqlx::FromRow, serde::Serialize)]
pub struct PoolUtilizationLevel {
    pub protocol: String,
    pub utilization: BigDecimal,
    pub supplied: BigDecimal,
    pub borrowed: BigDecimal,
    pub borrow_apr: BigDecimal,
    pub earn_apr: BigDecimal,
}
