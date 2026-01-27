use super::{DataBase, QueryResult};
use crate::model::{LS_Opening, LS_State, Pnl_Over_Time, Table};
use chrono::{DateTime, Utc};
use sqlx::{types::BigDecimal, Error, FromRow, QueryBuilder};
use std::str::FromStr as _;

#[derive(Debug, FromRow)]
pub struct OpenPositionsByToken {
    pub token: String,
    pub market_value: BigDecimal,
}

#[derive(Debug, FromRow)]
pub struct PositionBucket {
    pub loan_category: Option<String>,
    pub loan_count: i64,
    pub loan_size: BigDecimal,
}

#[derive(Debug, FromRow)]
pub struct LoansByToken {
    pub symbol: String,
    pub value: BigDecimal,
}

#[derive(Debug, Clone, FromRow)]
pub struct LeaseValueStats {
    pub asset: String,
    pub avg_value: BigDecimal,
    pub max_value: BigDecimal,
}

impl Table<LS_State> {
    pub async fn insert(&self, data: LS_State) -> Result<QueryResult, Error> {
        sqlx::query(
            r#"
            INSERT INTO "LS_State" (
                "LS_contract_id",
                "LS_timestamp",
                "LS_amnt_stable",
                "LS_amnt",
                "LS_prev_margin_stable",
                "LS_prev_interest_stable",
                "LS_current_margin_stable",
                "LS_current_interest_stable",
                "LS_principal_stable",
                "LS_lpn_loan_amnt",
                "LS_prev_margin_asset",
                "LS_prev_interest_asset",
                "LS_current_margin_asset",
                "LS_current_interest_asset",
                "LS_principal_asset"
            )
            VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
            "#,
        )
        .bind(&data.LS_contract_id)
        .bind(data.LS_timestamp)
        .bind(&data.LS_amnt_stable)
        .bind(&data.LS_amnt)
        .bind(&data.LS_prev_margin_stable)
        .bind(&data.LS_prev_interest_stable)
        .bind(&data.LS_current_margin_stable)
        .bind(&data.LS_current_interest_stable)
        .bind(&data.LS_principal_stable)
        .bind(&data.LS_lpn_loan_amnt)
        .persistent(true)
        .execute(&self.pool)
        .await
    }

    pub async fn get_active_states(&self) -> Result<Vec<LS_Opening>, Error> {
        // Using NOT EXISTS instead of NOT IN for better performance:
        // - NOT EXISTS can short-circuit on first match
        // - NOT IN must scan all rows and can have NULL handling issues
        // - PostgreSQL optimizer handles NOT EXISTS better with indexes
        sqlx::query_as(
            r#"
              SELECT
                o."LS_contract_id",
                o."LS_address_id",
                o."LS_asset_symbol",
                o."LS_interest",
                o."LS_timestamp",
                o."LS_loan_pool_id",
                o."LS_loan_amnt_stable",
                o."LS_loan_amnt_asset",
                o."LS_cltr_symbol",
                o."LS_cltr_amnt_stable",
                o."LS_cltr_amnt_asset",
                o."LS_native_amnt_stable",
                o."LS_native_amnt_nolus",
                o."Tx_Hash",
                o."LS_loan_amnt",
                o."LS_lpn_loan_amnt",
                o."LS_position_type",
                o."LS_lpn_symbol",
                o."LS_lpn_decimals",
                o."LS_opening_price",
                o."LS_liquidation_price_at_open"
              FROM "LS_Opening" o
              WHERE NOT EXISTS (
                  SELECT 1 FROM "LS_Closing" c
                  WHERE c."LS_contract_id" = o."LS_contract_id"
              )
              AND NOT EXISTS (
                  SELECT 1 FROM "LS_Close_Position" cp
                  WHERE cp."LS_contract_id" = o."LS_contract_id"
                  AND cp."LS_loan_close" = true
              )
              AND NOT EXISTS (
                  SELECT 1 FROM "LS_Repayment" r
                  WHERE r."LS_contract_id" = o."LS_contract_id"
                  AND r."LS_loan_close" = true
              )
              AND NOT EXISTS (
                  SELECT 1 FROM "LS_Liquidation" l
                  WHERE l."LS_contract_id" = o."LS_contract_id"
                  AND l."LS_loan_close" = true
              )
            "#,
        )
        .persistent(true)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn insert_many(&self, data: &Vec<LS_State>) -> Result<(), Error> {
        if data.is_empty() {
            return Ok(());
        }

        let mut query_builder: QueryBuilder<DataBase> = QueryBuilder::new(
            r#"
            INSERT INTO "LS_State" (
                "LS_contract_id",
                "LS_timestamp",
                "LS_amnt_stable",
                "LS_amnt",
                "LS_prev_margin_stable",
                "LS_prev_interest_stable",
                "LS_current_margin_stable",
                "LS_current_interest_stable",
                "LS_principal_stable",
                "LS_lpn_loan_amnt",
                "LS_prev_margin_asset",
                "LS_prev_interest_asset",
                "LS_current_margin_asset",
                "LS_current_interest_asset",
                "LS_principal_asset"
            )"#,
        );

        query_builder.push_values(data, |mut b, data| {
            b.push_bind(&data.LS_contract_id)
                .push_bind(data.LS_timestamp)
                .push_bind(&data.LS_amnt_stable)
                .push_bind(&data.LS_amnt)
                .push_bind(&data.LS_prev_margin_stable)
                .push_bind(&data.LS_prev_interest_stable)
                .push_bind(&data.LS_current_margin_stable)
                .push_bind(&data.LS_current_interest_stable)
                .push_bind(&data.LS_principal_stable)
                .push_bind(&data.LS_lpn_loan_amnt)
                .push_bind(&data.LS_prev_margin_asset)
                .push_bind(&data.LS_prev_interest_asset)
                .push_bind(&data.LS_current_margin_asset)
                .push_bind(&data.LS_current_interest_asset)
                .push_bind(&data.LS_principal_asset);
        });

        let query = query_builder.build().persistent(true);
        query.execute(&self.pool).await?;
        Ok(())
    }

    pub async fn count(
        &self,
        timestamp: DateTime<Utc>,
    ) -> Result<i64, crate::error::Error> {
        let (value,): (i64,) = sqlx::query_as(
            r#"
            SELECT
                COUNT(*)
            FROM "LS_State" WHERE "LS_timestamp" = $1
            "#,
        )
        .bind(timestamp)
        .persistent(true)
        .fetch_one(&self.pool)
        .await?;
        Ok(value)
    }

    pub async fn get_loans_by_token(
        &self,
    ) -> Result<Vec<LoansByToken>, crate::error::Error> {
        let data = sqlx::query_as(
            r#"
            WITH LatestAggregation AS (
                SELECT MAX("LS_timestamp") AS max_ts FROM "LS_State"
            ),
            Opened AS (
                SELECT
                    lo."LS_asset_symbol" as "Symbol",
                    s1."LS_contract_id" as "Contract ID",
                    s1."LS_principal_stable" / pc.lpn_decimals::numeric AS "Loan in Stables",
                    CASE
                        WHEN pc.position_type = 'Short' THEN pc.lpn_symbol || ' (Short)'
                        ELSE lo."LS_asset_symbol"
                    END AS "Asset Type"
                FROM
                    "LS_State" s1
                CROSS JOIN
                    LatestAggregation la
                INNER JOIN
                    "LS_Opening" lo ON lo."LS_contract_id" = s1."LS_contract_id"
                LEFT JOIN
                    pool_config pc ON lo."LS_loan_pool_id" = pc.pool_id
                WHERE
                    s1."LS_timestamp" = la.max_ts
            )
            SELECT
                o."Asset Type" AS symbol,
                SUM(o."Loan in Stables") AS value
            FROM
                Opened o
            WHERE
                o."Loan in Stables" > 0
            GROUP BY
                o."Asset Type"
            ORDER BY
                value ASC
            "#,
        )
        .persistent(true)
        .fetch_all(&self.pool)
        .await?;

        Ok(data)
    }

    pub async fn get_position_buckets(
        &self,
    ) -> Result<Vec<PositionBucket>, crate::error::Error> {
        let data = sqlx::query_as(
            r#"
            WITH LatestAggregation AS (
                SELECT MAX("LS_timestamp") AS max_ts FROM "LS_State"
            ),
            OpenedLoans AS (
                SELECT
                    s."LS_contract_id",
                    s."LS_principal_stable" / pc.lpn_decimals::numeric AS "Loan in Stables"
                FROM
                    "LS_State" s
                CROSS JOIN
                    LatestAggregation la
                INNER JOIN
                    "LS_Opening" lo ON lo."LS_contract_id" = s."LS_contract_id"
                LEFT JOIN
                    pool_config pc ON lo."LS_loan_pool_id" = pc.pool_id
                WHERE
                    s."LS_timestamp" = la.max_ts
                    AND s."LS_principal_stable" > 0
            )
            SELECT
                CASE
                    WHEN "Loan in Stables" < 1000 THEN '0-0.999k'
                    WHEN "Loan in Stables" BETWEEN 1000 AND 1999 THEN '1-1.9k'
                    WHEN "Loan in Stables" BETWEEN 2000 AND 4999 THEN '2-4.9k'
                    WHEN "Loan in Stables" BETWEEN 5000 AND 9999 THEN '5-9.9k'
                    WHEN "Loan in Stables" BETWEEN 10000 AND 14999 THEN '10-14.9k'
                    WHEN "Loan in Stables" >= 15000 THEN '15k+'
                END AS loan_category,
                COUNT("Loan in Stables") AS loan_count,
                SUM("Loan in Stables") AS loan_size
            FROM
                OpenedLoans
            GROUP BY
                loan_category
            "#,
        )
        .persistent(true)
        .fetch_all(&self.pool)
        .await?;

        Ok(data)
    }

    pub async fn get_open_positions_by_token(
        &self,
    ) -> Result<Vec<OpenPositionsByToken>, crate::error::Error> {
        let data = sqlx::query_as(
            r#"
            WITH LatestAggregation AS (
                SELECT MAX("LS_timestamp") AS max_ts FROM "LS_State"
            ),
            Opened AS (
                SELECT
                    s."LS_contract_id",
                    s."LS_amnt_stable",
                    lo."LS_asset_symbol",
                    CASE
                        WHEN pc.position_type = 'Short' THEN pc.lpn_symbol || ' (Short)'
                        ELSE lo."LS_asset_symbol"
                    END AS "Asset Type",
                    cr.decimal_digits AS asset_decimals
                FROM
                    "LS_State" s
                CROSS JOIN
                    LatestAggregation la
                INNER JOIN
                    "LS_Opening" lo ON lo."LS_contract_id" = s."LS_contract_id"
                LEFT JOIN
                    pool_config pc ON lo."LS_loan_pool_id" = pc.pool_id
                LEFT JOIN
                    currency_registry cr ON cr.ticker = lo."LS_asset_symbol"
                WHERE
                    s."LS_timestamp" = la.max_ts
                    AND s."LS_amnt_stable" > 0
            ),
            Lease_Value_Table AS (
                SELECT
                    op."Asset Type" AS "Token",
                    op."LS_amnt_stable" / POWER(10, op.asset_decimals)::NUMERIC AS "Lease Value"
                FROM
                    Opened op
            )
            SELECT
                "Token" AS token,
                SUM("Lease Value") AS market_value
            FROM
                Lease_Value_Table
            GROUP BY
                "Token"
            ORDER BY
                market_value DESC
            "#,
        )
        .persistent(true)
        .fetch_all(&self.pool)
        .await?;

        Ok(data)
    }

    pub async fn get_open_position_value(
        &self,
    ) -> Result<BigDecimal, crate::error::Error> {
        let value: Option<(Option<BigDecimal>,)>  = sqlx::query_as(
        r#"
          WITH LatestAggregation AS (
              SELECT MAX("LS_timestamp") AS max_ts FROM "LS_State"
          ),
          Opened AS (
              SELECT
                  s."LS_contract_id",
                  s."LS_amnt_stable",
                  lo."LS_asset_symbol",
                  CASE
                      WHEN pc.position_type = 'Short' THEN pc.lpn_symbol || ' (Short)'
                      ELSE lo."LS_asset_symbol"
                  END AS "Asset Type",
                  cr.decimal_digits AS asset_decimals
              FROM
                  "LS_State" s
              CROSS JOIN
                  LatestAggregation la
              INNER JOIN
                  "LS_Opening" lo ON lo."LS_contract_id" = s."LS_contract_id"
              LEFT JOIN
                  pool_config pc ON lo."LS_loan_pool_id" = pc.pool_id
              LEFT JOIN
                  currency_registry cr ON cr.ticker = lo."LS_asset_symbol"
              WHERE
                  s."LS_timestamp" = la.max_ts
                  AND s."LS_amnt_stable" > 0
          ),
      Lease_Value_Table AS (
          SELECT
              op."Asset Type" AS "Token",
              op."LS_amnt_stable" / POWER(10, op.asset_decimals)::NUMERIC AS "Lease Value"
          FROM
              Opened op
      )
      SELECT SUM("Lease Value") FROM Lease_Value_Table
            "#,
        )
        .persistent(true)
        .fetch_optional(&self.pool)
        .await?;

        let default = BigDecimal::from_str("0")?;
        let amount = if let Some(v) = value {
            v.0
        } else {
            Some(default.to_owned())
        };

        Ok(amount.unwrap_or(default.to_owned()))
    }

    pub async fn get_open_interest(
        &self,
    ) -> Result<BigDecimal, crate::error::Error> {
        let value: Option<(Option<BigDecimal>,)>  = sqlx::query_as(
      r#"
          WITH LatestAggregation AS (
              SELECT MAX("LS_timestamp") AS max_ts FROM "LS_State"
          ),
          Opened AS (
              SELECT
                  s."LS_contract_id",
                  (s."LS_prev_margin_stable" + s."LS_prev_interest_stable" + s."LS_current_margin_stable" + s."LS_current_interest_stable") AS "Interest",
                  lo."LS_asset_symbol",
                  CASE
                      WHEN pc.position_type = 'Short' THEN pc.lpn_symbol || ' (Short)'
                      ELSE lo."LS_asset_symbol"
                  END AS "Asset Type",
                  cr.decimal_digits AS asset_decimals
              FROM
                  "LS_State" s
              CROSS JOIN
                  LatestAggregation la
              INNER JOIN
                  "LS_Opening" lo ON lo."LS_contract_id" = s."LS_contract_id"
              LEFT JOIN
                  pool_config pc ON lo."LS_loan_pool_id" = pc.pool_id
              LEFT JOIN
                  currency_registry cr ON cr.ticker = lo."LS_asset_symbol"
              WHERE
                  s."LS_timestamp" = la.max_ts
          ),
          Lease_Value_Table AS (
              SELECT
                  op."Asset Type" AS "Token",
                  op."Interest" / POWER(10, op.asset_decimals)::NUMERIC AS "Total Interest Due"
              FROM
                  Opened op
          )
          SELECT SUM("Total Interest Due") FROM Lease_Value_Table
          "#,
      )
      .persistent(true)
      .fetch_optional(&self.pool)
      .await?;

        let default = BigDecimal::from_str("0")?;
        let amount = if let Some(v) = value {
            v.0
        } else {
            Some(default.to_owned())
        };

        Ok(amount.unwrap_or(default.to_owned()))
    }

    pub async fn get_unrealized_pnl(
        &self,
    ) -> Result<BigDecimal, crate::error::Error> {
        let value: Option<(Option<BigDecimal>,)>  = sqlx::query_as(
    r#"
        WITH Latest_Aggregation AS (
          SELECT MAX("LS_timestamp") AS max_ts FROM "LS_State"
        ),
        Latest_States AS (
          SELECT DISTINCT ON ("LS_contract_id") *
          FROM "LS_State"
          WHERE "LS_timestamp" = (SELECT max_ts FROM Latest_Aggregation)
          ORDER BY "LS_contract_id", "LS_timestamp" DESC
        ),
        Repayments AS (
          SELECT
            r."LS_contract_id",
            (
              SUM(
                r."LS_prev_margin_stable"
              + r."LS_prev_interest_stable"
              + r."LS_current_margin_stable"
              + r."LS_current_interest_stable"
              + r."LS_principal_stable"
              )
            ) / pc.stable_currency_decimals::numeric AS "Repayment Stable"
          FROM "LS_Repayment" r
          JOIN Latest_States ls ON ls."LS_contract_id" = r."LS_contract_id"
          LEFT JOIN "LS_Opening" o ON o."LS_contract_id" = r."LS_contract_id"
          INNER JOIN pool_config pc ON pc.pool_id = o."LS_loan_pool_id"
          GROUP BY r."LS_contract_id", pc.stable_currency_decimals
        ),
        Joined_States AS (
          SELECT
            o."LS_contract_id",
            -- Lease Value (use currency_registry for asset decimals)
            s."LS_amnt_stable" / POWER(10, cr_asset.decimal_digits)::NUMERIC AS "Lease Value",

            -- Loan (use currency_registry for lpn decimals)
            s."LS_principal_stable" / POWER(10, cr_lpn.decimal_digits)::NUMERIC AS "Loan",

            -- Down Payment (use currency_registry for collateral decimals)
            o."LS_cltr_amnt_stable" / POWER(10, cr_cltr.decimal_digits)::NUMERIC AS "Down Payment",

            -- Margin & Loan Interest (use pool_config decimals)
            (s."LS_prev_margin_stable" + s."LS_current_margin_stable") / pc.lpn_decimals::numeric AS "Margin Interest",
            (s."LS_prev_interest_stable" + s."LS_current_interest_stable") / pc.lpn_decimals::numeric AS "Loan Interest",

            -- Repayment
            COALESCE(rp."Repayment Stable", 0) AS "Repayment"
          FROM Latest_States s
          JOIN "LS_Opening" o ON s."LS_contract_id" = o."LS_contract_id"
          INNER JOIN pool_config pc ON o."LS_loan_pool_id" = pc.pool_id
          INNER JOIN currency_registry cr_asset ON cr_asset.ticker = o."LS_asset_symbol"
          INNER JOIN currency_registry cr_cltr ON cr_cltr.ticker = o."LS_cltr_symbol"
          INNER JOIN currency_registry cr_lpn ON cr_lpn.ticker = pc.lpn_symbol
          LEFT JOIN Repayments rp ON s."LS_contract_id" = rp."LS_contract_id"
          WHERE s."LS_amnt_stable" > 0
        )
        SELECT
          SUM("Lease Value" - "Loan" - "Down Payment" - "Margin Interest" - "Loan Interest" - "Repayment") AS "PnL"
        FROM Joined_States
        "#,
    )
    .persistent(true)
    .fetch_optional(&self.pool)
    .await?;

        let default = BigDecimal::from_str("0")?;
        let amount = if let Some(v) = value {
            v.0
        } else {
            Some(default.to_owned())
        };

        Ok(amount.unwrap_or(default.to_owned()))
    }

    pub async fn get_pnl_over_time(
        &self,
        contract_id: String,
        _period: i64,
    ) -> Result<Vec<Pnl_Over_Time>, Error> {
        let value  = sqlx::query_as(&format!(r#"
          WITH DP_Loan_Table AS (
          SELECT
            o."LS_contract_id" AS "Contract ID",
            DATE_TRUNC('hour', s."LS_timestamp") AS "Hour",
            s."LS_principal_stable" / pc.lpn_decimals::numeric AS "Loan",
            o."LS_cltr_amnt_stable" / POWER(10, cr_cltr.decimal_digits)::NUMERIC AS "Down Payment"
          FROM "LS_State" s
          INNER JOIN "LS_Opening" o ON o."LS_contract_id" = s."LS_contract_id"
          INNER JOIN pool_config pc ON o."LS_loan_pool_id" = pc.pool_id
          INNER JOIN currency_registry cr_cltr ON cr_cltr.ticker = o."LS_cltr_symbol"
          WHERE s."LS_contract_id" = '{}'
            AND s."LS_timestamp" >= NOW() - INTERVAL '24 HOURS'
        ),
        Lease_Value_Table AS (
          SELECT
            o."LS_contract_id" AS "Contract ID",
            DATE_TRUNC('hour', s."LS_timestamp") AS "Hour",
            s."LS_amnt_stable" / POWER(10, cr_asset.decimal_digits)::NUMERIC AS "Lease Value",
            (s."LS_prev_margin_stable" + s."LS_current_margin_stable") / pc.lpn_decimals::numeric AS "Margin Interest",
            (s."LS_prev_interest_stable" + s."LS_current_interest_stable") / pc.lpn_decimals::numeric AS "Loan Interest"
          FROM "LS_State" s
          INNER JOIN "LS_Opening" o ON o."LS_contract_id" = s."LS_contract_id"
          INNER JOIN pool_config pc ON o."LS_loan_pool_id" = pc.pool_id
          INNER JOIN currency_registry cr_asset ON cr_asset.ticker = o."LS_asset_symbol"
          WHERE s."LS_contract_id" = '{}'
            AND s."LS_timestamp" >= NOW() - INTERVAL '24 HOURS'
        ),
        Lease_Hours AS (
          SELECT DISTINCT DATE_TRUNC('hour', s."LS_timestamp") AS "Hour"
          FROM "LS_State" s
          WHERE s."LS_contract_id" = '{}'
            AND s."LS_timestamp" >= NOW() - INTERVAL '24 HOURS'
        ),
        Normalized_Repayments AS (
          SELECT
            r."LS_contract_id" AS "Contract ID",
            DATE_TRUNC('hour', r."LS_timestamp") AS "Repayment Hour",
            (r."LS_principal_stable" + r."LS_current_interest_stable" + r."LS_current_margin_stable" + r."LS_prev_interest_stable" + r."LS_prev_margin_stable") / pc.lpn_decimals::numeric AS "Repayment Value"
          FROM "LS_Repayment" r
          INNER JOIN "LS_Opening" o ON o."LS_contract_id" = r."LS_contract_id"
          INNER JOIN pool_config pc ON o."LS_loan_pool_id" = pc.pool_id
          WHERE r."LS_contract_id" = '{}'
        ),
        Repayment_Summary AS (
          SELECT
            h."Hour",
            nr."Contract ID",
            SUM(nr."Repayment Value") AS "Cumulative Repayment"
          FROM Lease_Hours h
          LEFT JOIN Normalized_Repayments nr
            ON nr."Repayment Hour" <= h."Hour"
          GROUP BY h."Hour", nr."Contract ID"
        )
        SELECT DISTINCT ON (lvt."Hour")
          lvt."Hour",
          lvt."Contract ID",
          (
            lvt."Lease Value"
            - dplt."Loan"
            - dplt."Down Payment"
            - lvt."Margin Interest"
            - lvt."Loan Interest"
            - COALESCE(rs."Cumulative Repayment", 0)
          ) AS "Hourly Unrealized PnL"
        FROM Lease_Value_Table lvt
        LEFT JOIN DP_Loan_Table dplt
          ON lvt."Contract ID" = dplt."Contract ID"
          AND lvt."Hour" = dplt."Hour"
        LEFT JOIN Repayment_Summary rs
          ON lvt."Contract ID" = rs."Contract ID"
          AND lvt."Hour" = rs."Hour"
        ORDER BY lvt."Hour";
      "#, contract_id.to_owned(), contract_id.to_owned(), contract_id.to_owned(), contract_id.to_owned()))
      .persistent(true)
    .fetch_all(&self.pool)
  .await?;

        Ok(value)
    }

    /// Get the current unrealized PnL for an address by summing PnL from all active positions.
    /// This returns a single value (the sum) instead of a time series.
    /// Filters by address early to avoid scanning the entire LS_State table.
    pub async fn get_current_unrealized_pnl_by_address(
        &self,
        address: String,
    ) -> Result<BigDecimal, Error> {
        let result: Option<(Option<BigDecimal>,)> = sqlx::query_as(
            r#"
            WITH Address_Contracts AS (
              -- First, get only the contract IDs for this address
              SELECT "LS_contract_id"
              FROM "LS_Opening"
              WHERE "LS_address_id" = $1
            ),
            Latest_Aggregation AS (
              SELECT MAX("LS_timestamp") AS max_ts FROM "LS_State"
            ),
            Latest_States AS (
              -- Only fetch states for this address's contracts
              SELECT DISTINCT ON (s."LS_contract_id")
                s."LS_contract_id",
                s."LS_amnt_stable",
                s."LS_principal_stable",
                s."LS_prev_margin_stable",
                s."LS_current_margin_stable",
                s."LS_prev_interest_stable",
                s."LS_current_interest_stable"
              FROM "LS_State" s
              WHERE s."LS_contract_id" IN (SELECT "LS_contract_id" FROM Address_Contracts)
                AND s."LS_timestamp" = (SELECT max_ts FROM Latest_Aggregation)
                AND s."LS_amnt_stable" > 0
              ORDER BY s."LS_contract_id", s."LS_timestamp" DESC
            ),
            Repayments AS (
              SELECT
                r."LS_contract_id",
                SUM(
                  r."LS_prev_margin_stable"
                  + r."LS_prev_interest_stable"
                  + r."LS_current_margin_stable"
                  + r."LS_current_interest_stable"
                  + r."LS_principal_stable"
                ) / pc.stable_currency_decimals::numeric AS total_repayment
              FROM "LS_Repayment" r
              LEFT JOIN "LS_Opening" o ON o."LS_contract_id" = r."LS_contract_id"
              INNER JOIN pool_config pc ON pc.pool_id = o."LS_loan_pool_id"
              WHERE r."LS_contract_id" IN (SELECT "LS_contract_id" FROM Address_Contracts)
              GROUP BY r."LS_contract_id", pc.stable_currency_decimals
            )
            SELECT SUM(
              -- Lease Value (use currency_registry for asset decimals)
              s."LS_amnt_stable" / POWER(10, cr_asset.decimal_digits)::NUMERIC
              -- Minus Loan (use currency_registry for lpn decimals)
              - s."LS_principal_stable" / POWER(10, cr_lpn.decimal_digits)::NUMERIC
              -- Minus Down Payment (use currency_registry for collateral decimals)
              - o."LS_cltr_amnt_stable" / POWER(10, cr_cltr.decimal_digits)::NUMERIC
              -- Minus Margin Interest
              - (s."LS_prev_margin_stable" + s."LS_current_margin_stable") / pc.lpn_decimals::numeric
              -- Minus Loan Interest
              - (s."LS_prev_interest_stable" + s."LS_current_interest_stable") / pc.lpn_decimals::numeric
              -- Minus Repayments
              - COALESCE(rp.total_repayment, 0)
            ) AS total_pnl
            FROM Latest_States s
            JOIN "LS_Opening" o ON s."LS_contract_id" = o."LS_contract_id"
            INNER JOIN pool_config pc ON o."LS_loan_pool_id" = pc.pool_id
            INNER JOIN currency_registry cr_asset ON cr_asset.ticker = o."LS_asset_symbol"
            INNER JOIN currency_registry cr_cltr ON cr_cltr.ticker = o."LS_cltr_symbol"
            INNER JOIN currency_registry cr_lpn ON cr_lpn.ticker = pc.lpn_symbol
            LEFT JOIN Repayments rp ON s."LS_contract_id" = rp."LS_contract_id"
            "#,
        )
        .bind(address)
        .persistent(true)
        .fetch_optional(&self.pool)
        .await?;

        Ok(result
            .and_then(|(pnl,)| pnl)
            .unwrap_or_else(|| BigDecimal::from(0)))
    }

    pub async fn get_total_value_locked(
        &self,
        pool_ids: Vec<String>,
    ) -> Result<BigDecimal, crate::error::Error> {
        let value: Option<(Option<BigDecimal>,)>  = sqlx::query_as(
        r#"
          WITH Latest_Aggregation AS (
            SELECT MAX("LS_timestamp") AS max_ts FROM "LS_State"
          ),
          Lease_Value AS (
            SELECT s."LS_amnt_stable" / POWER(10, cr.decimal_digits)::NUMERIC AS "Lease Value"
            FROM
              "LS_State" s
            LEFT JOIN "LS_Opening" o ON o."LS_contract_id" = s."LS_contract_id"
            INNER JOIN currency_registry cr ON cr.ticker = o."LS_asset_symbol"
            WHERE s."LS_timestamp" = (SELECT max_ts FROM Latest_Aggregation)
          ),
          Pool_Available AS (
            SELECT 
              lps."LP_Pool_id",
              (lps."LP_Pool_total_value_locked_stable" - lps."LP_Pool_total_borrowed_stable") / pc.lpn_decimals::numeric AS "Available Assets"
            FROM (
              SELECT DISTINCT ON ("LP_Pool_id") *
              FROM "LP_Pool_State"
              WHERE "LP_Pool_id" = ANY($1)
              ORDER BY "LP_Pool_id", "LP_Pool_timestamp" DESC
            ) lps
            INNER JOIN pool_config pc ON pc.pool_id = lps."LP_Pool_id"
          ),
          Lease_Value_Sum AS (
            SELECT SUM("Lease Value") AS "Total Lease Value" FROM Lease_Value
          ),
          Pool_Available_Sum AS (
            SELECT COALESCE(SUM("Available Assets"), 0) AS "Total Available" FROM Pool_Available
          )
          SELECT
            (SELECT "Total Lease Value" FROM Lease_Value_Sum) +
            (SELECT "Total Available" FROM Pool_Available_Sum) AS "TVL"
            "#,
        )
        .bind(&pool_ids)
        .persistent(true)
        .fetch_optional(&self.pool)
        .await?;

        let default = BigDecimal::from_str("0")?;
        let amount = if let Some(v) = value {
            v.0
        } else {
            Some(default.to_owned())
        };

        Ok(amount.unwrap_or(default.to_owned()))
    }

    pub async fn get_lease_value_stats(
        &self,
    ) -> Result<Vec<LeaseValueStats>, crate::error::Error> {
        let data = sqlx::query_as(
            r#"
            WITH Latest_Aggregation AS (
                SELECT MAX("LS_timestamp") AS max_ts FROM "LS_State"
            ),
            Latest_States AS (
                SELECT DISTINCT ON ("LS_contract_id") *
                FROM "LS_State"
                WHERE "LS_timestamp" = (SELECT max_ts FROM Latest_Aggregation)
                ORDER BY "LS_contract_id", "LS_timestamp" DESC
            ),
            Joined_States AS (
                SELECT
                    o."LS_asset_symbol" AS "Symbol",
                    s."LS_amnt_stable" / POWER(10, cr_asset.decimal_digits)::NUMERIC AS "Lease Value"
                FROM Latest_States s
                JOIN "LS_Opening" o ON s."LS_contract_id" = o."LS_contract_id"
                INNER JOIN currency_registry cr_asset ON cr_asset.ticker = o."LS_asset_symbol"
                WHERE s."LS_amnt_stable" > 0
            )
            SELECT
                js."Symbol" AS asset,
                AVG("Lease Value") AS avg_value,
                MAX("Lease Value") AS max_value
            FROM Joined_States js
            GROUP BY js."Symbol"
            "#,
        )
        .persistent(true)
        .fetch_all(&self.pool)
        .await?;

        Ok(data)
    }

    pub async fn get_positions(
        &self,
        skip: i64,
        limit: i64,
    ) -> Result<Vec<crate::model::Position>, Error> {
        let data = sqlx::query_as(
            r#"
            WITH Latest_Aggregation AS (
              SELECT MAX("LS_timestamp") AS max_ts FROM "LS_State"
            ),
            Latest_States AS (
              SELECT DISTINCT ON ("LS_contract_id") *
              FROM "LS_State"
              WHERE "LS_timestamp" = (SELECT max_ts FROM Latest_Aggregation)
              ORDER BY "LS_contract_id", "LS_timestamp" DESC
            ),
            Repayments AS (
              SELECT
                r."LS_contract_id",
                (
                  SUM(
                    r."LS_prev_margin_stable"
                  + r."LS_prev_interest_stable"
                  + r."LS_current_margin_stable"
                  + r."LS_current_interest_stable"
                  + r."LS_principal_stable"
                  )
                ) / pc.stable_currency_decimals::numeric AS "Repayment Stable"
              FROM "LS_Repayment" r
              JOIN Latest_States ls ON ls."LS_contract_id" = r."LS_contract_id"
              LEFT JOIN "LS_Opening" o ON o."LS_contract_id" = r."LS_contract_id"
              INNER JOIN pool_config pc ON pc.pool_id = o."LS_loan_pool_id"
              GROUP BY r."LS_contract_id", pc.stable_currency_decimals
            ),
            Joined_States AS (
              SELECT
                o."LS_timestamp" AS "Time",
                o."LS_address_id" AS "User",
                o."LS_contract_id" AS "Contract ID",
                COALESCE(pc.position_type, 'Long') AS "Type",
                COALESCE(pc.lpn_symbol, o."LS_asset_symbol") AS "Symbol",
                o."LS_asset_symbol" AS "Asset",
                pc.lpn_decimals::numeric AS denom,

                -- Loan from LS_State (use currency_registry for lpn decimals)
                s."LS_principal_stable" / POWER(10, cr_lpn.decimal_digits)::NUMERIC AS "Loan",

                -- Down Payment from LS_Opening (use currency_registry for collateral decimals)
                o."LS_cltr_amnt_stable" / POWER(10, cr_cltr.decimal_digits)::NUMERIC AS "Down Payment",

                -- Lease Value from LS_State (use currency_registry for asset decimals)
                s."LS_amnt_stable" / POWER(10, cr_asset.decimal_digits)::NUMERIC AS "Lease Value",

                -- Margin & Interest from LS_State (use pool_config decimals)
                (s."LS_prev_margin_stable" + s."LS_current_margin_stable") / pc.lpn_decimals::numeric AS "Margin Interest",
                (s."LS_prev_interest_stable" + s."LS_current_interest_stable") / pc.lpn_decimals::numeric AS "Loan Interest",

                -- Loan Token Amount (use pool_config decimals)
                (s."LS_prev_margin_asset"+s."LS_prev_interest_asset"+s."LS_current_margin_asset"+s."LS_current_interest_asset"+s."LS_principal_asset") / pc.lpn_decimals::numeric AS "Loan Token Amount"

              FROM Latest_States s
              JOIN "LS_Opening" o ON s."LS_contract_id" = o."LS_contract_id"
              INNER JOIN pool_config pc ON o."LS_loan_pool_id" = pc.pool_id
              INNER JOIN currency_registry cr_asset ON cr_asset.ticker = o."LS_asset_symbol"
              INNER JOIN currency_registry cr_cltr ON cr_cltr.ticker = o."LS_cltr_symbol"
              INNER JOIN currency_registry cr_lpn ON cr_lpn.ticker = pc.lpn_symbol
              WHERE s."LS_amnt_stable" > 0
            ),
            SymbolsInUse AS (
              SELECT DISTINCT "Symbol" AS "MP_asset_symbol"
              FROM Joined_States
            ),
            LongProtocols AS (
              SELECT protocol FROM pool_config WHERE position_type = 'Long' AND is_active = true
            ),
            Latest_Prices AS (
              SELECT DISTINCT ON (a."MP_asset_symbol")
                a."MP_asset_symbol",
                a."MP_price_in_stable" AS "Current Price"
              FROM
                "MP_Asset" a
                INNER JOIN SymbolsInUse s ON a."MP_asset_symbol" = s."MP_asset_symbol"
                INNER JOIN LongProtocols lp ON a."Protocol" = lp.protocol
              ORDER BY
                a."MP_asset_symbol", a."MP_asset_timestamp" DESC
            )
            SELECT
              TO_CHAR("Time", 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS "Date",
              "Type",
              js."Symbol",
              js."Asset",
              "Contract ID",
              "User",
              "Loan",
              "Down Payment",
              "Lease Value",
              (
                "Lease Value" - "Loan" - "Down Payment" - "Margin Interest" - "Loan Interest"
                - COALESCE(rp."Repayment Stable", 0)
              ) AS "PnL",
              ROUND((
                (
                  "Lease Value" - "Loan" - "Down Payment" - "Margin Interest" - "Loan Interest"
                  - COALESCE(rp."Repayment Stable", 0)
                ) / "Down Payment"
              ) * 100, 2) AS "PnL %",
              lp."Current Price",
              CASE
                WHEN "Type" = 'Long' THEN ROUND((("Loan" / 0.9) / "Lease Value") * lp."Current Price", 4)
                WHEN "Type" = 'Short' THEN ROUND("Lease Value" / ("Loan Token Amount" / 0.9), 4)
              END AS "Liquidation Price"
            FROM Joined_States js
            LEFT JOIN Latest_Prices lp ON js."Symbol" = lp."MP_asset_symbol"
            LEFT JOIN Repayments rp ON js."Contract ID" = rp."LS_contract_id"
            OFFSET $1 LIMIT $2
            "#,
        )
        .bind(skip)
        .bind(limit)
        .persistent(true)
        .fetch_all(&self.pool)
        .await?;

        Ok(data)
    }

    /// Get all positions without pagination - uses single cache key pattern
    /// Uses pool_config table instead of hardcoded CTE and extended timeout
    /// for background cache refresh
    pub async fn get_all_positions(
        &self,
    ) -> Result<Vec<crate::model::Position>, Error> {
        // Use a transaction with extended timeout for this expensive query
        let mut tx = self.pool.begin().await?;

        // Set statement timeout to 5 minutes (300000 ms) for this transaction only
        sqlx::query("SET LOCAL statement_timeout = '300000'")
            .execute(&mut *tx)
            .await?;

        let data = sqlx::query_as(
            r#"
            WITH Latest_Aggregation AS (
              SELECT MAX("LS_timestamp") AS max_ts FROM "LS_State"
            ),
            Latest_States AS (
              SELECT DISTINCT ON ("LS_contract_id") *
              FROM "LS_State"
              WHERE "LS_timestamp" = (SELECT max_ts FROM Latest_Aggregation)
              ORDER BY "LS_contract_id", "LS_timestamp" DESC
            ),
            Repayments AS (
              SELECT
                r."LS_contract_id",
                (
                  SUM(
                    r."LS_prev_margin_stable"
                  + r."LS_prev_interest_stable"
                  + r."LS_current_margin_stable"
                  + r."LS_current_interest_stable"
                  + r."LS_principal_stable"
                  )
                ) / pc.stable_currency_decimals::numeric AS "Repayment Stable"
              FROM "LS_Repayment" r
              JOIN Latest_States ls ON ls."LS_contract_id" = r."LS_contract_id"
              LEFT JOIN "LS_Opening" o ON o."LS_contract_id" = r."LS_contract_id"
              INNER JOIN pool_config pc ON pc.pool_id = o."LS_loan_pool_id"
              GROUP BY r."LS_contract_id", pc.stable_currency_decimals
            ),
            Joined_States AS (
              SELECT
                o."LS_timestamp" AS "Time",
                o."LS_address_id" AS "User",
                o."LS_contract_id" AS "Contract ID",
                COALESCE(pc.position_type, 'Long') AS "Type",
                COALESCE(pc.lpn_symbol, o."LS_asset_symbol") AS "Symbol",
                o."LS_asset_symbol" AS "Asset",
                pc.lpn_decimals::numeric AS denom,

                -- Loan from LS_State (use currency_registry for lpn decimals)
                s."LS_principal_stable" / POWER(10, cr_lpn.decimal_digits)::NUMERIC AS "Loan",

                -- Down Payment from LS_Opening (use currency_registry for collateral decimals)
                o."LS_cltr_amnt_stable" / POWER(10, cr_cltr.decimal_digits)::NUMERIC AS "Down Payment",

                -- Lease Value from LS_State (use currency_registry for asset decimals)
                s."LS_amnt_stable" / POWER(10, cr_asset.decimal_digits)::NUMERIC AS "Lease Value",

                -- Margin & Interest from LS_State (use pool_config decimals)
                (s."LS_prev_margin_stable" + s."LS_current_margin_stable") / pc.lpn_decimals::numeric AS "Margin Interest",
                (s."LS_prev_interest_stable" + s."LS_current_interest_stable") / pc.lpn_decimals::numeric AS "Loan Interest",

                -- Loan Token Amount (use pool_config decimals)
                (s."LS_prev_margin_asset"+s."LS_prev_interest_asset"+s."LS_current_margin_asset"+s."LS_current_interest_asset"+s."LS_principal_asset") / pc.lpn_decimals::numeric AS "Loan Token Amount"

              FROM Latest_States s
              JOIN "LS_Opening" o ON s."LS_contract_id" = o."LS_contract_id"
              INNER JOIN pool_config pc ON o."LS_loan_pool_id" = pc.pool_id
              INNER JOIN currency_registry cr_asset ON cr_asset.ticker = o."LS_asset_symbol"
              INNER JOIN currency_registry cr_cltr ON cr_cltr.ticker = o."LS_cltr_symbol"
              INNER JOIN currency_registry cr_lpn ON cr_lpn.ticker = pc.lpn_symbol
              WHERE s."LS_amnt_stable" > 0
            ),
            SymbolsInUse AS (
              SELECT DISTINCT "Symbol" AS "MP_asset_symbol"
              FROM Joined_States
            ),
            LongProtocols AS (
              SELECT protocol FROM pool_config WHERE position_type = 'Long' AND is_active = true
            ),
            Latest_Prices AS (
              SELECT DISTINCT ON (a."MP_asset_symbol")
                a."MP_asset_symbol",
                a."MP_price_in_stable" AS "Current Price"
              FROM
                "MP_Asset" a
                INNER JOIN SymbolsInUse s ON a."MP_asset_symbol" = s."MP_asset_symbol"
                INNER JOIN LongProtocols lp ON a."Protocol" = lp.protocol
              ORDER BY
                a."MP_asset_symbol", a."MP_asset_timestamp" DESC
            )
            SELECT
              TO_CHAR("Time", 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS "Date",
              "Type",
              js."Symbol",
              js."Asset",
              "Contract ID",
              "User",
              "Loan",
              "Down Payment",
              "Lease Value",
              (
                "Lease Value" - "Loan" - "Down Payment" - "Margin Interest" - "Loan Interest"
                - COALESCE(rp."Repayment Stable", 0)
              ) AS "PnL",
              ROUND((
                (
                  "Lease Value" - "Loan" - "Down Payment" - "Margin Interest" - "Loan Interest"
                  - COALESCE(rp."Repayment Stable", 0)
                ) / "Down Payment"
              ) * 100, 2) AS "PnL %",
              lp."Current Price",
              CASE
                WHEN "Type" = 'Long' THEN ROUND((("Loan" / 0.9) / "Lease Value") * lp."Current Price", 4)
                WHEN "Type" = 'Short' THEN ROUND("Lease Value" / ("Loan Token Amount" / 0.9), 4)
              END AS "Liquidation Price"
            FROM Joined_States js
            LEFT JOIN Latest_Prices lp ON js."Symbol" = lp."MP_asset_symbol"
            LEFT JOIN Repayments rp ON js."Contract ID" = rp."LS_contract_id"
            "#,
        )
        .persistent(true)
        .fetch_all(&mut *tx)
        .await?;

        tx.commit().await?;

        Ok(data)
    }
}
