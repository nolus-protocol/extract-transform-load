use super::{DataBase, QueryResult};
use crate::model::{LS_Opening, LS_State, Pnl_Over_Time, Table, TvlPoolParams};
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
        sqlx::query_as(
            r#"
              SELECT
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
                "LS_native_amnt_nolus",
                "Tx_Hash",
                "LS_loan_amnt",
                "LS_lpn_loan_amnt",
                "LS_position_type",
                "LS_lpn_symbol",
                "LS_lpn_decimals",
                "LS_opening_price",
                "LS_liquidation_price_at_open"
              FROM "LS_Opening" WHERE "LS_contract_id" NOT IN
              (
                SELECT "LS_contract_id" as "Total" FROM (
                      SELECT "LS_contract_id" FROM "LS_Closing"
                UNION ALL
                      SELECT
                          "LS_contract_id"
                      FROM "LS_Close_Position"
                      WHERE
                          "LS_loan_close" = true
                UNION ALL
                      SELECT
                          "LS_contract_id"
                      FROM "LS_Repayment"
                      WHERE
                          "LS_loan_close" = true
                UNION ALL
                      SELECT
                          "LS_contract_id"
                      FROM "LS_Liquidation"
                      WHERE
                          "LS_loan_close" = true
                ) AS combined_data GROUP BY "LS_contract_id"
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
                    s1."LS_principal_stable" / COALESCE(pc.lpn_decimals, 1000000)::numeric AS "Loan in Stables",
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
                    s."LS_principal_stable" / COALESCE(pc.lpn_decimals, 1000000)::numeric AS "Loan in Stables"
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
                    END AS "Asset Type"
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
                    AND s."LS_amnt_stable" > 0
            ),
            Lease_Value_Table AS (
                SELECT
                    op."Asset Type" AS "Token",
                    CASE
                        WHEN op."LS_asset_symbol" IN ('ALL_BTC', 'WBTC', 'CRO') THEN op."LS_amnt_stable" / 100000000
                        WHEN op."LS_asset_symbol" IN ('ALL_SOL') THEN op."LS_amnt_stable" / 1000000000
                        WHEN op."LS_asset_symbol" IN ('PICA') THEN op."LS_amnt_stable" / 1000000000000
                        WHEN op."LS_asset_symbol" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS', 'ALL_ETH') THEN op."LS_amnt_stable" / 1000000000000000000
                    ELSE op."LS_amnt_stable" / 1000000
                END AS "Lease Value"
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
                  END AS "Asset Type"
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
                  AND s."LS_amnt_stable" > 0
          ),
      Lease_Value_Table AS (
          SELECT
              op."Asset Type" AS "Token",
              CASE
                  WHEN op."LS_asset_symbol" IN ('ALL_BTC', 'WBTC', 'CRO') THEN op."LS_amnt_stable" / 100000000
                  WHEN op."LS_asset_symbol" IN ('ALL_SOL') THEN op."LS_amnt_stable" / 1000000000
                  WHEN op."LS_asset_symbol" IN ('PICA') THEN op."LS_amnt_stable" / 1000000000000
                  WHEN op."LS_asset_symbol" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS', 'ALL_ETH') THEN op."LS_amnt_stable" / 1000000000000000000
              ELSE op."LS_amnt_stable" / 1000000
          END AS "Lease Value"
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
                  END AS "Asset Type"
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
          ),
          Lease_Value_Table AS (
              SELECT
                  op."Asset Type" AS "Token",
                  CASE
                      WHEN op."LS_asset_symbol" IN ('ALL_BTC', 'WBTC', 'CRO') THEN op."Interest" / 100000000
                      WHEN op."LS_asset_symbol" IN ('ALL_SOL') THEN op."Interest" / 1000000000
                      WHEN op."LS_asset_symbol" IN ('PICA') THEN op."Interest" / 1000000000000
                      WHEN op."LS_asset_symbol" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS', 'ALL_ETH') THEN op."Interest" / 1000000000000000000
                  ELSE op."Interest" / 1000000
                  END AS "Total Interest Due"
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
        Joined_States AS (
          SELECT
            o."LS_contract_id",
            -- Lease Value
            CASE
              WHEN o."LS_asset_symbol" IN ('WBTC', 'CRO', 'ALL_BTC') THEN s."LS_amnt_stable" / 100000000.0
              WHEN o."LS_asset_symbol" = 'ALL_SOL' THEN s."LS_amnt_stable" / 1000000000.0
              WHEN o."LS_asset_symbol" = 'PICA' THEN s."LS_amnt_stable" / 1000000000000.0
              WHEN o."LS_asset_symbol" IN ('WETH','EVMOS','INJ','DYDX','DYM','CUDOS','ALL_ETH') THEN s."LS_amnt_stable" / 1000000000000000000.0
              ELSE s."LS_amnt_stable" / 1000000.0
            END AS "Lease Value",

            -- Loan (use pool_config decimals)
            CASE
              WHEN pc.lpn_symbol = 'ALL_BTC' THEN s."LS_principal_stable" / 100000000.0
              WHEN pc.lpn_symbol = 'ALL_SOL' THEN s."LS_principal_stable" / 1000000000.0
              ELSE s."LS_principal_stable" / 1000000.0
            END AS "Loan",

            -- Down Payment
            CASE
              WHEN o."LS_cltr_symbol" IN ('ALL_BTC','WBTC','CRO') THEN o."LS_cltr_amnt_stable" / 100000000.0
              WHEN o."LS_cltr_symbol" = 'ALL_SOL' THEN o."LS_cltr_amnt_stable" / 1000000000.0
              WHEN o."LS_cltr_symbol" = 'PICA' THEN o."LS_cltr_amnt_stable" / 1000000000000.0
              WHEN o."LS_cltr_symbol" IN ('WETH','EVMOS','INJ','DYDX','DYM','CUDOS','ALL_ETH') THEN o."LS_cltr_amnt_stable" / 1000000000000000000.0
              ELSE o."LS_cltr_amnt_stable" / 1000000.0
            END AS "Down Payment",

            -- Margin & Loan Interest (use pool_config decimals)
            (s."LS_prev_margin_stable" + s."LS_current_margin_stable") / COALESCE(pc.lpn_decimals, 1000000)::numeric AS "Margin Interest",
            (s."LS_prev_interest_stable" + s."LS_current_interest_stable") / COALESCE(pc.lpn_decimals, 1000000)::numeric AS "Loan Interest"
          FROM Latest_States s
          JOIN "LS_Opening" o ON s."LS_contract_id" = o."LS_contract_id"
          LEFT JOIN pool_config pc ON o."LS_loan_pool_id" = pc.pool_id
          WHERE s."LS_amnt_stable" > 0
        )
        SELECT
          SUM("Lease Value" - "Loan" - "Down Payment" - "Margin Interest" - "Loan Interest") AS "PnL"
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
            s."LS_principal_stable" / COALESCE(pc.lpn_decimals, 1000000)::numeric AS "Loan",
            CASE
              WHEN o."LS_cltr_symbol" IN ('ALL_BTC', 'WBTC', 'CRO') THEN o."LS_cltr_amnt_stable" / 100000000
              WHEN o."LS_cltr_symbol" IN ('ALL_SOL') THEN o."LS_cltr_amnt_stable" / 1000000000
              WHEN o."LS_cltr_symbol" IN ('PICA') THEN o."LS_cltr_amnt_stable" / 1000000000000
              WHEN o."LS_cltr_symbol" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS', 'ALL_ETH') THEN o."LS_cltr_amnt_stable" / 1000000000000000000
              ELSE o."LS_cltr_amnt_stable" / 1000000
            END AS "Down Payment"
          FROM "LS_State" s
          INNER JOIN "LS_Opening" o ON o."LS_contract_id" = s."LS_contract_id"
          LEFT JOIN pool_config pc ON o."LS_loan_pool_id" = pc.pool_id
          WHERE s."LS_contract_id" = '{}'
            AND s."LS_timestamp" >= NOW() - INTERVAL '24 HOURS'
        ),
        Lease_Value_Table AS (
          SELECT
            o."LS_contract_id" AS "Contract ID",
            DATE_TRUNC('hour', s."LS_timestamp") AS "Hour",
            CASE
              WHEN o."LS_asset_symbol" IN ('WBTC', 'CRO', 'ALL_BTC') THEN s."LS_amnt_stable" / 100000000
              WHEN o."LS_asset_symbol" IN ('ALL_SOL') THEN s."LS_amnt_stable" / 1000000000
              WHEN o."LS_asset_symbol" IN ('PICA') THEN s."LS_amnt_stable" / 1000000000000
              WHEN o."LS_asset_symbol" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS', 'ALL_ETH') THEN s."LS_amnt_stable" / 1000000000000000000
              ELSE s."LS_amnt_stable" / 1000000
            END AS "Lease Value",
            (s."LS_prev_margin_stable" + s."LS_current_margin_stable") / COALESCE(pc.lpn_decimals, 1000000)::numeric AS "Margin Interest",
            (s."LS_prev_interest_stable" + s."LS_current_interest_stable") / COALESCE(pc.lpn_decimals, 1000000)::numeric AS "Loan Interest"
          FROM "LS_State" s
          INNER JOIN "LS_Opening" o ON o."LS_contract_id" = s."LS_contract_id"
          LEFT JOIN pool_config pc ON o."LS_loan_pool_id" = pc.pool_id
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
            (r."LS_principal_stable" + r."LS_current_interest_stable" + r."LS_current_margin_stable" + r."LS_prev_interest_stable" + r."LS_prev_margin_stable") / COALESCE(pc.lpn_decimals, 1000000)::numeric AS "Repayment Value"
          FROM "LS_Repayment" r
          INNER JOIN "LS_Opening" o ON o."LS_contract_id" = r."LS_contract_id"
          LEFT JOIN pool_config pc ON o."LS_loan_pool_id" = pc.pool_id
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
    pub async fn get_current_unrealized_pnl_by_address(
        &self,
        address: String,
    ) -> Result<BigDecimal, Error> {
        let result: Option<(Option<BigDecimal>,)> = sqlx::query_as(
            r#"
            WITH Latest_States AS (
              SELECT DISTINCT ON (s."LS_contract_id")
                s."LS_contract_id",
                s."LS_timestamp",
                s."LS_amnt_stable",
                s."LS_principal_stable",
                s."LS_prev_margin_stable",
                s."LS_current_margin_stable",
                s."LS_prev_interest_stable",
                s."LS_current_interest_stable"
              FROM "LS_State" s
              WHERE s."LS_amnt_stable" > 0
              ORDER BY s."LS_contract_id", s."LS_timestamp" DESC
            ),
            Position_PnL AS (
              SELECT
                o."LS_address_id",
                (
                  -- Lease Value (position value)
                  CASE
                    WHEN o."LS_asset_symbol" IN ('WBTC', 'CRO', 'ALL_BTC') THEN s."LS_amnt_stable" / 100000000
                    WHEN o."LS_asset_symbol" IN ('ALL_SOL') THEN s."LS_amnt_stable" / 1000000000
                    WHEN o."LS_asset_symbol" IN ('PICA') THEN s."LS_amnt_stable" / 1000000000000
                    WHEN o."LS_asset_symbol" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS', 'ALL_ETH') THEN s."LS_amnt_stable" / 1000000000000000000
                    ELSE s."LS_amnt_stable" / 1000000
                  END
                  -- Minus Loan
                  - s."LS_principal_stable" / COALESCE(pc.lpn_decimals, 1000000)::numeric
                  -- Minus Down Payment
                  - CASE
                      WHEN o."LS_cltr_symbol" IN ('ALL_BTC', 'WBTC', 'CRO') THEN o."LS_cltr_amnt_stable" / 100000000
                      WHEN o."LS_cltr_symbol" IN ('ALL_SOL') THEN o."LS_cltr_amnt_stable" / 1000000000
                      WHEN o."LS_cltr_symbol" IN ('PICA') THEN o."LS_cltr_amnt_stable" / 1000000000000
                      WHEN o."LS_cltr_symbol" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS', 'ALL_ETH') THEN o."LS_cltr_amnt_stable" / 1000000000000000000
                      ELSE o."LS_cltr_amnt_stable" / 1000000
                    END
                  -- Minus Margin Interest
                  - (s."LS_prev_margin_stable" + s."LS_current_margin_stable") / COALESCE(pc.lpn_decimals, 1000000)::numeric
                  -- Minus Loan Interest
                  - (s."LS_prev_interest_stable" + s."LS_current_interest_stable") / COALESCE(pc.lpn_decimals, 1000000)::numeric
                  -- Minus Repayments
                  - COALESCE(rp.total_repayment, 0)
                ) AS pnl
              FROM Latest_States s
              JOIN "LS_Opening" o ON s."LS_contract_id" = o."LS_contract_id"
              LEFT JOIN pool_config pc ON o."LS_loan_pool_id" = pc.pool_id
              LEFT JOIN (
                SELECT
                  r."LS_contract_id",
                  SUM(
                    (r."LS_principal_stable" + r."LS_current_interest_stable" + r."LS_current_margin_stable" + r."LS_prev_interest_stable" + r."LS_prev_margin_stable") / COALESCE(pc2.lpn_decimals, 1000000)::numeric
                  ) AS total_repayment
                FROM "LS_Repayment" r
                JOIN "LS_Opening" o2 ON r."LS_contract_id" = o2."LS_contract_id"
                LEFT JOIN pool_config pc2 ON o2."LS_loan_pool_id" = pc2.pool_id
                GROUP BY r."LS_contract_id"
              ) rp ON s."LS_contract_id" = rp."LS_contract_id"
              WHERE o."LS_address_id" = $1
            )
            SELECT SUM(pnl) AS total_pnl
            FROM Position_PnL
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
        pools: TvlPoolParams,
    ) -> Result<BigDecimal, crate::error::Error> {
        let value: Option<(Option<BigDecimal>,)>  = sqlx::query_as(
        r#"
          WITH Latest_Aggregation AS (
            SELECT MAX("LS_timestamp") AS max_ts FROM "LS_State"
          ),
          Lease_Value_Divisor AS (
            SELECT
              "LS_asset_symbol",
              CASE
                WHEN "LS_asset_symbol" IN ('WBTC', 'ALL_BTC', 'CRO') THEN 100000000
                WHEN "LS_asset_symbol" IN ('ALL_SOL') THEN 1000000000
                WHEN "LS_asset_symbol" IN ('PICA') THEN 1000000000000
                WHEN "LS_asset_symbol" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS', 'ALL_ETH') THEN 1000000000000000000
                ELSE 1000000
              END AS "Divisor"
            FROM
              "LS_Opening"
            GROUP BY
              "LS_asset_symbol"
          ),
          Lease_Value AS (
            SELECT s."LS_amnt_stable" / d."Divisor" AS "Lease Value"
            FROM
              "LS_State" s
            LEFT JOIN "LS_Opening" o ON o."LS_contract_id" = s."LS_contract_id"
            LEFT JOIN Lease_Value_Divisor d ON o."LS_asset_symbol" = d."LS_asset_symbol"
            WHERE s."LS_timestamp" = (SELECT max_ts FROM Latest_Aggregation)
          ),
          Available_Assets_Osmosis AS (
            SELECT
              ("LP_Pool_total_value_locked_stable" - "LP_Pool_total_borrowed_stable") / 1000000 AS "Available Assets"
            FROM
              "LP_Pool_State"
            WHERE "LP_Pool_id" = $1
            ORDER BY "LP_Pool_timestamp" DESC LIMIT 1
          ),
          Available_Assets_Neutron AS (
            SELECT ("LP_Pool_total_value_locked_stable" - "LP_Pool_total_borrowed_stable") / 1000000 AS "Available Assets"
            FROM
              "LP_Pool_State"
            WHERE "LP_Pool_id" = $2
            ORDER BY "LP_Pool_timestamp" DESC LIMIT 1
          ),
          Available_Osmosis_Noble AS (
            SELECT ("LP_Pool_total_value_locked_stable" - "LP_Pool_total_borrowed_stable") / 1000000 AS "Available Assets"
            FROM
              "LP_Pool_State"
            WHERE "LP_Pool_id" = $3
            ORDER BY "LP_Pool_timestamp" DESC LIMIT 1
          ),
          Available_Neutron_Noble AS (
            SELECT ("LP_Pool_total_value_locked_stable" - "LP_Pool_total_borrowed_stable") / 1000000 AS "Available Assets"
            FROM
              "LP_Pool_State"
            WHERE "LP_Pool_id" = $4
            ORDER BY "LP_Pool_timestamp" DESC LIMIT 1
          ),
          Available_ST_ATOM_OSMOSIS AS (
            SELECT ("LP_Pool_total_value_locked_stable" - "LP_Pool_total_borrowed_stable") / 1000000 AS "Available Assets"
            FROM
              "LP_Pool_State"
            WHERE "LP_Pool_id" = $5
            ORDER BY "LP_Pool_timestamp" DESC LIMIT 1
          ),
          Available_ALL_BTC_OSMOSIS AS (
            SELECT ("LP_Pool_total_value_locked_stable" - "LP_Pool_total_borrowed_stable") / 100000000 AS "Available Assets"
            FROM
              "LP_Pool_State"
            WHERE "LP_Pool_id" = $6
            ORDER BY "LP_Pool_timestamp" DESC LIMIT 1
          ),
          Available_ALL_SOL_OSMOSIS AS (
            SELECT ("LP_Pool_total_value_locked_stable" - "LP_Pool_total_borrowed_stable") / 1000000000 AS "Available Assets"
            FROM
              "LP_Pool_State"
            WHERE "LP_Pool_id" = $7
            ORDER BY "LP_Pool_timestamp" DESC LIMIT 1
          ),
          Available_AKT_OSMOSIS AS (
            SELECT ("LP_Pool_total_value_locked_stable" - "LP_Pool_total_borrowed_stable") / 1000000 AS "Available Assets"
            FROM
              "LP_Pool_State"
            WHERE "LP_Pool_id" = $8
            ORDER BY "LP_Pool_timestamp" DESC LIMIT 1
          ),
          Lease_Value_Sum AS (
            SELECT SUM("Lease Value") AS "Total Lease Value" FROM Lease_Value
          )
          SELECT
            (SELECT "Total Lease Value" FROM Lease_Value_Sum) +
            (SELECT "Available Assets" FROM Available_Assets_Osmosis) +
            (SELECT "Available Assets" FROM Available_Assets_Neutron) +
            (SELECT "Available Assets" FROM Available_Osmosis_Noble) +
            (SELECT "Available Assets" FROM Available_Neutron_Noble) +
            (SELECT "Available Assets" FROM Available_ST_ATOM_OSMOSIS) +
            (SELECT "Available Assets" FROM Available_ALL_BTC_OSMOSIS) +
            (SELECT "Available Assets" FROM Available_ALL_SOL_OSMOSIS) +
            (SELECT "Available Assets" FROM Available_AKT_OSMOSIS) AS "TVL"
            "#,
        )
        .bind(pools.osmosis_usdc)
        .bind(pools.neutron_axelar)
        .bind(pools.osmosis_usdc_noble)
        .bind(pools.neutron_usdc_noble)
        .bind(pools.osmosis_st_atom)
        .bind(pools.osmosis_all_btc)
        .bind(pools.osmosis_all_sol)
        .bind(pools.osmosis_akt)
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
                    CASE
                        WHEN o."LS_asset_symbol" IN ('WBTC', 'CRO', 'ALL_BTC') THEN s."LS_amnt_stable" / 100000000.0
                        WHEN o."LS_asset_symbol" = 'ALL_SOL' THEN s."LS_amnt_stable" / 1000000000.0
                        WHEN o."LS_asset_symbol" = 'PICA' THEN s."LS_amnt_stable" / 1000000000000.0
                        WHEN o."LS_asset_symbol" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS', 'ALL_ETH') THEN s."LS_amnt_stable" / 1000000000000000000.0
                        ELSE s."LS_amnt_stable" / 1000000.0
                    END AS "Lease Value"
                FROM Latest_States s
                JOIN "LS_Opening" o ON s."LS_contract_id" = o."LS_contract_id"
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
                ) / 1000000.0 AS "Repayment Stable"
              FROM "LS_Repayment" r
              JOIN Latest_States ls ON ls."LS_contract_id" = r."LS_contract_id"
              GROUP BY r."LS_contract_id"
            ),
            Joined_States AS (
              SELECT
                o."LS_timestamp" AS "Time",
                o."LS_address_id" AS "User",
                o."LS_contract_id" AS "Contract ID",
                COALESCE(pc.position_type, 'Long') AS "Type",
                COALESCE(pc.lpn_symbol, o."LS_asset_symbol") AS "Symbol",
                o."LS_asset_symbol" AS "Asset",
                COALESCE(pc.lpn_decimals, 1000000)::numeric AS denom,

                -- Loan from LS_State (use pool_config decimals)
                CASE
                  WHEN pc.lpn_symbol = 'ALL_BTC' THEN s."LS_principal_stable" / 100000000.0
                  WHEN pc.lpn_symbol = 'ALL_SOL' THEN s."LS_principal_stable" / 1000000000.0
                  ELSE s."LS_principal_stable" / 1000000.0
                END AS "Loan",

                -- Down Payment from LS_Opening
                CASE
                  WHEN o."LS_cltr_symbol" IN ('ALL_BTC', 'WBTC', 'CRO') THEN o."LS_cltr_amnt_stable" / 100000000.0
                  WHEN o."LS_cltr_symbol" = 'ALL_SOL' THEN o."LS_cltr_amnt_stable" / 1000000000.0
                  WHEN o."LS_cltr_symbol" = 'PICA' THEN o."LS_cltr_amnt_stable" / 1000000000000.0
                  WHEN o."LS_cltr_symbol" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS', 'ALL_ETH') THEN o."LS_cltr_amnt_stable" / 1000000000000000000.0
                  ELSE o."LS_cltr_amnt_stable" / 1000000.0
                END AS "Down Payment",

                -- Lease Value from LS_State
                CASE
                  WHEN o."LS_asset_symbol" IN ('WBTC', 'CRO', 'ALL_BTC') THEN s."LS_amnt_stable" / 100000000.0
                  WHEN o."LS_asset_symbol" = 'ALL_SOL' THEN s."LS_amnt_stable" / 1000000000.0
                  WHEN o."LS_asset_symbol" = 'PICA' THEN s."LS_amnt_stable" / 1000000000000.0
                  WHEN o."LS_asset_symbol" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS', 'ALL_ETH') THEN s."LS_amnt_stable" / 1000000000000000000.0
                  ELSE s."LS_amnt_stable" / 1000000.0
                END AS "Lease Value",

                -- Margin & Interest from LS_State (use pool_config decimals)
                (s."LS_prev_margin_stable" + s."LS_current_margin_stable") / COALESCE(pc.lpn_decimals, 1000000)::numeric AS "Margin Interest",
                (s."LS_prev_interest_stable" + s."LS_current_interest_stable") / COALESCE(pc.lpn_decimals, 1000000)::numeric AS "Loan Interest",

                -- Loan Token Amount (use pool_config decimals)
                (s."LS_prev_margin_asset"+s."LS_prev_interest_asset"+s."LS_current_margin_asset"+s."LS_current_interest_asset"+s."LS_principal_asset") / COALESCE(pc.lpn_decimals, 1000000)::numeric AS "Loan Token Amount"

              FROM Latest_States s
              JOIN "LS_Opening" o ON s."LS_contract_id" = o."LS_contract_id"
              LEFT JOIN pool_config pc ON o."LS_loan_pool_id" = pc.pool_id
              WHERE s."LS_amnt_stable" > 0
            ),
            SymbolsInUse AS (
              SELECT DISTINCT "Symbol" AS "MP_asset_symbol"
              FROM Joined_States
            ),
            Latest_Prices AS (
              SELECT DISTINCT ON (a."MP_asset_symbol")
                a."MP_asset_symbol",
                a."MP_price_in_stable" AS "Current Price"
              FROM
                "MP_Asset" a
                INNER JOIN SymbolsInUse s ON a."MP_asset_symbol" = s."MP_asset_symbol"
              WHERE
                a."Protocol" IN ('OSMOSIS-OSMOSIS-USDC_NOBLE', 'NEUTRON-ASTROPORT-USDC_NOBLE')
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
                ) / 1000000.0 AS "Repayment Stable"
              FROM "LS_Repayment" r
              JOIN Latest_States ls ON ls."LS_contract_id" = r."LS_contract_id"
              GROUP BY r."LS_contract_id"
            ),
            Joined_States AS (
              SELECT
                o."LS_timestamp" AS "Time",
                o."LS_address_id" AS "User",
                o."LS_contract_id" AS "Contract ID",
                COALESCE(pc.position_type, 'Long') AS "Type",
                COALESCE(pc.lpn_symbol, o."LS_asset_symbol") AS "Symbol",
                o."LS_asset_symbol" AS "Asset",
                COALESCE(pc.lpn_decimals, 1000000)::numeric AS denom,

                -- Loan from LS_State (use pool_config decimals)
                CASE
                  WHEN pc.lpn_symbol = 'ALL_BTC' THEN s."LS_principal_stable" / 100000000.0
                  WHEN pc.lpn_symbol = 'ALL_SOL' THEN s."LS_principal_stable" / 1000000000.0
                  ELSE s."LS_principal_stable" / 1000000.0
                END AS "Loan",

                -- Down Payment from LS_Opening
                CASE
                  WHEN o."LS_cltr_symbol" IN ('ALL_BTC', 'WBTC', 'CRO') THEN o."LS_cltr_amnt_stable" / 100000000.0
                  WHEN o."LS_cltr_symbol" = 'ALL_SOL' THEN o."LS_cltr_amnt_stable" / 1000000000.0
                  WHEN o."LS_cltr_symbol" = 'PICA' THEN o."LS_cltr_amnt_stable" / 1000000000000.0
                  WHEN o."LS_cltr_symbol" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS', 'ALL_ETH') THEN o."LS_cltr_amnt_stable" / 1000000000000000000.0
                  ELSE o."LS_cltr_amnt_stable" / 1000000.0
                END AS "Down Payment",

                -- Lease Value from LS_State
                CASE
                  WHEN o."LS_asset_symbol" IN ('WBTC', 'CRO', 'ALL_BTC') THEN s."LS_amnt_stable" / 100000000.0
                  WHEN o."LS_asset_symbol" = 'ALL_SOL' THEN s."LS_amnt_stable" / 1000000000.0
                  WHEN o."LS_asset_symbol" = 'PICA' THEN s."LS_amnt_stable" / 1000000000000.0
                  WHEN o."LS_asset_symbol" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS', 'ALL_ETH') THEN s."LS_amnt_stable" / 1000000000000000000.0
                  ELSE s."LS_amnt_stable" / 1000000.0
                END AS "Lease Value",

                -- Margin & Interest from LS_State (use pool_config decimals)
                (s."LS_prev_margin_stable" + s."LS_current_margin_stable") / COALESCE(pc.lpn_decimals, 1000000)::numeric AS "Margin Interest",
                (s."LS_prev_interest_stable" + s."LS_current_interest_stable") / COALESCE(pc.lpn_decimals, 1000000)::numeric AS "Loan Interest",

                -- Loan Token Amount (use pool_config decimals)
                (s."LS_prev_margin_asset"+s."LS_prev_interest_asset"+s."LS_current_margin_asset"+s."LS_current_interest_asset"+s."LS_principal_asset") / COALESCE(pc.lpn_decimals, 1000000)::numeric AS "Loan Token Amount"

              FROM Latest_States s
              JOIN "LS_Opening" o ON s."LS_contract_id" = o."LS_contract_id"
              LEFT JOIN pool_config pc ON o."LS_loan_pool_id" = pc.pool_id
              WHERE s."LS_amnt_stable" > 0
            ),
            SymbolsInUse AS (
              SELECT DISTINCT "Symbol" AS "MP_asset_symbol"
              FROM Joined_States
            ),
            Latest_Prices AS (
              SELECT DISTINCT ON (a."MP_asset_symbol")
                a."MP_asset_symbol",
                a."MP_price_in_stable" AS "Current Price"
              FROM
                "MP_Asset" a
                INNER JOIN SymbolsInUse s ON a."MP_asset_symbol" = s."MP_asset_symbol"
              WHERE
                a."Protocol" IN ('OSMOSIS-OSMOSIS-USDC_NOBLE', 'NEUTRON-ASTROPORT-USDC_NOBLE')
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
