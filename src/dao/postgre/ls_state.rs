use super::{DataBase, QueryResult};
use crate::model::{
    LS_Opening, LS_State, Pnl_Over_Time, Table, Unrealized_Pnl,
};
use chrono::{DateTime, Utc};
use sqlx::{types::BigDecimal, Error, QueryBuilder};
use std::str::FromStr as _;

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
                "LS_lpn_loan_amnt"
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

        let query = query_builder.build();
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
        .fetch_one(&self.pool)
        .await?;
        Ok(value)
    }

    pub async fn get_open_position_value(
        &self,
    ) -> Result<BigDecimal, crate::error::Error> {
        let value: Option<(Option<BigDecimal>,)>  = sqlx::query_as(
        r#"
          WITH LatestTimestamps AS (
          SELECT 
              "LS_contract_id", 
              MAX("LS_timestamp") AS "MaxTimestamp"
          FROM 
              "LS_State"
          WHERE
              "LS_timestamp" > (now() - INTERVAL '1 hour')
          GROUP BY 
              "LS_contract_id"
      ),
      Opened AS (
          SELECT
              s."LS_contract_id",
              s."LS_amnt_stable",
              CASE
                  WHEN lo."LS_loan_pool_id" = 'nolus1jufcaqm6657xmfltdezzz85quz92rmtd88jk5x0hq9zqseem32ysjdm990' THEN 'ST_ATOM (Short)'
                  WHEN lo."LS_loan_pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN 'ALL_BTC (Short)'
                  WHEN lo."LS_loan_pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN 'ALL_SOL (Short)'
                  WHEN lo."LS_loan_pool_id" = 'nolus1lxr7f5xe02jq6cce4puk6540mtu9sg36at2dms5sk69wdtzdrg9qq0t67z' THEN 'AKT (Short)'
                  ELSE lo."LS_asset_symbol"
              END AS "Asset Type"
          FROM
              "LS_State" s
          INNER JOIN 
              LatestTimestamps lt ON s."LS_contract_id" = lt."LS_contract_id" AND s."LS_timestamp" = lt."MaxTimestamp"
          INNER JOIN
              "LS_Opening" lo ON lo."LS_contract_id" = s."LS_contract_id"
          WHERE
              s."LS_amnt_stable" > 0
      ),
      Lease_Value_Table AS (
          SELECT
              op."Asset Type" AS "Token",
              CASE
                  WHEN "Asset Type" IN ('ALL_BTC', 'WBTC', 'CRO') THEN "LS_amnt_stable" / 100000000
                  WHEN "Asset Type" IN ('ALL_SOL') THEN "LS_amnt_stable" / 1000000000
                  WHEN "Asset Type" IN ('PICA') THEN "LS_amnt_stable" / 1000000000000
                  WHEN "Asset Type" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS') THEN "LS_amnt_stable" / 1000000000000000000
              ELSE "LS_amnt_stable" / 1000000
          END AS "Lease Value"
          FROM
              Opened op
      )
      SELECT SUM("Lease Value") FROM Lease_Value_Table
            "#,
        )
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
          WITH LatestTimestamps AS (
              SELECT 
                  "LS_contract_id", 
                  MAX("LS_timestamp") AS "MaxTimestamp"
              FROM 
                  "LS_State"
              WHERE
                  "LS_timestamp" > (now() - INTERVAL '1 hour')
              GROUP BY 
                  "LS_contract_id"
          ),
          Opened AS (
              SELECT
                  s."LS_contract_id",
                  (s."LS_prev_margin_stable" + s."LS_prev_interest_stable" + s."LS_current_margin_stable" + s."LS_current_interest_stable") AS "Interest",
                  CASE
                      WHEN lo."LS_loan_pool_id" = 'nolus1jufcaqm6657xmfltdezzz85quz92rmtd88jk5x0hq9zqseem32ysjdm990' THEN 'ST_ATOM (Short)'
                      WHEN lo."LS_loan_pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN 'ALL_BTC (Short)'
                      WHEN lo."LS_loan_pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN 'ALL_SOL (Short)'
                      WHEN lo."LS_loan_pool_id" = 'nolus1lxr7f5xe02jq6cce4puk6540mtu9sg36at2dms5sk69wdtzdrg9qq0t67z' THEN 'AKT (Short)'
                      ELSE lo."LS_asset_symbol"
                  END AS "Asset Type"
              FROM
                  "LS_State" s
              INNER JOIN 
                  LatestTimestamps lt ON s."LS_contract_id" = lt."LS_contract_id" AND s."LS_timestamp" = lt."MaxTimestamp"
              INNER JOIN
                  "LS_Opening" lo ON lo."LS_contract_id" = s."LS_contract_id"
          ),
          Lease_Value_Table AS (
              SELECT
                  op."Asset Type" AS "Token",
                  CASE
                      WHEN "Asset Type" IN ('ALL_BTC', 'WBTC', 'CRO') THEN "Interest" / 100000000
                      WHEN "Asset Type" IN ('ALL_SOL') THEN "Interest" / 1000000000
                      WHEN "Asset Type" IN ('PICA') THEN "Interest" / 1000000000000
                      WHEN "Asset Type" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS') THEN "Interest" / 1000000000000000000
                  ELSE "Interest" / 1000000
                  END AS "Total Interest Due"
              FROM
                  Opened op
          )
          SELECT SUM("Total Interest Due") FROM Lease_Value_Table
          "#,
      )
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
        WITH DP_Loan_Table AS (
        SELECT
          "LS_Opening"."LS_contract_id" as "Contract ID",
              CASE
            WHEN "LS_Opening"."LS_loan_pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN "LS_principal_stable" / 100000000
            WHEN "LS_Opening"."LS_loan_pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN "LS_principal_stable" / 1000000000
            ELSE "LS_principal_stable" / 1000000
          END AS "Loan",
          CASE
            WHEN "LS_cltr_symbol" IN ('ALL_BTC', 'WBTC', 'CRO') THEN "LS_cltr_amnt_stable" / 100000000
            WHEN "LS_cltr_symbol" IN ('ALL_SOL') THEN "LS_cltr_amnt_stable" / 1000000000
            WHEN "LS_cltr_symbol" IN ('PICA') THEN "LS_cltr_amnt_stable" / 1000000000000
            WHEN "LS_cltr_symbol" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS') THEN "LS_cltr_amnt_stable" / 1000000000000000000
            ELSE "LS_cltr_amnt_stable" / 1000000
          END AS "Down Payment"
        FROM
          (
            SELECT
              *
            FROM
              "LS_State" s1
            WHERE
              s1."LS_timestamp" >= NOW() - INTERVAL '1 hours'
          ) AS "Opened"
          LEFT JOIN "LS_Opening" ON "LS_Opening"."LS_contract_id" = "Opened"."LS_contract_id"
      ),
      Lease_Value_Table AS (
        SELECT
          "LS_Opening"."LS_contract_id" as "Contract ID",
          CASE
            WHEN "LS_asset_symbol" IN ('WBTC', 'CRO', 'ALL_BTC') THEN "LS_amnt_stable" / 100000000
            WHEN "LS_asset_symbol" IN ('ALL_SOL') THEN "LS_amnt_stable" / 1000000000
            WHEN "LS_asset_symbol" IN ('PICA') THEN "LS_amnt_stable" / 1000000000000
            WHEN "LS_asset_symbol" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS') THEN "LS_amnt_stable" / 1000000000000000000
            ELSE "LS_amnt_stable" / 1000000
          END AS "Lease Value",
      CASE
        WHEN "LS_Opening"."LS_loan_pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN ("LS_prev_margin_stable" + "LS_current_margin_stable") / 100000000
        WHEN "LS_Opening"."LS_loan_pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN ("LS_prev_margin_stable" + "LS_current_margin_stable") / 1000000000
        ELSE ("LS_prev_margin_stable" + "LS_current_margin_stable") / 1000000
      END AS "Margin Interest",

      CASE
        WHEN "LS_Opening"."LS_loan_pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN ("LS_prev_interest_stable" + "LS_current_interest_stable") / 100000000
        WHEN "LS_Opening"."LS_loan_pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN ("LS_prev_interest_stable" + "LS_current_interest_stable") / 1000000000
        ELSE ("LS_prev_interest_stable" + "LS_current_interest_stable") / 1000000
      END AS "Loan Interest"
        FROM
          (
            SELECT
              *
            FROM
              "LS_State" s1
            WHERE
              s1."LS_timestamp" = (
                SELECT
                  MAX("LS_timestamp")
                FROM
                  "LS_State" s2
                WHERE
                  s1."LS_contract_id" = s2."LS_contract_id"
                  and "LS_timestamp" > now() - INTERVAL '1 hours'
              )
            ORDER BY
              "LS_timestamp"
          ) AS "Opened"
          LEFT JOIN "LS_Opening" ON "LS_Opening"."LS_contract_id" = "Opened"."LS_contract_id"
        WHERE
          "LS_amnt_stable" > 0
      )
      SELECT
        SUM("Lease Value" - "Loan" - "Down Payment" - "Margin Interest" - "Loan Interest") AS "PnL"
      FROM
        Lease_Value_Table lvt
        LEFT JOIN DP_Loan_Table dplt ON lvt."Contract ID" = dplt."Contract ID"
        "#,
    )
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
            CASE
              WHEN o."LS_loan_pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3'
                THEN s."LS_principal_stable" / 100000000
              WHEN o."LS_loan_pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm'
                THEN s."LS_principal_stable" / 1000000000
              ELSE s."LS_principal_stable" / 1000000
            END AS "Loan",
            CASE
              WHEN o."LS_cltr_symbol" IN ('ALL_BTC', 'WBTC', 'CRO') THEN o."LS_cltr_amnt_stable" / 100000000
              WHEN o."LS_cltr_symbol" IN ('ALL_SOL') THEN o."LS_cltr_amnt_stable" / 1000000000
              WHEN o."LS_cltr_symbol" IN ('PICA') THEN o."LS_cltr_amnt_stable" / 1000000000000
              WHEN o."LS_cltr_symbol" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS') THEN o."LS_cltr_amnt_stable" / 1000000000000000000
              ELSE o."LS_cltr_amnt_stable" / 1000000
            END AS "Down Payment"
          FROM "LS_State" s
          INNER JOIN "LS_Opening" o ON o."LS_contract_id" = s."LS_contract_id"
          WHERE s."LS_contract_id" = '{}'
            AND s."LS_timestamp" >= NOW() - INTERVAL '20 days'
        ),
        Lease_Value_Table AS (
          SELECT
            o."LS_contract_id" AS "Contract ID",
            DATE_TRUNC('hour', s."LS_timestamp") AS "Hour",
            CASE
              WHEN o."LS_asset_symbol" IN ('WBTC', 'CRO', 'ALL_BTC') THEN s."LS_amnt_stable" / 100000000
              WHEN o."LS_asset_symbol" IN ('ALL_SOL') THEN s."LS_amnt_stable" / 1000000000
              WHEN o."LS_asset_symbol" IN ('PICA') THEN s."LS_amnt_stable" / 1000000000000
              WHEN o."LS_asset_symbol" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS') THEN s."LS_amnt_stable" / 1000000000000000000
              ELSE s."LS_amnt_stable" / 1000000
            END AS "Lease Value",
            CASE
              WHEN o."LS_loan_pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3'
                THEN (s."LS_prev_margin_stable" + s."LS_current_margin_stable") / 100000000
              WHEN o."LS_loan_pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm'
                THEN (s."LS_prev_margin_stable" + s."LS_current_margin_stable") / 1000000000
              ELSE (s."LS_prev_margin_stable" + s."LS_current_margin_stable") / 1000000
            END AS "Margin Interest",
            CASE
              WHEN o."LS_loan_pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3'
                THEN (s."LS_prev_interest_stable" + s."LS_current_interest_stable") / 100000000
              WHEN o."LS_loan_pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm'
                THEN (s."LS_prev_interest_stable" + s."LS_current_interest_stable") / 1000000000
              ELSE (s."LS_prev_interest_stable" + s."LS_current_interest_stable") / 1000000
            END AS "Loan Interest"
          FROM "LS_State" s
          INNER JOIN "LS_Opening" o ON o."LS_contract_id" = s."LS_contract_id"
          WHERE s."LS_contract_id" = '{}'
            AND s."LS_timestamp" >= NOW() - INTERVAL '20 days'
        ),
        Lease_Hours AS (
          SELECT DISTINCT DATE_TRUNC('hour', s."LS_timestamp") AS "Hour"
          FROM "LS_State" s
          WHERE s."LS_contract_id" = '{}'
            AND s."LS_timestamp" >= NOW() - INTERVAL '20 days'
        ),
        Normalized_Repayments AS (
          SELECT
            r."LS_contract_id" AS "Contract ID",
            DATE_TRUNC('hour', r."LS_timestamp") AS "Repayment Hour",
            CASE
              WHEN o."LS_loan_pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3'
                THEN (r."LS_principal_stable" + r."LS_current_interest_stable" + r."LS_current_margin_stable" + r."LS_prev_interest_stable" + r."LS_prev_margin_stable") / 100000000
              WHEN o."LS_loan_pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm'
                THEN (r."LS_principal_stable" + r."LS_current_interest_stable" + r."LS_current_margin_stable" + r."LS_prev_interest_stable" + r."LS_prev_margin_stable") / 1000000000
              ELSE (r."LS_principal_stable" + r."LS_current_interest_stable" + r."LS_current_margin_stable" + r."LS_prev_interest_stable" + r."LS_prev_margin_stable") / 1000000
            END AS "Repayment Value"
          FROM "LS_Repayment" r
          INNER JOIN "LS_Opening" o ON o."LS_contract_id" = r."LS_contract_id"
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
    .fetch_all(&self.pool)
  .await?;

        Ok(value)
    }

    pub async fn get_unrealized_pnl_by_address(
        &self,
        address: String,
    ) -> Result<Vec<Unrealized_Pnl>, Error> {
        let data = sqlx::query_as(
          r#"
          WITH Filtered_Opening AS (
            SELECT *
            FROM "LS_Opening"
            WHERE "LS_address_id" = $1
          ),
          Active_Positions AS (
            SELECT DISTINCT "LS_contract_id"
            FROM "LS_State"
            WHERE "LS_timestamp" >= NOW() - INTERVAL '1 hour'
          ),
          DP_Loan_Table AS (
            SELECT
              fo."LS_address_id" AS "Address ID",
              fo."LS_contract_id" AS "Contract ID",
              DATE_TRUNC('hour', fs."LS_timestamp") AS "Hour",
              SUM(
                CASE
                  WHEN fo."LS_loan_pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' 
                  THEN fs."LS_principal_stable" / 100000000
                  WHEN fo."LS_loan_pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' 
                  THEN fs."LS_principal_stable" / 1000000000
                  ELSE fs."LS_principal_stable" / 1000000
                END
              ) AS "Loan",
              SUM(
                CASE
                  WHEN fo."LS_cltr_symbol" IN ('ALL_BTC', 'WBTC', 'CRO') THEN fo."LS_cltr_amnt_stable" / 100000000
                  WHEN fo."LS_cltr_symbol" IN ('ALL_SOL') THEN fo."LS_cltr_amnt_stable" / 1000000000
                  WHEN fo."LS_cltr_symbol" IN ('PICA') THEN fo."LS_cltr_amnt_stable" / 1000000000000
                  ELSE fo."LS_cltr_amnt_stable" / 1000000
                END
              ) AS "Down Payment"
            FROM "LS_State" fs
            INNER JOIN Filtered_Opening fo ON fo."LS_contract_id" = fs."LS_contract_id"
            WHERE fs."LS_timestamp" >= NOW() - INTERVAL '20 days'
              AND fs."LS_contract_id" IN (SELECT "LS_contract_id" FROM Active_Positions)
            GROUP BY fo."LS_address_id", fo."LS_contract_id", DATE_TRUNC('hour', fs."LS_timestamp")
          ),
          Lease_Value_Table AS (
            SELECT
              fo."LS_address_id" AS "Address ID",
              fo."LS_contract_id" AS "Contract ID",
              DATE_TRUNC('hour', fs."LS_timestamp") AS "Hour",
              SUM(
                CASE
                  WHEN fo."LS_asset_symbol" IN ('WBTC', 'CRO', 'ALL_BTC') THEN fs."LS_amnt_stable" / 100000000
                  WHEN fo."LS_asset_symbol" IN ('ALL_SOL') THEN fs."LS_amnt_stable" / 1000000000
                  ELSE fs."LS_amnt_stable" / 1000000
                END
              ) AS "Lease Value",
              SUM(
                CASE
                  WHEN fo."LS_loan_pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' 
                  THEN (fs."LS_prev_margin_stable" + fs."LS_current_margin_stable") / 100000000
                  WHEN fo."LS_loan_pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' 
                  THEN (fs."LS_prev_margin_stable" + fs."LS_current_margin_stable") / 1000000000
                  ELSE (fs."LS_prev_margin_stable" + fs."LS_current_margin_stable") / 1000000
                END
              ) AS "Margin Interest",
              SUM(
                CASE
                  WHEN fo."LS_loan_pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' 
                  THEN (fs."LS_prev_interest_stable" + fs."LS_current_interest_stable") / 100000000
                  WHEN fo."LS_loan_pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' 
                  THEN (fs."LS_prev_interest_stable" + fs."LS_current_interest_stable") / 1000000000
                  ELSE (fs."LS_prev_interest_stable" + fs."LS_current_interest_stable") / 1000000
                END
              ) AS "Loan Interest"
            FROM "LS_State" fs
            INNER JOIN Filtered_Opening fo ON fo."LS_contract_id" = fs."LS_contract_id"
            WHERE fs."LS_timestamp" >= NOW() - INTERVAL '20 days'
              AND fs."LS_contract_id" IN (SELECT "LS_contract_id" FROM Active_Positions)
            GROUP BY fo."LS_address_id", fo."LS_contract_id", DATE_TRUNC('hour', fs."LS_timestamp")
          ),
          Repayment_Summary AS (
            SELECT
              fo."LS_address_id" AS "Address ID",
              r."LS_contract_id" AS "Contract ID",
              DATE_TRUNC('hour', r."LS_timestamp") AS "Hour",
              SUM(
                CASE
                  WHEN fo."LS_loan_pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' 
                  THEN (r."LS_principal_stable" + r."LS_current_interest_stable" + r."LS_current_margin_stable" + r."LS_prev_interest_stable" + r."LS_prev_margin_stable") / 100000000
                  WHEN fo."LS_loan_pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' 
                  THEN (r."LS_principal_stable" + r."LS_current_interest_stable" + r."LS_current_margin_stable" + r."LS_prev_interest_stable" + r."LS_prev_margin_stable") / 1000000000
                  ELSE (r."LS_principal_stable" + r."LS_current_interest_stable" + r."LS_current_margin_stable" + r."LS_prev_interest_stable" + r."LS_prev_margin_stable") / 1000000
                END
              ) OVER (PARTITION BY fo."LS_address_id", r."LS_contract_id" ORDER BY DATE_TRUNC('hour', r."LS_timestamp") ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
              AS "Cumulative Repayment"
            FROM "LS_Repayment" r
            INNER JOIN Filtered_Opening fo ON fo."LS_contract_id" = r."LS_contract_id"
            WHERE r."LS_contract_id" IN (SELECT "LS_contract_id" FROM Active_Positions)
          )
          SELECT DISTINCT ON (lvt."Hour")
            lvt."Hour" as "time",
            lvt."Address ID" as "address",
            (lvt."Lease Value" - dplt."Loan" - dplt."Down Payment" - lvt."Margin Interest" - lvt."Loan Interest" - COALESCE(rs."Cumulative Repayment", 0))
            AS "pnl"
          FROM Lease_Value_Table lvt
          LEFT JOIN DP_Loan_Table dplt 
            ON lvt."Contract ID" = dplt."Contract ID" 
            AND lvt."Hour" = dplt."Hour"
          LEFT JOIN Repayment_Summary rs 
            ON lvt."Contract ID" = rs."Contract ID"
            AND lvt."Hour" >= rs."Hour"
          ORDER BY lvt."Hour";
          "#,
      )
      .bind(address)
      .fetch_all(&self.pool)
      .await?;
        Ok(data)
    }

    #[cfg(feature = "mainnet")]
    pub async fn get_total_value_locked(
        &self,
        osmosis_usdc_protocol: String,
        neutron_axelar_protocol: String,
        osmosis_usdc_noble_protocol: String,
        neutron_usdc_noble_protocol: String,
        osmosis_st_atom: String,
        osmosis_all_btc: String,
        osmosis_all_sol: String,
        osmosis_akt: String,
    ) -> Result<BigDecimal, crate::error::Error> {
        let value: Option<(Option<BigDecimal>,)>  = sqlx::query_as(
        r#"
          WITH Lease_Value_Divisor AS (
            SELECT
              "LS_asset_symbol",
              CASE
                WHEN "LS_asset_symbol" IN ('WBTC', 'ALL_BTC', 'CRO') THEN 100000000
                WHEN "LS_asset_symbol" IN ('ALL_SOL') THEN 1000000000
                WHEN "LS_asset_symbol" IN ('PICA') THEN 1000000000000
                WHEN "LS_asset_symbol" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS') THEN 1000000000000000000
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
            WHERE s."LS_timestamp" > NOW() - INTERVAL '1 hours'
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
        .bind(osmosis_usdc_protocol)
        .bind(neutron_axelar_protocol)
        .bind(osmosis_usdc_noble_protocol)
        .bind(neutron_usdc_noble_protocol)
        .bind(osmosis_st_atom)
        .bind(osmosis_all_btc)
        .bind(osmosis_all_sol)
        .bind(osmosis_akt)
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

    #[cfg(feature = "testnet")]
    pub async fn get_total_value_locked(
        &self,
        osmosis_usdc_protocol: String,
        neutron_axelar_protocol: String,
    ) -> Result<BigDecimal, crate::error::Error> {
        let value: Option<(Option<BigDecimal>,)>  = sqlx::query_as(
        r#"
          WITH Lease_Value_Divisor AS (
            SELECT
              "LS_asset_symbol",
              CASE
                WHEN "LS_asset_symbol" IN ('WBTC', 'CRO') THEN 100000000
                WHEN "LS_asset_symbol" IN ('PICA') THEN 1000000000000
                WHEN "LS_asset_symbol" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS') THEN 1000000000000000000
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
            WHERE s."LS_timestamp" > NOW() - INTERVAL '1 hours'
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
          Lease_Value_Sum AS (
            SELECT SUM("Lease Value") AS "Total Lease Value" FROM Lease_Value
          )
          SELECT
            (SELECT "Total Lease Value" FROM Lease_Value_Sum) +
            (SELECT "Available Assets" FROM Available_Assets_Osmosis) +
            (SELECT "Available Assets" FROM Available_Assets_Neutron) AS "TVL"
            "#,
        )
        .bind(osmosis_usdc_protocol)
        .bind(neutron_axelar_protocol)

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
}
