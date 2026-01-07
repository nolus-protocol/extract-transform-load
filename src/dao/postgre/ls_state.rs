use super::{DataBase, QueryResult};
use crate::model::{
    LS_Opening, LS_State, Pnl_Over_Time, Table, TvlPoolParams, Unrealized_Pnl,
};
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
            WITH MaxTimestamps AS (
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
                    lo."LS_asset_symbol" as "Symbol",
                    s1."LS_contract_id" as "Contract ID",
                    CASE
                        WHEN lo."LS_loan_pool_id" = 'nolus1jufcaqm6657xmfltdezzz85quz92rmtd88jk5x0hq9zqseem32ysjdm990' THEN s1."LS_principal_stable" / 1000000
                        WHEN lo."LS_loan_pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN s1."LS_principal_stable" / 100000000
                        WHEN lo."LS_loan_pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN s1."LS_principal_stable" / 1000000000
                        WHEN lo."LS_loan_pool_id" = 'nolus1lxr7f5xe02jq6cce4puk6540mtu9sg36at2dms5sk69wdtzdrg9qq0t67z' THEN s1."LS_principal_stable" / 1000000
                        ELSE s1."LS_principal_stable" / 1000000
                    END AS "Loan in Stables",
                    CASE
                        WHEN lo."LS_loan_pool_id" = 'nolus1jufcaqm6657xmfltdezzz85quz92rmtd88jk5x0hq9zqseem32ysjdm990' THEN 'ST_ATOM (Short)'
                        WHEN lo."LS_loan_pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN 'ALL_BTC (Short)'
                        WHEN lo."LS_loan_pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN 'ALL_SOL (Short)'
                        WHEN lo."LS_loan_pool_id" = 'nolus1lxr7f5xe02jq6cce4puk6540mtu9sg36at2dms5sk69wdtzdrg9qq0t67z' THEN 'AKT (Short)'
                        WHEN lo."LS_loan_pool_id" = 'nolus1u0zt8x3mkver0447glfupz9lz6wnt62j70p5fhhtu3fr46gcdd9s5dz9l6' THEN 'ATOM (Short)'
                        WHEN lo."LS_loan_pool_id" = 'nolus1py7pxw74qvlgq0n6rfz7mjrhgnls37mh87wasg89n75qt725rams8yr46t' THEN 'OSMO (Short)'
                        ELSE lo."LS_asset_symbol"
                    END AS "Asset Type"
                FROM
                    "LS_State" s1
                INNER JOIN
                    MaxTimestamps mt ON s1."LS_contract_id" = mt."LS_contract_id" AND s1."LS_timestamp" = mt."MaxTimestamp"
                INNER JOIN
                    "LS_Opening" lo ON lo."LS_contract_id" = s1."LS_contract_id"
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
            WITH LatestStates AS (
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
            OpenedLoans AS (
                SELECT
                    s."LS_contract_id",
                    CASE
                        WHEN lo."LS_loan_pool_id" = 'nolus1jufcaqm6657xmfltdezzz85quz92rmtd88jk5x0hq9zqseem32ysjdm990' THEN s."LS_principal_stable" / 1000000
                        WHEN lo."LS_loan_pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN s."LS_principal_stable" / 100000000
                        WHEN lo."LS_loan_pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN s."LS_principal_stable" / 1000000000
                        WHEN lo."LS_loan_pool_id" = 'nolus1lxr7f5xe02jq6cce4puk6540mtu9sg36at2dms5sk69wdtzdrg9qq0t67z' THEN s."LS_principal_stable" / 1000000
                        ELSE s."LS_principal_stable" / 1000000
                    END AS "Loan in Stables"
                FROM
                    LatestStates ls
                INNER JOIN
                    "LS_State" s ON ls."LS_contract_id" = s."LS_contract_id" AND ls."MaxTimestamp" = s."LS_timestamp"
                INNER JOIN
                    "LS_Opening" lo ON lo."LS_contract_id" = s."LS_contract_id"
                WHERE
                    s."LS_principal_stable" > 0
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
                        WHEN lo."LS_loan_pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN 'ALL_BTC (Short)'
                        WHEN lo."LS_loan_pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN 'ALL_SOL (Short)'
                        WHEN lo."LS_loan_pool_id" = 'nolus1lxr7f5xe02jq6cce4puk6540mtu9sg36at2dms5sk69wdtzdrg9qq0t67z' THEN 'AKT (Short)'
                        WHEN lo."LS_loan_pool_id" = 'nolus1u0zt8x3mkver0447glfupz9lz6wnt62j70p5fhhtu3fr46gcdd9s5dz9l6' THEN 'ATOM (Short)'
                        WHEN lo."LS_loan_pool_id" = 'nolus1py7pxw74qvlgq0n6rfz7mjrhgnls37mh87wasg89n75qt725rams8yr46t' THEN 'OSMO (Short)'
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
                        WHEN "Asset Type" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS', 'ALL_ETH') THEN "LS_amnt_stable" / 1000000000000000000
                    ELSE "LS_amnt_stable" / 1000000
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
                  WHEN "Asset Type" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS', 'ALL_ETH') THEN "LS_amnt_stable" / 1000000000000000000
              ELSE "LS_amnt_stable" / 1000000
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
                      WHEN "Asset Type" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS', 'ALL_ETH') THEN "Interest" / 1000000000000000000
                  ELSE "Interest" / 1000000
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
        WITH Loan_Type_Map AS (
          SELECT 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' AS id, 'Short' AS type, 'ALL_BTC' AS symbol, 100000000 AS denom
            UNION ALL SELECT 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm', 'Short', 'ALL_SOL', 1000000000
            UNION ALL SELECT 'nolus1lxr7f5xe02jq6cce4puk6540mtu9sg36at2dms5sk69wdtzdrg9qq0t67z', 'Short', 'AKT', 1000000
            UNION ALL SELECT 'nolus1u0zt8x3mkver0447glfupz9lz6wnt62j70p5fhhtu3fr46gcdd9s5dz9l6', 'Short', 'ATOM', 1000000
            UNION ALL SELECT 'nolus1py7pxw74qvlgq0n6rfz7mjrhgnls37mh87wasg89n75qt725rams8yr46t', 'Short', 'OSMO', 1000000
        ),
        Latest_States AS (
          SELECT DISTINCT ON ("LS_contract_id") *
          FROM "LS_State"
          WHERE "LS_timestamp" > NOW() - INTERVAL '1 hour'
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

            -- Loan
            CASE
              WHEN m.symbol = 'ALL_BTC' THEN s."LS_principal_stable" / 100000000.0
              WHEN m.symbol = 'ALL_SOL' THEN s."LS_principal_stable" / 1000000000.0
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

            -- Margin & Loan Interest
            (s."LS_prev_margin_stable" + s."LS_current_margin_stable") / COALESCE(m.denom, 1000000.0) AS "Margin Interest",
            (s."LS_prev_interest_stable" + s."LS_current_interest_stable") / COALESCE(m.denom, 1000000.0) AS "Loan Interest"
          FROM Latest_States s
          JOIN "LS_Opening" o ON s."LS_contract_id" = o."LS_contract_id"
          LEFT JOIN Loan_Type_Map m ON o."LS_loan_pool_id" = m.id
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
              WHEN o."LS_cltr_symbol" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS', 'ALL_ETH') THEN o."LS_cltr_amnt_stable" / 1000000000000000000
              ELSE o."LS_cltr_amnt_stable" / 1000000
            END AS "Down Payment"
          FROM "LS_State" s
          INNER JOIN "LS_Opening" o ON o."LS_contract_id" = s."LS_contract_id"
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
      .persistent(true)
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
      .persistent(true)
      .fetch_all(&self.pool)
      .await?;
        Ok(data)
    }

    pub async fn get_total_value_locked(
        &self,
        pools: TvlPoolParams,
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
            WITH Latest_States AS (
                SELECT DISTINCT ON ("LS_contract_id") *
                FROM "LS_State"
                WHERE "LS_timestamp" > NOW() - INTERVAL '1 hour'
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
            WITH Loan_Type_Map AS (
              SELECT
                'nolus1jufcaqm6657xmfltdezzz85quz92rmtd88jk5x0hq9zqseem32ysjdm990' AS id, 'Short' AS type, 'ST_ATOM' AS symbol, 1000000 AS denom
                UNION ALL SELECT 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3', 'Short', 'ALL_BTC', 100000000
                UNION ALL SELECT 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm', 'Short', 'ALL_SOL', 1000000000
                UNION ALL SELECT 'nolus1lxr7f5xe02jq6cce4puk6540mtu9sg36at2dms5sk69wdtzdrg9qq0t67z', 'Short', 'AKT', 1000000
                UNION ALL SELECT 'nolus1u0zt8x3mkver0447glfupz9lz6wnt62j70p5fhhtu3fr46gcdd9s5dz9l6', 'Short', 'ATOM', 1000000
                UNION ALL SELECT 'nolus1py7pxw74qvlgq0n6rfz7mjrhgnls37mh87wasg89n75qt725rams8yr46t', 'Short', 'OSMO', 1000000
            ),
            Latest_States AS (
              SELECT DISTINCT ON ("LS_contract_id") *
              FROM "LS_State"
              WHERE "LS_timestamp" > NOW() - INTERVAL '1 hour'
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
                COALESCE(m.type, 'Long') AS "Type",
                COALESCE(m.symbol, o."LS_asset_symbol") AS "Symbol",
                COALESCE(m.denom, 1000000.0) AS denom,

                -- Loan from LS_State
                CASE
                  WHEN m.symbol = 'ALL_BTC' THEN s."LS_principal_stable" / 100000000.0
                  WHEN m.symbol = 'ALL_SOL' THEN s."LS_principal_stable" / 1000000000.0
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

                -- Margin & Interest from LS_State
                (s."LS_prev_margin_stable" + s."LS_current_margin_stable") / COALESCE(m.denom, 1000000.0) AS "Margin Interest",
                (s."LS_prev_interest_stable" + s."LS_current_interest_stable") / COALESCE(m.denom, 1000000.0) AS "Loan Interest",

                CASE
                  WHEN o."LS_loan_pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3'
                    THEN (s."LS_prev_margin_asset"+s."LS_prev_interest_asset"+s."LS_current_margin_asset"+s."LS_current_interest_asset"+s."LS_principal_asset") / 100000000
                  WHEN o."LS_loan_pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm'
                    THEN (s."LS_prev_margin_asset"+s."LS_prev_interest_asset"+s."LS_current_margin_asset"+s."LS_current_interest_asset"+s."LS_principal_asset") / 1000000000
                  ELSE (s."LS_prev_margin_asset"+s."LS_prev_interest_asset"+s."LS_current_margin_asset"+s."LS_current_interest_asset"+s."LS_principal_asset") / 1000000
                END AS "Loan Token Amount"

              FROM Latest_States s
              JOIN "LS_Opening" o ON s."LS_contract_id" = o."LS_contract_id"
              LEFT JOIN Loan_Type_Map m ON o."LS_loan_pool_id" = m.id
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
                AND a."MP_asset_timestamp" > NOW() - INTERVAL '1 hour'
              ORDER BY
                a."MP_asset_symbol", a."MP_asset_timestamp" DESC
            )
            SELECT
              TO_CHAR("Time", 'YYYY-MM-DD') AS "Date",
              "Type",
              js."Symbol",
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
}
