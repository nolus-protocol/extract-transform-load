use std::str::FromStr as _;

use chrono::{DateTime, Utc};
use sqlx::{types::BigDecimal, Error, QueryBuilder};

use crate::model::{LS_Opening, LS_State, Table};

use super::{DataBase, QueryResult};

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
