use super::{DataBase, QueryResult};
use crate::model::{LS_Opening, LS_State, Table};
use chrono::{DateTime, Utc};
use sqlx::{error::Error, types::BigDecimal, QueryBuilder};
use std::str::FromStr;

impl Table<LS_State> {
    pub async fn insert(&self, data: LS_State) -> Result<QueryResult, Error> {
        sqlx::query(
            r#"
            INSERT INTO "LS_State" (
                "LS_contract_id",
                "LS_timestamp",
                "LS_amnt_stable",
                "LS_prev_margin_stable",
                "LS_prev_interest_stable",
                "LS_current_margin_stable",
                "LS_current_interest_stable",
                "LS_principal_stable"
            )
            VALUES($1, $2, $3, $4, $5, $6, $7, $8)
            "#,
        )
        .bind(&data.LS_contract_id)
        .bind(data.LS_timestamp)
        .bind(&data.LS_amnt_stable)
        .bind(&data.LS_prev_margin_stable)
        .bind(&data.LS_prev_interest_stable)
        .bind(&data.LS_current_margin_stable)
        .bind(&data.LS_current_interest_stable)
        .bind(&data.LS_principal_stable)
        .execute(&self.pool)
        .await
    }

    pub async fn get_active_states(&self) -> Result<Vec<LS_Opening>, Error> {
        sqlx::query_as(
            r#"
                SELECT 
                    a."LS_contract_id",
                    a."LS_address_id",
                    a."LS_asset_symbol",
                    a."LS_interest",
                    a."LS_timestamp",
                    a."LS_loan_pool_id",
                    a."LS_loan_amnt_stable",
                    a."LS_loan_amnt_asset",
                    a."LS_cltr_symbol",
                    a."LS_cltr_amnt_stable",
                    a."LS_cltr_amnt_asset",
                    a."LS_native_amnt_stable",
                    a."LS_native_amnt_nolus"
                FROM "LS_Opening" as a 
                LEFT JOIN "LS_Closing" as b 
                ON a."LS_contract_id" = b."LS_contract_id" 
                WHERE b."LS_contract_id" IS NULL
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
                "LS_prev_margin_stable",
                "LS_prev_interest_stable",
                "LS_current_margin_stable",
                "LS_current_interest_stable",
                "LS_principal_stable"
            )"#,
        );

        query_builder.push_values(data, |mut b, data| {
            b.push_bind(&data.LS_contract_id)
                .push_bind(data.LS_timestamp)
                .push_bind(&data.LS_amnt_stable)
                .push_bind(&data.LS_prev_margin_stable)
                .push_bind(&data.LS_prev_interest_stable)
                .push_bind(&data.LS_current_margin_stable)
                .push_bind(&data.LS_current_interest_stable)
                .push_bind(&data.LS_principal_stable);
        });

        let query = query_builder.build();
        query.execute(&self.pool).await?;
        Ok(())
    }

    pub async fn count(&self, timestamp: DateTime<Utc>) -> Result<i64, crate::error::Error> {
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

    pub async fn get_total_value_locked(
        &self,
        osmosis_usdc_protocol: String,
        neutron_axelar_protocol: String,
        osmosis_usdc_noble_protocol: String,
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
          Available_Osmosis_Noble AS (
            SELECT ("LP_Pool_total_value_locked_stable" - "LP_Pool_total_borrowed_stable") / 1000000 AS "Available Assets"
            FROM
              "LP_Pool_State"
            WHERE "LP_Pool_id" = $3
            ORDER BY "LP_Pool_timestamp" DESC LIMIT 1
          ),
          Lease_Value_Sum AS (
            SELECT SUM("Lease Value") AS "Total Lease Value" FROM Lease_Value
          )
          SELECT
            (SELECT "Total Lease Value" FROM Lease_Value_Sum) +
            (SELECT "Available Assets" FROM Available_Assets_Osmosis) +
            (SELECT "Available Assets" FROM Available_Assets_Neutron) +
            (SELECT "Available Assets" FROM Available_Osmosis_Noble) AS "TVL"
            "#,
        )
        .bind(osmosis_usdc_protocol)
        .bind(neutron_axelar_protocol)
        .bind(osmosis_usdc_noble_protocol)
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
