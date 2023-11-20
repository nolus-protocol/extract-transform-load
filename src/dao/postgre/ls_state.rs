use super::{DataBase, QueryResult};
use crate::model::{LS_Opening, LS_State, Table, TVL_Serie};
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

    pub async fn get_total_value_locked(&self) -> Result<BigDecimal, crate::error::Error> {
      let value: Option<(Option<BigDecimal>,)>  = sqlx::query_as(
        r#"
              WITH Lease_Value AS (
                SELECT
                  "LS_State"."LS_timestamp" AS "Timestamp",
                  "LS_asset_symbol" AS "Token",
                  "LS_Opening"."LS_contract_id" as "Contract ID",
                  CASE
                    WHEN "LS_asset_symbol" = 'WBTC' THEN "LS_amnt_stable" / 100000000
                    WHEN "LS_asset_symbol" = 'WETH' THEN "LS_amnt_stable" / 1000000000000000000
                    WHEN "LS_asset_symbol" = 'EVMOS' THEN "LS_amnt_stable" / 1000000000000000000
                    WHEN "LS_asset_symbol" = 'CRO' THEN "LS_amnt_stable" / 100000000
                    WHEN "LS_asset_symbol" = 'TIA' THEN "LS_amnt_stable" / 100000000

                    WHEN "LS_asset_symbol" != 'WETH'
                    AND "LS_asset_symbol" != 'WBTC' 
                    AND "LS_asset_symbol" != 'EVMOS'
                    AND "LS_asset_symbol" != 'CRO' 
                    AND "LS_asset_symbol" != 'TIA' 

                    THEN "LS_amnt_stable" / 1000000

                  END AS "Lease Value"
                FROM
                  "LS_State"
                  LEFT JOIN "LS_Opening" ON "LS_Opening"."LS_contract_id" = "LS_State"."LS_contract_id"
                WHERE
                  "LS_amnt_stable" > 0
                ORDER BY
                  "Lease Value" DESC
              ),
              Available_Assets AS (
                SELECT
                  "LP_Pool_timestamp" AS "Timestamp",
                  (
                    "LP_Pool_total_value_locked_stable" - "LP_Pool_total_borrowed_stable"
                  ) / 1000000 AS "Available Assets"
                FROM
                  "LP_Pool_State"
              )
              SELECT
                (SUM("Lease Value") + "Available Assets") AS "TVL"
              FROM
                Lease_Value l
                LEFT JOIN Available_Assets a ON l."Timestamp" = a."Timestamp"
              GROUP BY
                l."Timestamp",
                a."Available Assets"
              ORDER BY
                l."Timestamp" DESC
              LIMIT
                1
            "#,
        )
        .fetch_optional(&self.pool)
        .await?;

        let default = BigDecimal::from_str("0")?;
        let amount = if let Some(v) = value {
          v.0
        }else{
          Some(default.to_owned())
        };

        Ok(amount.unwrap_or(default.to_owned()))
    }

    pub async fn get_total_value_locked_series(&self) -> Result<Vec<TVL_Serie>, Error> {
      let data = sqlx::query_as(
          r#"
          WITH Lease_Value AS (
            SELECT
              "LS_State"."LS_timestamp" AS "Timestamp",
              "LS_asset_symbol" AS "Token",
              "LS_Opening"."LS_contract_id" as "Contract ID",
              CASE
                WHEN "LS_asset_symbol" = 'WBTC' THEN "LS_amnt_stable" / 100000000
                WHEN "LS_asset_symbol" = 'WETH' THEN "LS_amnt_stable" / 1000000000000000000
                WHEN "LS_asset_symbol" = 'EVMOS' THEN "LS_amnt_stable" / 1000000000000000000
                WHEN "LS_asset_symbol" = 'CRO' THEN "LS_amnt_stable" / 100000000
                WHEN "LS_asset_symbol" = 'TIA' THEN "LS_amnt_stable" / 100000000

                WHEN "LS_asset_symbol" != 'WETH'
                AND "LS_asset_symbol" != 'WBTC' 
                AND "LS_asset_symbol" != 'EVMOS'
                AND "LS_asset_symbol" != 'CRO' 
                AND "LS_asset_symbol" != 'TIA' 

                THEN "LS_amnt_stable" / 1000000

              END AS "Lease Value"
            FROM
              "LS_State"
              LEFT JOIN "LS_Opening" ON "LS_Opening"."LS_contract_id" = "LS_State"."LS_contract_id"
            WHERE
              "LS_amnt_stable" > 0
            ORDER BY
              "Lease Value" DESC
          ),
          Available_Assets AS (
            SELECT
              "LP_Pool_timestamp" AS "Timestamp",
              (
                "LP_Pool_total_value_locked_stable" - "LP_Pool_total_borrowed_stable"
              ) / 1000000 AS "Available Assets"
            FROM
              "LP_Pool_State"
          )
          SELECT
            (SUM("Lease Value") + "Available Assets") AS "TVL",
            l."Timestamp" as  "Timestamp"
          FROM
            Lease_Value l
            LEFT JOIN Available_Assets a ON l."Timestamp" = a."Timestamp"
          GROUP BY
            l."Timestamp",
            a."Available Assets"
          ORDER BY
            l."Timestamp" ASC;
          "#,
      )
      .fetch_all(&self.pool)
      .await?;
      Ok(data)
  }
}
