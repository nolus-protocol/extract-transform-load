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

    pub async fn get_total_value_locked(&self) -> Result<BigDecimal, crate::error::Error> {
      let value: Option<(Option<BigDecimal>,)>  = sqlx::query_as(
        r#"
              WITH Lease_Value_Divisor AS (
                SELECT
                  "LS_asset_symbol",
                  CASE
                    WHEN "LS_asset_symbol" IN ('WBTC', 'CRO') THEN 100000000
                    WHEN "LS_asset_symbol" IN ('WETH', 'EVMOS', 'INJ', 'DYDX') THEN 1000000000000000000
                    ELSE 1000000
                  END AS "Divisor"
                FROM "LS_Opening"
                GROUP BY "LS_asset_symbol"
              ),
              Lease_Value AS (
                SELECT
                  s."LS_timestamp" AS "Timestamp",
                  o."LS_asset_symbol" AS "Token",
                  o."LS_contract_id" as "Contract ID",
                  s."LS_amnt_stable" / d."Divisor" AS "Lease Value"
                FROM
                  "LS_State" s
                LEFT JOIN "LS_Opening" o ON o."LS_contract_id" = s."LS_contract_id"
                LEFT JOIN Lease_Value_Divisor d ON o."LS_asset_symbol" = d."LS_asset_symbol"
                WHERE
                  s."LS_amnt_stable" > 0
              ),
              Available_Assets_Osmosis AS (
                SELECT
                  "LP_Pool_timestamp" AS "Timestamp",
                  ("LP_Pool_total_value_locked_stable" - "LP_Pool_total_borrowed_stable") / 1000000 AS "Available Assets"
                FROM
                  "LP_Pool_State"
                WHERE "LP_Pool_id" = 'nolus1qg5ega6dykkxc307y25pecuufrjkxkaggkkxh7nad0vhyhtuhw3sqaa3c5'
              ),
              Available_Assets_Neutron AS (
                SELECT
                  "LP_Pool_timestamp" AS "Timestamp",
                  CASE
                    WHEN "LP_Pool_timestamp" >= '2024-01-22 16:04:17' AND "LP_Pool_id" = 'nolus1qqcr7exupnymvg6m63eqwu8pd4n5x6r5t3pyyxdy7r97rcgajmhqy3gn94' THEN ("LP_Pool_total_value_locked_stable" - "LP_Pool_total_borrowed_stable") / 1000000
                    ELSE 0
                  END AS "Available Assets"
                FROM
                  "LP_Pool_State"
                WHERE NOT ("LP_Pool_timestamp" >= '2024-01-22 16:04:17' AND "LP_Pool_id" = 'nolus1qg5ega6dykkxc307y25pecuufrjkxkaggkkxh7nad0vhyhtuhw3sqaa3c5')
              )
              SELECT
                (SUM("Lease Value") + a."Available Assets" + an."Available Assets") AS "TVL"
              FROM
                Lease_Value l
              LEFT JOIN Available_Assets_Osmosis a ON l."Timestamp" = a."Timestamp"
              LEFT JOIN Available_Assets_Neutron an ON a."Timestamp" = an."Timestamp"
              GROUP BY
                l."Timestamp", a."Available Assets", an."Available Assets"
              ORDER BY
                l."Timestamp" DESC
              limit 1;
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

}
