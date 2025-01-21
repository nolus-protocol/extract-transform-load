use std::iter;

use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use sqlx::{error::Error, QueryBuilder};

use crate::{
    custom_uint::UInt63,
    model::{LS_Opening, LS_State, Table},
};

impl Table<LS_State> {
    pub async fn insert(
        &self,
        &LS_State {
            ref LS_contract_id,
            LS_timestamp,
            ref LS_amnt_stable,
            ref LS_amnt,
            ref LS_prev_margin_stable,
            ref LS_prev_interest_stable,
            ref LS_current_margin_stable,
            ref LS_current_interest_stable,
            ref LS_principal_stable,
            ref LS_lpn_loan_amnt,
            ref LS_prev_margin_asset,
            ref LS_prev_interest_asset,
            ref LS_current_margin_asset,
            ref LS_current_interest_asset,
            ref LS_principal_asset,
        }: &LS_State,
    ) -> Result<(), Error> {
        const SQL: &str = r#"
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
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
        "#;

        sqlx::query(SQL)
            .bind(LS_contract_id)
            .bind(LS_timestamp)
            .bind(LS_amnt_stable)
            .bind(LS_amnt)
            .bind(LS_prev_margin_stable)
            .bind(LS_prev_interest_stable)
            .bind(LS_current_margin_stable)
            .bind(LS_current_interest_stable)
            .bind(LS_principal_stable)
            .bind(LS_lpn_loan_amnt)
            .bind(LS_prev_margin_asset)
            .bind(LS_prev_interest_asset)
            .bind(LS_current_margin_asset)
            .bind(LS_current_interest_asset)
            .bind(LS_principal_asset)
            .execute(&self.pool)
            .await
            .map(drop)
    }

    pub async fn get_active_states(&self) -> Result<Vec<LS_Opening>, Error> {
        const SQL: &str = r#"
        SELECT
            "LS_contract_id",
            "LS_address_id",
            "LS_asset_symbol",
            "LS_interest",
            "LS_timestamp",
            "LS_loan_pool_id",
            "LS_loan_amnt",
            "LS_loan_amnt_stable",
            "LS_loan_amnt_asset",
            "LS_cltr_symbol",
            "LS_cltr_amnt_stable",
            "LS_cltr_amnt_asset",
            "LS_native_amnt_stable",
            "LS_native_amnt_nolus",
            "LS_lpn_loan_amnt",
            "Tx_Hash"
        FROM "LS_Opening"
        WHERE "LS_contract_id" NOT IN (
            SELECT "LS_contract_id" as "Total"
            FROM (
                SELECT "LS_contract_id"
                FROM "LS_Closing"
                UNION ALL
                    SELECT "LS_contract_id"
                    FROM "LS_Close_Position"
                    WHERE "LS_loan_close" = true
                UNION ALL
                    SELECT "LS_contract_id"
                    FROM "LS_Repayment"
                    WHERE "LS_loan_close" = true
                UNION ALL
                    SELECT "LS_contract_id"
                    FROM "LS_Liquidation"
                    WHERE "LS_loan_close" = true
            ) AS "combined_data"
            GROUP BY "LS_contract_id"
        )
        "#;

        sqlx::query_as(SQL).fetch_all(&self.pool).await
    }

    pub async fn insert_many<'r, T>(&self, data: T) -> Result<(), Error>
    where
        T: IntoIterator<Item = &'r LS_State>,
    {
        const SQL: &str = r#"
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
        "#;

        let mut iter = data.into_iter();

        let Some(first) = iter.next() else {
            return Ok(());
        };

        QueryBuilder::new(SQL)
            .push_values(
                iter::once(first).chain(iter),
                |mut b,
                 &LS_State {
                     ref LS_contract_id,
                     LS_timestamp,
                     ref LS_amnt_stable,
                     ref LS_amnt,
                     ref LS_prev_margin_stable,
                     ref LS_prev_interest_stable,
                     ref LS_current_margin_stable,
                     ref LS_current_interest_stable,
                     ref LS_principal_stable,
                     ref LS_lpn_loan_amnt,
                     ref LS_prev_margin_asset,
                     ref LS_prev_interest_asset,
                     ref LS_current_margin_asset,
                     ref LS_current_interest_asset,
                     ref LS_principal_asset,
                 }| {
                    b.push_bind(LS_contract_id)
                        .push_bind(LS_timestamp)
                        .push_bind(LS_amnt_stable)
                        .push_bind(LS_amnt)
                        .push_bind(LS_prev_margin_stable)
                        .push_bind(LS_prev_interest_stable)
                        .push_bind(LS_current_margin_stable)
                        .push_bind(LS_current_interest_stable)
                        .push_bind(LS_principal_stable)
                        .push_bind(LS_lpn_loan_amnt)
                        .push_bind(LS_prev_margin_asset)
                        .push_bind(LS_prev_interest_asset)
                        .push_bind(LS_current_margin_asset)
                        .push_bind(LS_current_interest_asset)
                        .push_bind(LS_principal_asset);
                },
            )
            .build()
            .execute(&self.pool)
            .await
            .map(drop)
    }

    pub async fn count(
        &self,
        timestamp: DateTime<Utc>,
    ) -> Result<UInt63, Error> {
        const SQL: &str = r#"
        SELECT COUNT(1)
        FROM "LS_State"
        WHERE "LS_timestamp" = $1
        "#;

        sqlx::query_as(SQL)
            .bind(timestamp)
            .fetch_one(&self.pool)
            .await
            .map(|(result,)| result)
    }

    #[cfg(all(not(feature = "testnet"), feature = "mainnet"))]
    pub async fn get_total_value_locked(
        &self,
        osmosis_usdc_protocol: &str,
        neutron_axelar_protocol: &str,
        osmosis_usdc_noble_protocol: &str,
        neutron_usdc_noble_protocol: &str,
        osmosis_st_atom: &str,
        osmosis_all_btc: &str,
        osmosis_all_sol: &str,
        osmosis_akt: &str,
    ) -> Result<BigDecimal, Error> {
        const SQL: &str = r#"
        WITH "Lease_Value_Divisor" AS (
            SELECT
                "LS_asset_symbol",
                (
                    CASE
                        WHEN "LS_asset_symbol" IN ('WBTC', 'ALL_BTC', 'CRO') THEN 100000000
                        WHEN "LS_asset_symbol" IN ('ALL_SOL') THEN 1000000000
                        WHEN "LS_asset_symbol" IN ('PICA') THEN 1000000000000
                        WHEN "LS_asset_symbol" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS') THEN 1000000000000000000
                        ELSE 1000000
                    END
                ) AS "Divisor"
            FROM "LS_Opening"
            GROUP BY "LS_asset_symbol"
        ),
        "Lease_Value" AS (
            SELECT "s"."LS_amnt_stable" / "d"."Divisor" AS "Lease Value"
            FROM "LS_State" AS "s"
            LEFT JOIN "LS_Opening" AS "o" ON "o"."LS_contract_id" = "s"."LS_contract_id"
            LEFT JOIN "Lease_Value_Divisor" AS "d" ON "o"."LS_asset_symbol" = "d"."LS_asset_symbol"
            WHERE "s"."LS_timestamp" > NOW() - INTERVAL '1 hours'
        ),
        "Available_Assets_Osmosis" AS (
            SELECT ("LP_Pool_total_value_locked_stable" - "LP_Pool_total_borrowed_stable") / 1000000 AS "Available Assets"
            FROM "LP_Pool_State"
            WHERE "LP_Pool_id" = $1
            ORDER BY "LP_Pool_timestamp" DESC
            LIMIT 1
        ),
        "Available_Assets_Neutron" AS (
            SELECT ("LP_Pool_total_value_locked_stable" - "LP_Pool_total_borrowed_stable") / 1000000 AS "Available Assets"
            FROM "LP_Pool_State"
            WHERE "LP_Pool_id" = $2
            ORDER BY "LP_Pool_timestamp" DESC
            LIMIT 1
        ),
        "Available_Osmosis_Noble" AS (
            SELECT ("LP_Pool_total_value_locked_stable" - "LP_Pool_total_borrowed_stable") / 1000000 AS "Available Assets"
            FROM "LP_Pool_State"
            WHERE "LP_Pool_id" = $3
            ORDER BY "LP_Pool_timestamp" DESC
            LIMIT 1
        ),
        "Available_Neutron_Noble" AS (
            SELECT ("LP_Pool_total_value_locked_stable" - "LP_Pool_total_borrowed_stable") / 1000000 AS "Available Assets"
            FROM "LP_Pool_State"
            WHERE "LP_Pool_id" = $4
            ORDER BY "LP_Pool_timestamp" DESC
            LIMIT 1
        ),
        "Available_ST_ATOM_OSMOSIS" AS (
            SELECT ("LP_Pool_total_value_locked_stable" - "LP_Pool_total_borrowed_stable") / 1000000 AS "Available Assets"
            FROM "LP_Pool_State"
            WHERE "LP_Pool_id" = $5
            ORDER BY "LP_Pool_timestamp" DESC
            LIMIT 1
        ),
        "Available_ALL_BTC_OSMOSIS" AS (
            SELECT ("LP_Pool_total_value_locked_stable" - "LP_Pool_total_borrowed_stable") / 100000000 AS "Available Assets"
            FROM "LP_Pool_State"
            WHERE "LP_Pool_id" = $6
            ORDER BY "LP_Pool_timestamp" DESC
            LIMIT 1
        ),
        "Available_ALL_SOL_OSMOSIS" AS (
            SELECT ("LP_Pool_total_value_locked_stable" - "LP_Pool_total_borrowed_stable") / 1000000000 AS "Available Assets"
            FROM "LP_Pool_State"
            WHERE "LP_Pool_id" = $7
            ORDER BY "LP_Pool_timestamp" DESC
            LIMIT 1
        ),
        "Available_AKT_OSMOSIS" AS (
            SELECT ("LP_Pool_total_value_locked_stable" - "LP_Pool_total_borrowed_stable") / 1000000 AS "Available Assets"
            FROM "LP_Pool_State"
            WHERE "LP_Pool_id" = $8
            ORDER BY "LP_Pool_timestamp" DESC
            LIMIT 1
        ),
        "Lease_Value_Sum" AS (
            SELECT SUM("Lease Value") AS "Total Lease Value"
            FROM "Lease_Value"
        )
        SELECT (
            COALESCE(
                (
                    SELECT "Total Lease Value"
                    FROM "Lease_Value_Sum"
                ),
                0
            ) + COALESCE(
                (
                    SELECT "Available Assets"
                    FROM "Available_Assets_Osmosis"
                ),
                0
            ) + COALESCE(
                (
                    SELECT "Available Assets"
                    FROM "Available_Assets_Neutron"
                ),
                0
            ) + COALESCE(
                (
                    SELECT "Available Assets"
                    FROM "Available_Osmosis_Noble"
                ),
                0
            ) + COALESCE(
                (
                    SELECT "Available Assets"
                    FROM "Available_Neutron_Noble"
                ),
                0
            ) + COALESCE(
                (
                    SELECT "Available Assets"
                    FROM "Available_ST_ATOM_OSMOSIS"
                ),
                0
            ) + COALESCE(
                (
                    SELECT "Available Assets"
                    FROM "Available_ALL_BTC_OSMOSIS"
                ),
                0
            ) + COALESCE(
                (
                    SELECT "Available Assets"
                    FROM "Available_ALL_SOL_OSMOSIS"
                ),
                0
            ) + COALESCE(
                (
                    SELECT "Available Assets"
                    FROM "Available_AKT_OSMOSIS"
                ),
                0
            )
        ) AS "TVL"
        "#;

        sqlx::query_as(SQL)
            .bind(osmosis_usdc_protocol)
            .bind(neutron_axelar_protocol)
            .bind(osmosis_usdc_noble_protocol)
            .bind(neutron_usdc_noble_protocol)
            .bind(osmosis_st_atom)
            .bind(osmosis_all_btc)
            .bind(osmosis_all_sol)
            .bind(osmosis_akt)
            .fetch_one(&self.pool)
            .await
            .map(|(amnt,)| amnt)
    }

    #[cfg(all(feature = "testnet", not(feature = "mainnet")))]
    pub async fn get_total_value_locked(
        &self,
        osmosis_usdc_protocol: &str,
        neutron_axelar_protocol: &str,
    ) -> Result<BigDecimal, Error> {
        const SQL: &str = r#"
        WITH "Lease_Value_Divisor" AS (
            SELECT
                "LS_asset_symbol",
                (
                    CASE
                        WHEN "LS_asset_symbol" IN ('WBTC', 'CRO') THEN 100000000
                        WHEN "LS_asset_symbol" IN ('PICA') THEN 1000000000000
                        WHEN "LS_asset_symbol" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS') THEN 1000000000000000000
                        ELSE 1000000
                    END
                ) AS "Divisor"
            FROM "LS_Opening"
            GROUP BY "LS_asset_symbol"
        ),
        "Lease_Value" AS (
            SELECT "s"."LS_amnt_stable" / "d"."Divisor" AS "Lease Value"
            FROM "LS_State" AS "s"
            LEFT JOIN "LS_Opening" AS "o" ON "o"."LS_contract_id" = "s"."LS_contract_id"
            LEFT JOIN Lease_Value_Divisor AS "d" ON "o"."LS_asset_symbol" = "d"."LS_asset_symbol"
            WHERE "s"."LS_timestamp" > NOW() - INTERVAL '1 hours'
        ),
        "Available_Assets_Osmosis" AS (
            SELECT ("LP_Pool_total_value_locked_stable" - "LP_Pool_total_borrowed_stable") / 1000000 AS "Available Assets"
            FROM "LP_Pool_State"
            WHERE "LP_Pool_id" = $1
            ORDER BY "LP_Pool_timestamp" DESC LIMIT 1
        ),
        "Available_Assets_Neutron" AS (
            SELECT ("LP_Pool_total_value_locked_stable" - "LP_Pool_total_borrowed_stable") / 1000000 AS "Available Assets"
            FROM "LP_Pool_State"
            WHERE "LP_Pool_id" = $2
            ORDER BY "LP_Pool_timestamp" DESC LIMIT 1
        ),
        "Lease_Value_Sum" AS (
            SELECT SUM("Lease Value") AS "Total Lease Value"
            FROM "Lease_Value"
        )
        SELECT (
            COALESCE(
                (
                    SELECT "Total Lease Value"
                    FROM "Lease_Value_Sum"
                ),
                0
            ) + COALESCE(
                (
                    SELECT "Available Assets"
                    FROM "Available_Assets_Osmosis"
                ),
                0
            ) + (
                    SELECT "Available Assets"
                    FROM "Available_Assets_Neutron"
                ),
                0
            )
        ) AS "TVL"
        "#;

        sqlx::query_as(SQL)
            .bind(osmosis_usdc_protocol)
            .bind(neutron_axelar_protocol)
            .fetch_one(&self.pool)
            .await
            .map(|(amnt,)| amnt)
    }
}
