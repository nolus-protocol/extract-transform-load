use bigdecimal::Zero as _;
use chrono::{DateTime, Utc};
use sqlx::{types::BigDecimal, Error, QueryBuilder};

use crate::model::{
    LS_Opening, LS_State, Pnl_Over_Time, Table, Unrealized_Pnl,
};

impl Table<LS_State> {
    // FIXME Pass data by reference, as separate arguments or as a dedicated
    //  structure. Avoid the need for owned data.
    pub async fn insert(&self, data: LS_State) -> Result<(), Error> {
        // FIXME [MAJOR] Insert with all of the argument's fields or use
        //  `DEFAULT`.
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
            .map(drop)
    }

    // FIXME Driver might limit number of returned rows.
    pub async fn get_active_states(&self) -> Result<Vec<LS_Opening>, Error> {
        // FIXME Research possible query optimizations.
        const SQL: &str = r#"
        SELECT *
        FROM "LS_Opening"
        WHERE "LS_contract_id" NOT IN (
            SELECT "LS_contract_id"
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
            )
            GROUP BY "LS_contract_id"
        )
        "#;

        sqlx::query_as(SQL).fetch_all(&self.pool).await
    }

    // FIXME Pass data by reference, as separate arguments or as a dedicated
    //  structure. Avoid the need for owned data.
    // FIXME Use iterators instead.
    pub async fn insert_many(&self, data: &Vec<LS_State>) -> Result<(), Error> {
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

        if data.is_empty() {
            return Ok(());
        }

        QueryBuilder::new(SQL)
            .push_values(data, |mut b, data| {
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
            })
            .build()
            .persistent(false)
            .execute(&self.pool)
            .await
            .map(drop)
    }

    // FIXME Use `UInt63` instead.
    pub async fn count(&self, timestamp: DateTime<Utc>) -> Result<i64, Error> {
        const SQL: &str = r#"
        SELECT
            COUNT(*)
        FROM "LS_State"
        WHERE "LS_timestamp" = $1
        "#;

        sqlx::query_as(SQL)
            .bind(timestamp)
            .fetch_one(&self.pool)
            .await
            .map(|(result,)| result)
    }

    pub async fn get_open_position_value(&self) -> Result<BigDecimal, Error> {
        // FIXME Find a way to describe currencies dynamically.
        // FIXME Find a way to describe currencies' decimal places dynamically.
        const SQL: &str = r#"
        WITH "LatestTimestamps" AS (
            SELECT
                "LS_contract_id", 
                MAX("LS_timestamp") AS "MaxTimestamp"
            FROM "LS_State"
            WHERE "LS_timestamp" > (now() - INTERVAL '1 hour')
            GROUP BY "LS_contract_id"
        ),
        "Opened" AS (
            SELECT
                "s"."LS_contract_id",
                "s"."LS_amnt_stable",
                (
                    CASE
                        WHEN "lo"."LS_loan_pool_id" = 'nolus1jufcaqm6657xmfltdezzz85quz92rmtd88jk5x0hq9zqseem32ysjdm990' THEN
                            'ST_ATOM (Short)'
                        WHEN "lo"."LS_loan_pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN
                            'ALL_BTC (Short)'
                        WHEN "lo"."LS_loan_pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN
                            'ALL_SOL (Short)'
                        WHEN "lo"."LS_loan_pool_id" = 'nolus1lxr7f5xe02jq6cce4puk6540mtu9sg36at2dms5sk69wdtzdrg9qq0t67z' THEN
                            'AKT (Short)'
                        ELSE
                            "lo"."LS_asset_symbol"
                    END
                ) AS "Asset Type"
            FROM "LS_State" AS "s"
            JOIN "LatestTimestamps" AS "lt" ON
                "s"."LS_contract_id" = "lt"."LS_contract_id" AND
                "s"."LS_timestamp" = "lt"."MaxTimestamp"
            JOIN "LS_Opening" AS "lo" ON "lo"."LS_contract_id" = "s"."LS_contract_id"
            WHERE "s"."LS_amnt_stable" > 0
        ),
        "Lease_Value_Table" AS (
            SELECT
                "op"."Asset Type" AS "Token",
                (
                    "LS_amnt_stable" / (
                        CASE
                            WHEN "Asset Type" IN ('ALL_BTC', 'WBTC', 'CRO') THEN
                                100000000
                            WHEN "Asset Type" IN ('ALL_SOL') THEN
                                1000000000
                            WHEN "Asset Type" IN ('PICA') THEN
                                1000000000000
                            WHEN "Asset Type" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS') THEN
                                1000000000000000000
                            ELSE
                                1000000
                        END
                    )
                ) AS "Lease Value"
            FROM "Opened" "op"
        )
        SELECT
            SUM("Lease Value")
        FROM "Lease_Value_Table"
        "#;

        sqlx::query_as(SQL)
            .fetch_optional(&self.pool)
            .await
            .map(|maybe_row| {
                maybe_row.map_or_else(BigDecimal::zero, |(result,)| result)
            })
    }

    pub async fn get_open_interest(&self) -> Result<BigDecimal, Error> {
        // FIXME Find a way to describe currencies dynamically.
        // FIXME Find a way to describe currencies' decimal places dynamically.
        const SQL: &str = r#"
        WITH LatestTimestamps AS (
            SELECT
                "LS_contract_id", 
                MAX("LS_timestamp") AS "MaxTimestamp"
            FROM "LS_State"
            WHERE "LS_timestamp" > (now() - INTERVAL '1 hour')
            GROUP BY "LS_contract_id"
        ),
        Opened AS (
            SELECT
                "s"."LS_contract_id",
                (
                    "s"."LS_prev_margin_stable" +
                        "s"."LS_prev_interest_stable" +
                        "s"."LS_current_margin_stable" +
                        "s"."LS_current_interest_stable"
                ) AS "Interest",
                (
                    CASE
                        WHEN "lo"."LS_loan_pool_id" = 'nolus1jufcaqm6657xmfltdezzz85quz92rmtd88jk5x0hq9zqseem32ysjdm990' THEN
                            'ST_ATOM (Short)'
                        WHEN "lo"."LS_loan_pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN
                            'ALL_BTC (Short)'
                        WHEN "lo"."LS_loan_pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN
                            'ALL_SOL (Short)'
                        WHEN "lo"."LS_loan_pool_id" = 'nolus1lxr7f5xe02jq6cce4puk6540mtu9sg36at2dms5sk69wdtzdrg9qq0t67z' THEN
                            'AKT (Short)'
                        ELSE
                            "lo"."LS_asset_symbol"
                    END
                ) AS "Asset Type"
            FROM "LS_State" AS "s"
            JOIN "LatestTimestamps" AS "lt" ON
                "s"."LS_contract_id" = "lt"."LS_contract_id" AND
                "s"."LS_timestamp" = "lt"."MaxTimestamp"
            JOIN "LS_Opening" AS "lo" ON "lo"."LS_contract_id" = "s"."LS_contract_id"
        ),
        "Lease_Value_Table" AS (
            SELECT
                "op"."Asset Type" AS "Token",
                (
                    "Interest" / (
                        CASE
                            WHEN "Asset Type" IN ('ALL_BTC', 'WBTC', 'CRO') THEN
                                100000000
                            WHEN "Asset Type" IN ('ALL_SOL') THEN
                                1000000000
                            WHEN "Asset Type" IN ('PICA') THEN
                                1000000000000
                            WHEN "Asset Type" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS') THEN
                                1000000000000000000
                            ELSE
                                1000000
                        END
                    )
                ) AS "Total Interest Due"
            FROM "Opened" AS "op"
        )
        SELECT SUM("Total Interest Due")
        FROM "Lease_Value_Table"
        "#;

        sqlx::query_as(SQL)
            .fetch_optional(&self.pool)
            .await
            .map(|maybe_row| {
                maybe_row.map_or_else(BigDecimal::zero, |(result,)| result)
            })
    }

    pub async fn get_unrealized_pnl(&self) -> Result<BigDecimal, Error> {
        // FIXME Find a way to describe currencies' decimal places dynamically.
        const SQL: &str = r#"
        WITH "DP_Loan_Table" AS (
            SELECT
                "LS_Opening"."LS_contract_id" as "Contract ID",
                (
                    "LS_principal_stable" / (
                        CASE
                            WHEN "LS_Opening"."LS_loan_pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN
                                100000000
                            WHEN "LS_Opening"."LS_loan_pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN
                                1000000000
                            ELSE
                                1000000
                        END
                    )
                ) AS "Loan",
                (
                    "LS_cltr_amnt_stable" / (
                        CASE
                            WHEN "LS_cltr_symbol" IN ('ALL_BTC', 'WBTC', 'CRO') THEN
                                100000000
                            WHEN "LS_cltr_symbol" IN ('ALL_SOL') THEN
                                1000000000
                            WHEN "LS_cltr_symbol" IN ('PICA') THEN
                                1000000000000
                            WHEN "LS_cltr_symbol" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS') THEN
                                1000000000000000000
                            ELSE
                                1000000
                        END
                    )
                ) AS "Down Payment"
            FROM "LS_State" AS "Opened"
            LEFT JOIN "LS_Opening" ON "LS_Opening"."LS_contract_id" = "Opened"."LS_contract_id"
            WHERE "Opened"."LS_timestamp" >= (NOW() - INTERVAL '1 hours')
        ),
        "Lease_Value_Table" AS (
            SELECT
                "LS_Opening"."LS_contract_id" as "Contract ID",
                (
                    "LS_amnt_stable" / (
                        CASE
                            WHEN "LS_asset_symbol" IN ('WBTC', 'CRO', 'ALL_BTC') THEN
                                100000000
                            WHEN "LS_asset_symbol" IN ('ALL_SOL') THEN
                                1000000000
                            WHEN "LS_asset_symbol" IN ('PICA') THEN
                                1000000000000
                            WHEN "LS_asset_symbol" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS') THEN
                                1000000000000000000
                            ELSE
                                1000000
                        END
                    )
                ) AS "Lease Value",
                (
                    (
                        "LS_prev_margin_stable" + "LS_current_margin_stable"
                    ) / (
                        CASE
                            WHEN "LS_Opening"."LS_loan_pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN
                                100000000
                            WHEN "LS_Opening"."LS_loan_pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN
                                1000000000
                            ELSE
                                1000000
                        END
                    )
                ) AS "Margin Interest",
                (
                    (
                        "LS_prev_interest_stable" + "LS_current_interest_stable"
                    ) / (
                        CASE
                            WHEN "LS_Opening"."LS_loan_pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN
                                100000000
                            WHEN "LS_Opening"."LS_loan_pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN
                                1000000000
                            ELSE
                                1000000
                        END
                    )
                ) AS "Loan Interest"
            FROM (
                SELECT *
                FROM "LS_State" AS "s1"
                WHERE "s1"."LS_timestamp" = (
                    SELECT MAX("LS_timestamp")
                    FROM "LS_State" AS "s2"
                    WHERE
                        "s1"."LS_contract_id" = "s2"."LS_contract_id" AND
                        "LS_timestamp" > (NOW() - INTERVAL '1 hours')
                )
                ORDER BY "LS_timestamp"
            ) AS "Opened"
            LEFT JOIN "LS_Opening" ON "LS_Opening"."LS_contract_id" = "Opened"."LS_contract_id"
            WHERE "LS_amnt_stable" > 0
        )
        SELECT
            SUM(
                "Lease Value" -
                    "Loan" -
                    "Down Payment" -
                    "Margin Interest" -
                    "Loan Interest"
            ) AS "PnL"
        FROM "Lease_Value_Table" AS "lvt"
        LEFT JOIN "DP_Loan_Table" AS "dplt" ON "lvt"."Contract ID" = "dplt"."Contract ID"
        "#;

        sqlx::query_as(SQL)
            .fetch_optional(&self.pool)
            .await
            .map(|maybe_row| {
                maybe_row.map_or_else(BigDecimal::zero, |(result,)| result)
            })
    }

    pub async fn get_pnl_over_time(
        &self,
        contract_id: String,
        period: i64,
    ) -> Result<Vec<Pnl_Over_Time>, Error> {
        // FIXME Find a way to describe currencies' decimal places dynamically.
        const SQL: &str = r#"
        WITH "Active_Positions" AS (
            SELECT
                DISTINCT
                "LS_contract_id"
            FROM "LS_State"
            WHERE "LS_timestamp" >= (NOW() - INTERVAL '1 hour')
        ),
        "DP_Loan_Table" AS (
            SELECT
                "LS_Opening"."LS_contract_id" AS "Contract ID",
                (
                    "LS_principal_stable" / (
                        CASE
                            WHEN "LS_Opening"."LS_loan_pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN
                                100000000
                            WHEN "LS_Opening"."LS_loan_pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN
                                1000000000
                            ELSE
                                1000000
                        END
                    )
                ) AS "Loan",
                (
                    "LS_cltr_amnt_stable" / (
                        CASE
                            WHEN "LS_cltr_symbol" IN ('ALL_BTC', 'WBTC', 'CRO') THEN
                                100000000
                            WHEN "LS_cltr_symbol" IN ('ALL_SOL') THEN
                                1000000000
                            WHEN "LS_cltr_symbol" IN ('PICA') THEN
                                1000000000000
                            WHEN "LS_cltr_symbol" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS') THEN
                                1000000000000000000
                            ELSE
                                1000000
                        END
                    )
                ) AS "Down Payment",
                DATE_TRUNC('hour', "LS_State"."LS_timestamp") AS "Hour"
            FROM "LS_State"
            JOIN "LS_Opening" ON "LS_Opening"."LS_contract_id" = "LS_State"."LS_contract_id"
            WHERE
                "LS_State"."LS_contract_id" = $1 AND
                "LS_State"."LS_timestamp" >= (NOW() - INTERVAL ($2 || ' days'))
        ),
        "Lease_Value_Table" AS (
            SELECT
                "LS_Opening"."LS_contract_id" AS "Contract ID",
                (
                    "LS_amnt_stable" / (
                        CASE
                            WHEN "LS_asset_symbol" IN ('WBTC', 'CRO', 'ALL_BTC') THEN
                                100000000
                            WHEN "LS_asset_symbol" IN ('ALL_SOL') THEN
                                1000000000
                            WHEN "LS_asset_symbol" IN ('PICA') THEN
                                1000000000000
                            WHEN "LS_asset_symbol" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS') THEN
                                1000000000000000000
                            ELSE
                                1000000
                        END
                    )
                ) AS "Lease Value",
                (
                    (
                        "LS_prev_margin_stable" + "LS_current_margin_stable"
                    ) / (
                        CASE
                            WHEN "LS_Opening"."LS_loan_pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN
                                100000000
                            WHEN "LS_Opening"."LS_loan_pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN
                                1000000000
                            ELSE
                                1000000
                        END
                    )
                ) AS "Margin Interest",
                (
                    (
                        "LS_prev_interest_stable" + "LS_current_interest_stable"
                    ) / (
                        CASE
                            WHEN "LS_Opening"."LS_loan_pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN
                                100000000
                            WHEN "LS_Opening"."LS_loan_pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN
                                1000000000
                            ELSE
                                1000000
                        END
                    )
                ) AS "Loan Interest",
                DATE_TRUNC('hour', "LS_State"."LS_timestamp") AS "Hour"
            FROM "LS_State"
            JOIN "LS_Opening" ON "LS_Opening"."LS_contract_id" = "LS_State"."LS_contract_id"
            WHERE
                "LS_State"."LS_contract_id" = $1 AND
                "LS_State"."LS_timestamp" >= (NOW() - INTERVAL ($2 || ' days'))
        ),
        "Hourly_Unrealized_PnL" AS (
            SELECT
                DATE_TRUNC('hour', lvt."Hour") AS "Hour",
                DATE_TRUNC('day', lvt."Hour") AS "Day",
                lvt."Contract ID",
                SUM(
                    lvt."Lease Value" -
                        dplt."Loan" -
                        dplt."Down Payment" -
                        lvt."Margin Interest" -
                        lvt."Loan Interest"
                ) AS "Hourly Unrealized PnL"
            FROM "Lease_Value_Table" AS "lvt"
            LEFT JOIN DP_Loan_Table dplt ON
                lvt."Contract ID" = dplt."Contract ID" AND
                lvt."Hour" = dplt."Hour"
            GROUP BY
                DATE_TRUNC('hour', lvt."Hour"),
                DATE_TRUNC('day', lvt."Hour"),
                lvt."Contract ID"
        )
        SELECT
            "Day",
            AVG("Hourly Unrealized PnL") AS "Daily Unrealized PnL"
        FROM "Hourly_Unrealized_PnL"
        GROUP BY
            "Day",
            "Contract ID"
        ORDER BY "Day"
        "#;

        sqlx::query_as(SQL)
            .bind(contract_id)
            .bind(period)
            .fetch_all(&self.pool)
            .await
    }

    pub async fn get_unrealized_pnl_by_address(
        &self,
        address: String,
    ) -> Result<Vec<Unrealized_Pnl>, Error> {
        // FIXME Find a way to describe currencies' decimal places dynamically.
        const SQL: &str = r#"
        WITH "Filtered_Opening" AS (
            SELECT *
            FROM "LS_Opening"
            WHERE "LS_address_id" = $1
        ),
        "Active_Positions" AS (
            SELECT
                DISTINCT
                "LS_contract_id"
            FROM "LS_State"
            WHERE "LS_timestamp" >= (NOW() - INTERVAL '1 hour')
        ),
        "DP_Loan_Table" AS (
            SELECT
                "fo"."LS_contract_id" AS "Contract ID",
                (
                    "fs"."LS_principal_stable" / (
                        CASE
                            WHEN "fo"."LS_loan_pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN
                                100000000
                            WHEN "fo"."LS_loan_pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN
                                1000000000
                            ELSE
                                1000000
                        END
                    )
                ) AS "Loan",
                (
                    "fo"."LS_cltr_amnt_stable" / (
                        CASE
                            WHEN "fo"."LS_cltr_symbol" IN ('ALL_BTC', 'WBTC', 'CRO') THEN
                                100000000
                            WHEN "fo"."LS_cltr_symbol" IN ('ALL_SOL') THEN
                                1000000000
                            WHEN "fo"."LS_cltr_symbol" IN ('PICA') THEN
                                1000000000000
                            ELSE
                                1000000
                        END
                    )
                ) AS "Down Payment",
                DATE_TRUNC('hour', "fs"."LS_timestamp") AS "Hour",
                "fo"."LS_address_id" AS "Address ID"
            FROM "LS_State" AS "fs"
            JOIN Filtered_Opening "fo" ON "fo"."LS_contract_id" = "fs"."LS_contract_id"
            WHERE
                "fs"."LS_timestamp" >= (NOW() - INTERVAL '20 days') AND
                "fs"."LS_contract_id" IN (
                    SELECT "LS_contract_id"
                    FROM "Active_Positions"
                )
        ),
        "Lease_Value_Table" AS (
            SELECT
                "fo"."LS_contract_id" AS "Contract ID",
                (
                    "fs"."LS_amnt_stable" / (
                        CASE
                            WHEN "fo"."LS_asset_symbol" IN ('WBTC', 'CRO', 'ALL_BTC') THEN
                                100000000
                            WHEN "fo"."LS_asset_symbol" IN ('ALL_SOL') THEN
                                1000000000
                            ELSE
                                1000000
                        END
                    )
                ) AS "Lease Value",
                (
                    (
                        "fs"."LS_prev_margin_stable" + "fs"."LS_current_margin_stable"
                    ) / (
                        CASE
                            WHEN "fo"."LS_loan_pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN
                                100000000
                            WHEN "fo"."LS_loan_pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN
                                1000000000
                            ELSE
                                1000000
                        END
                    )
                ) AS "Margin Interest",
                (
                    (
                        "fs"."LS_prev_interest_stable" + "fs"."LS_current_interest_stable"
                    ) / (
                        CASE
                            WHEN "fo"."LS_loan_pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN
                                100000000
                            WHEN "fo"."LS_loan_pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN
                                1000000000
                            ELSE
                                1000000
                        END
                    )
                ) AS "Loan Interest",
                DATE_TRUNC('hour', "fs"."LS_timestamp") AS "Hour",
                "fo"."LS_address_id" AS "Address ID"
            FROM "LS_State" AS "fs"
            JOIN Filtered_Opening "fo" ON "fo"."LS_contract_id" = "fs"."LS_contract_id"
            WHERE
                "fs"."LS_timestamp" >= (NOW() - INTERVAL '20 days') AND
                "fs"."LS_contract_id" IN (
                    SELECT "LS_contract_id"
                    FROM "Active_Positions"
                )
        ),
        Hourly_Unrealized_PnL AS (
            SELECT
                DATE_TRUNC('hour', lvt."Hour") AS "Hour",
                DATE_TRUNC('day', lvt."Hour") AS "Day",
                "lvt"."Address ID",
                SUM(
                    "Lease Value" -
                        "Loan" -
                        "Down Payment" -
                        "Margin Interest" -
                        "Loan Interest"
                ) AS "Hourly Unrealized PnL"
            FROM "Lease_Value_Table" "lvt"
            LEFT JOIN "DP_Loan_Table" AS "dplt" ON
                "lvt"."Contract ID" = "dplt"."Contract ID" AND
                "lvt"."Hour" = "dplt"."Hour"
            GROUP BY
                DATE_TRUNC('hour', "lvt"."Hour"),
                DATE_TRUNC('day', "lvt"."Hour"),
                "lvt"."Address ID"
        )
        SELECT
            "Day",
            "Address ID",
            AVG("Hourly Unrealized PnL") AS "Daily Unrealized PnL"
        FROM Hourly_Unrealized_PnL
        GROUP BY "Day", "Address ID"
        ORDER BY "Day", "Address ID"
        "#;

        sqlx::query_as(SQL)
            .bind(address)
            .fetch_all(&self.pool)
            .await
    }

    // FIXME Pass argument by reference.
    // FIXME Resolve collision between implementations, by passing protocol
    //  descriptions dynamically.
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
    ) -> Result<BigDecimal, Error> {
        // FIXME Find a way to describe protocols dynamically.
        // FIXME Find a way to describe currencies' decimal places dynamically.
        const SQL: &str = r#"
        WITH "Lease_Value_Divisor" AS (
            SELECT
                "LS_asset_symbol",
                (
                    CASE
                        WHEN "LS_asset_symbol" IN ('WBTC', 'ALL_BTC', 'CRO') THEN
                            100000000
                        WHEN "LS_asset_symbol" IN ('ALL_SOL') THEN
                            1000000000
                        WHEN "LS_asset_symbol" IN ('PICA') THEN
                            1000000000000
                        WHEN "LS_asset_symbol" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS') THEN
                            1000000000000000000
                        ELSE
                            1000000
                    END
                ) AS "Divisor"
            FROM "LS_Opening"
            GROUP BY "LS_asset_symbol"
        ),
        "Lease_Value" AS (
            SELECT
                (
                    "s"."LS_amnt_stable" / "d"."Divisor"
                ) AS "Lease Value"
            FROM "LS_State" AS "s"
            LEFT JOIN "LS_Opening" AS "o" ON "o"."LS_contract_id" = "s"."LS_contract_id"
            LEFT JOIN "Lease_Value_Divisor" AS "d" ON "o"."LS_asset_symbol" = "d"."LS_asset_symbol"
            WHERE "s"."LS_timestamp" > (NOW() - INTERVAL '1 hours')
        ),
        "Available_Assets_Osmosis" AS (
            SELECT
                (
                    (
                        "LP_Pool_total_value_locked_stable" - "LP_Pool_total_borrowed_stable"
                    ) / 1000000
                ) AS "Available Assets"
            FROM "LP_Pool_State"
            WHERE "LP_Pool_id" = $1
            ORDER BY "LP_Pool_timestamp" DESC
            LIMIT 1
        ),
        "Available_Assets_Neutron" AS (
            SELECT
                (
                    (
                        "LP_Pool_total_value_locked_stable" - "LP_Pool_total_borrowed_stable"
                    ) / 1000000
                ) AS "Available Assets"
            FROM "LP_Pool_State"
            WHERE "LP_Pool_id" = $2
            ORDER BY "LP_Pool_timestamp" DESC
            LIMIT 1
        ),
        "Available_Osmosis_Noble" AS (
            SELECT
                (
                    (
                        "LP_Pool_total_value_locked_stable" - "LP_Pool_total_borrowed_stable"
                    ) / 1000000
                ) AS "Available Assets"
            FROM "LP_Pool_State"
            WHERE "LP_Pool_id" = $3
            ORDER BY "LP_Pool_timestamp" DESC
            LIMIT 1
        ),
        "Available_Neutron_Noble" AS (
            SELECT
                (
                    (
                        "LP_Pool_total_value_locked_stable" - "LP_Pool_total_borrowed_stable"
                    ) / 1000000
                ) AS "Available Assets"
            FROM "LP_Pool_State"
            WHERE "LP_Pool_id" = $4
            ORDER BY "LP_Pool_timestamp" DESC
            LIMIT 1
        ),
        "Available_ST_ATOM_OSMOSIS" AS (
            SELECT
                (
                    (
                        "LP_Pool_total_value_locked_stable" - "LP_Pool_total_borrowed_stable"
                    ) / 1000000
                ) AS "Available Assets"
            FROM "LP_Pool_State"
            WHERE "LP_Pool_id" = $5
            ORDER BY "LP_Pool_timestamp" DESC
            LIMIT 1
        ),
        "Available_ALL_BTC_OSMOSIS" AS (
            SELECT
                (
                    (
                        "LP_Pool_total_value_locked_stable" - "LP_Pool_total_borrowed_stable"
                    ) / 100000000
                ) AS "Available Assets"
            FROM "LP_Pool_State"
            WHERE "LP_Pool_id" = $6
            ORDER BY "LP_Pool_timestamp" DESC
            LIMIT 1
        ),
        "Available_ALL_SOL_OSMOSIS" AS (
            SELECT 
                (
                    (
                        "LP_Pool_total_value_locked_stable" - "LP_Pool_total_borrowed_stable"
                    ) / 1000000000
                ) AS "Available Assets"
            FROM "LP_Pool_State"
            WHERE "LP_Pool_id" = $7
            ORDER BY "LP_Pool_timestamp" DESC
            LIMIT 1
        ),
        "Available_AKT_OSMOSIS" AS (
            SELECT
                (
                    (
                        "LP_Pool_total_value_locked_stable" - "LP_Pool_total_borrowed_stable"
                    ) / 1000000
                ) AS "Available Assets"
            FROM "LP_Pool_State"
            WHERE "LP_Pool_id" = $8
            ORDER BY "LP_Pool_timestamp" DESC
            LIMIT 1
        ),
        "Lease_Value_Sum" AS (
            SELECT
                SUM("Lease Value") AS "Total Lease Value"
            FROM "Lease_Value"
        )
        SELECT (
            (
                SELECT *
                FROM "Lease_Value_Sum"
            ) + (
                SELECT *
                FROM "Available_Assets_Osmosis"
            ) + (
                SELECT *
                FROM "Available_Assets_Neutron"
            ) + (
                SELECT *
                FROM "Available_Osmosis_Noble"
            ) + (
                SELECT *
                FROM "Available_Neutron_Noble"
            ) + (
                SELECT *
                FROM "Available_ST_ATOM_OSMOSIS"
            ) + (
                SELECT *
                FROM "Available_ALL_BTC_OSMOSIS"
            ) + (
                SELECT *
                FROM "Available_ALL_SOL_OSMOSIS"
            ) + (
                SELECT *
                FROM "Available_AKT_OSMOSIS"
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
            .fetch_optional(&self.pool)
            .await
            .map(|result| {
                result.map_or_else(BigDecimal::zero, |(result,)| result)
            })
    }

    // FIXME Pass argument by reference.
    // FIXME Resolve collision between implementations, by passing protocol
    //  descriptions dynamically.
    #[cfg(feature = "testnet")]
    pub async fn get_total_value_locked(
        &self,
        osmosis_usdc_protocol: String,
        neutron_axelar_protocol: String,
    ) -> Result<BigDecimal, Error> {
        // FIXME Find a way to describe protocols dynamically.
        // FIXME Find a way to describe currencies' decimal places dynamically.
        const SQL: &str = r#"
        WITH "Lease_Value_Divisor" AS (
            SELECT
                "LS_asset_symbol",
                (
                    CASE
                        WHEN "LS_asset_symbol" IN ('WBTC', 'CRO') THEN
                            100000000
                        WHEN "LS_asset_symbol" IN ('PICA') THEN
                            1000000000000
                        WHEN "LS_asset_symbol" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS') THEN
                            1000000000000000000
                        ELSE
                            1000000
                    END
                ) AS "Divisor"
            FROM "LS_Opening"
            GROUP BY "LS_asset_symbol"
        ),
        "Lease_Value" AS (
            SELECT
                (
                    "s"."LS_amnt_stable" / "d"."Divisor"
                ) AS "Lease Value"
            FROM "LS_State" AS "s"
            LEFT JOIN "LS_Opening" "o" ON "o"."LS_contract_id" = "s"."LS_contract_id"
            LEFT JOIN Lease_Value_Divisor "d" ON "o"."LS_asset_symbol" = "d"."LS_asset_symbol"
            WHERE "s"."LS_timestamp" > (NOW() - INTERVAL '1 hours')
        ),
        "Available_Assets_Osmosis" AS (
            SELECT
                (
                    (
                        "LP_Pool_total_value_locked_stable" - "LP_Pool_total_borrowed_stable"
                    ) / 1000000
                ) AS "Available Assets"
            FROM "LP_Pool_State"
            WHERE "LP_Pool_id" = $1
            ORDER BY "LP_Pool_timestamp" DESC
            LIMIT 1
        ),
        "Available_Assets_Neutron" AS (
            SELECT
                (
                    (
                        "LP_Pool_total_value_locked_stable" - "LP_Pool_total_borrowed_stable"
                    ) / 1000000
                ) AS "Available Assets"
            FROM "LP_Pool_State"
            WHERE "LP_Pool_id" = $2
            ORDER BY "LP_Pool_timestamp" DESC
            LIMIT 1
        ),
        "Lease_Value_Sum" AS (
            SELECT
                SUM("Lease Value") AS "Total Lease Value"
            FROM "Lease_Value"
        )
        SELECT (
            (
                SELECT *
                FROM "Lease_Value_Sum"
            ) + (
                SELECT *
                FROM "Available_Assets_Osmosis"
            ) + (
                SELECT *
                FROM "Available_Assets_Neutron"
            )
        ) AS "TVL"
        "#;

        sqlx::query_as(SQL)
            .bind(osmosis_usdc_protocol)
            .bind(neutron_axelar_protocol)
            .fetch_optional(&self.pool)
            .await
            .map(|result| {
                result.map_or_else(BigDecimal::zero, |(result,)| result)
            })
    }
}
