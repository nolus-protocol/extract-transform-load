use std::str::FromStr as _;

use bigdecimal::BigDecimal;
use sqlx::{Error, Transaction};

use crate::model::{LS_Loan_Closing, Pnl_Result, Table};

use super::{DataBase, QueryResult};

impl Table<LS_Loan_Closing> {
    pub async fn isExists(
        &self,
        contract: String,
    ) -> Result<bool, crate::error::Error> {
        let (value,): (i64,) = sqlx::query_as(
            r#"
            SELECT 
                COUNT(*)
            FROM "LS_Loan_Closing" 
            WHERE 
                "LS_contract_id" = $1
            "#,
        )
        .bind(contract)
        .persistent(false)
        .fetch_one(&self.pool)
        .await?;

        if value > 0 {
            return Ok(true);
        }

        Ok(false)
    }

    pub async fn insert(
        &self,
        data: LS_Loan_Closing,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<QueryResult, Error> {
        sqlx::query(
            r#"
            INSERT INTO "LS_Loan_Closing" (
                "LS_contract_id",
                "LS_amnt",
                "LS_amnt_stable",
                "LS_pnl",
                "LS_timestamp",
                "Type",
                "Block",
                "Active"
            )
            VALUES($1, $2, $3, $4, $5, $6, $7, $8)
        "#,
        )
        .bind(&data.LS_contract_id)
        .bind(&data.LS_amnt)
        .bind(&data.LS_amnt_stable)
        .bind(&data.LS_pnl)
        .bind(&data.LS_timestamp)
        .bind(&data.Type)
        .bind(&data.Block)
        .bind(&data.Active)
        .persistent(false)
        .execute(&mut **transaction)
        .await
    }

    pub async fn get_lease_amount(
        &self,
        contract_id: String,
    ) -> Result<BigDecimal, crate::error::Error> {
        let value: (Option<BigDecimal>,) = sqlx::query_as(
            r#"
                SELECT SUM("Amount") as "Total" FROM (
                    SELECT
                    SUM("LS_loan_amnt") as "Amount"
                    FROM "LS_Opening"
                    WHERE "LS_contract_id" = $1
                UNION ALL
                    SELECT
                    -SUM("LS_amnt") as "Amount"
                    FROM "LS_Close_Position"
                    WHERE "LS_contract_id" = $1
                UNION ALL
                    SELECT
                    -SUM("LS_amnt") as "Amount"
                    FROM "LS_Liquidation"
                    WHERE "LS_contract_id" = $1
                ) AS combined_data
            "#,
        )
        .bind(contract_id)
        .persistent(false)
        .fetch_one(&self.pool)
        .await?;
        let (amnt,) = value;
        let amnt = amnt.unwrap_or(BigDecimal::from_str("0")?);

        Ok(amnt)
    }

    pub async fn get_realized_pnl_stats(
        &self,
    ) -> Result<BigDecimal, crate::error::Error> {
        let value: (Option<BigDecimal>,) = sqlx::query_as(
            r#"
                SELECT 
                SUM(
                    CASE
                    WHEN o."LS_asset_symbol" IN ('ALL_BTC', 'WBTC', 'CRO') THEN c."LS_pnl" / 100000000
                    WHEN o."LS_asset_symbol" IN ('ALL_SOL') THEN c."LS_pnl" / 1000000000
                    WHEN o."LS_asset_symbol" IN ('PICA') THEN c."LS_pnl" / 1000000000000
                    WHEN o."LS_asset_symbol" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS', 'ALL_ETH') THEN c."LS_pnl" / 1000000000000000000
                    ELSE c."LS_pnl" / 1000000
                    END
                ) AS "Total Adjusted Stable Amount"
                FROM 
                "LS_Loan_Closing" c
                LEFT JOIN 
                "LS_Opening" o 
                ON 
                c."LS_contract_id" = o."LS_contract_id"
                WHERE 
                c."LS_timestamp" >= '2025-01-01';
            "#,
        )
        .persistent(false)
        .fetch_one(&self.pool)
        .await?;
        let (amnt,) = value;
        let amnt = amnt.unwrap_or(BigDecimal::from_str("0")?);

        Ok(amnt)
    }

    pub async fn update(
        &self,
        data: LS_Loan_Closing,
    ) -> Result<QueryResult, Error> {
        sqlx::query(
            r#"
            UPDATE 
                "LS_Loan_Closing"
            SET
                "LS_amnt" = $1,
                "LS_amnt_stable" = $2,
                "LS_pnl" = $3,
                "Active" = $4
            WHERE 
                "LS_contract_id" = $5

        "#,
        )
        .bind(&data.LS_amnt)
        .bind(&data.LS_amnt_stable)
        .bind(&data.LS_pnl)
        .bind(&data.Active)
        .bind(&data.LS_contract_id)
        .persistent(false)
        .execute(&self.pool)
        .await
    }

    pub async fn get_leases_to_proceed(
        &self,
    ) -> Result<Vec<LS_Loan_Closing>, Error> {
        let data = sqlx::query_as(
            r#"
            SELECT * FROM "LS_Loan_Closing" WHERE "Active" = false;
            "#,
        )
        .persistent(false)
        .fetch_all(&self.pool)
        .await?;
        Ok(data)
    }

    pub async fn get_leases(
        &self,
        address: String,
        skip: i64,
        limit: i64,
    ) -> Result<Vec<Pnl_Result>, Error> {
        let data = sqlx::query_as(
            r#"
                WITH
                pool_map AS (
                SELECT * FROM (
                    SELECT 'nolus1jufcaqm6657xmfltdezzz85quz92rmtd88jk5x0hq9zqseem32ysjdm990'::text AS id, 'ST_ATOM'::text AS symbol
                    UNION ALL SELECT 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3', 'ALL_BTC'
                    UNION ALL SELECT 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm', 'ALL_SOL'
                    UNION ALL SELECT 'nolus1lxr7f5xe02jq6cce4puk6540mtu9sg36at2dms5sk69wdtzdrg9qq0t67z', 'AKT'
                    UNION ALL SELECT 'nolus1u0zt8x3mkver0447glfupz9lz6wnt62j70p5fhhtu3fr46gcdd9s5dz9l6', 'ATOM'
                    UNION ALL SELECT 'nolus1py7pxw74qvlgq0n6rfz7mjrhgnls37mh87wasg89n75qt725rams8yr46t', 'OSMO'
                ) p
                ),

                openings AS (
                SELECT
                    o."LS_contract_id",
                    o."LS_timestamp"                              AS open_ts,
                    o."LS_asset_symbol",
                    o."LS_loan_amnt",
                    o."LS_cltr_symbol",
                    o."LS_cltr_amnt_stable",
                    o."LS_loan_pool_id",
                    o."Tx_Hash"                                   AS open_tx_hash
                FROM "LS_Opening" o
                WHERE o."LS_address_id" = $1
                ),

                repayments AS (
                SELECT
                    r."LS_contract_id",
                    SUM(
                    CASE
                        WHEN r."LS_payment_symbol" IN ('ALL_BTC','WBTC','CRO') THEN r."LS_payment_amnt_stable" / 100000000.0
                        WHEN r."LS_payment_symbol" = 'ALL_SOL'                 THEN r."LS_payment_amnt_stable" / 1000000000.0
                        WHEN r."LS_payment_symbol" = 'PICA'                    THEN r."LS_payment_amnt_stable" / 1000000000000.0
                        WHEN r."LS_payment_symbol" IN ('WETH','EVMOS','INJ','DYDX','DYM','CUDOS','ALL_ETH')
                                                                        THEN r."LS_payment_amnt_stable" / 1000000000000000000.0
                        ELSE r."LS_payment_amnt_stable" / 1000000.0
                    END
                    ) AS total_repaid_usdc
                FROM "LS_Repayment" r
                GROUP BY r."LS_contract_id"
                ),

                collects AS (
                SELECT
                    lc."LS_contract_id",
                    SUM(
                    CASE
                        WHEN lc."LS_symbol" IN ('ALL_BTC','WBTC','CRO') THEN lc."LS_amount_stable" / 100000000.0
                        WHEN lc."LS_symbol" = 'ALL_SOL'                 THEN lc."LS_amount_stable" / 1000000000.0
                        WHEN lc."LS_symbol" = 'PICA'                    THEN lc."LS_amount_stable" / 1000000000000.0
                        WHEN lc."LS_symbol" IN ('WETH','EVMOS','INJ','DYDX','DYM','CUDOS','ALL_ETH')
                                                                        THEN lc."LS_amount_stable" / 1000000000000000000.0
                        ELSE lc."LS_amount_stable" / 1000000.0
                    END
                    )::numeric(38,8) AS total_collected_usdc
                FROM "LS_Loan_Collect" lc
                GROUP BY lc."LS_contract_id"
                ),

                closing_ts AS (
                SELECT c."LS_contract_id", c."LS_timestamp" AS close_ts, c."Type"
                FROM "LS_Loan_Closing" c
                )

                SELECT
                o."LS_contract_id"                                                        AS "Position ID",
                o."LS_asset_symbol",
                o."LS_loan_pool_id",
                ct."Type",
                ct.close_ts                                                               AS "LS_timestamp",
                to_char(ct.close_ts, 'YYYY-MM-DD HH24:MI UTC')                            AS "Close Date UTC",
                (
                    (CASE
                    WHEN o."LS_cltr_symbol" IN ('ALL_BTC','WBTC','CRO') THEN o."LS_cltr_amnt_stable" / 100000000.0
                    WHEN o."LS_cltr_symbol" = 'ALL_SOL'                 THEN o."LS_cltr_amnt_stable" / 1000000000.0
                    WHEN o."LS_cltr_symbol" = 'PICA'                    THEN o."LS_cltr_amnt_stable" / 1000000000000.0
                    WHEN o."LS_cltr_symbol" IN ('WETH','EVMOS','INJ','DYDX','DYM','CUDOS','ALL_ETH')
                                                                        THEN o."LS_cltr_amnt_stable" / 1000000000000000000.0
                    ELSE o."LS_cltr_amnt_stable" / 1000000.0
                    END)::numeric(38,8)
                    + COALESCE(r.total_repaid_usdc, 0::numeric(38,8))
                )::double precision                                                          AS "Sent (USDC, Opening)",
                COALESCE(c.total_collected_usdc, 0::numeric(38,8))::double precision          AS "Received (USDC, Closing)",
                (
                    COALESCE(c.total_collected_usdc, 0::numeric(38,8))
                    - (
                        (CASE
                        WHEN o."LS_cltr_symbol" IN ('ALL_BTC','WBTC','CRO') THEN o."LS_cltr_amnt_stable" / 100000000.0
                        WHEN o."LS_cltr_symbol" = 'ALL_SOL'                 THEN o."LS_cltr_amnt_stable" / 1000000000.0
                        WHEN o."LS_cltr_symbol" = 'PICA'                    THEN o."LS_cltr_amnt_stable" / 1000000000000.0
                        WHEN o."LS_cltr_symbol" IN ('WETH','EVMOS','INJ','DYDX','DYM','CUDOS','ALL_ETH')
                                                                        THEN o."LS_cltr_amnt_stable" / 1000000000000000000.0
                        ELSE o."LS_cltr_amnt_stable" / 1000000.0
                        END)::numeric(38,8)
                        + COALESCE(r.total_repaid_usdc, 0::numeric(38,8))
                    )
                )::double precision                                                           AS "Realized PnL (USDC)"
                FROM openings o
                LEFT JOIN repayments r ON r."LS_contract_id" = o."LS_contract_id"
                LEFT JOIN collects   c ON c."LS_contract_id" = o."LS_contract_id"
                INNER JOIN closing_ts ct ON ct."LS_contract_id" = o."LS_contract_id"
                ORDER BY ct.close_ts DESC
                OFFSET 
                    $2 
                LIMIT 
                    $3
            "#,
        )
        .bind(address)
        .bind(skip)
        .bind(limit)
        .persistent(false)
        .fetch_all(&self.pool)
        .await?;
        Ok(data)
    }

    pub async fn get_realized_pnl(
        &self,
        address: String,
    ) -> Result<f64, crate::error::Error> {
        let value: (Option<f64>,) = sqlx::query_as(
            r#"
                WITH
                -- Map loan pools -> shorted asset (kept for parity; not needed for the math)
                pool_map AS (
                SELECT * FROM (
                    SELECT 'nolus1jufcaqm6657xmfltdezzz85quz92rmtd88jk5x0hq9zqseem32ysjdm990'::text AS id, 'ST_ATOM'::text AS symbol
                    UNION ALL SELECT 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3', 'ALL_BTC'
                    UNION ALL SELECT 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm', 'ALL_SOL'
                    UNION ALL SELECT 'nolus1lxr7f5xe02jq6cce4puk6540mtu9sg36at2dms5sk69wdtzdrg9qq0t67z', 'AKT'
                    UNION ALL SELECT 'nolus1u0zt8x3mkver0447glfupz9lz6wnt62j70p5fhhtu3fr46gcdd9s5dz9l6', 'ATOM'
                    UNION ALL SELECT 'nolus1py7pxw74qvlgq0n6rfz7mjrhgnls37mh87wasg89n75qt725rams8yr46t', 'OSMO'
                ) p
                ),

                -- Openings for this wallet (this is the *opening* side)
                openings AS (
                SELECT
                    o."LS_contract_id",
                    o."LS_timestamp"                              AS open_ts,
                    o."LS_asset_symbol",
                    o."LS_loan_amnt",
                    o."LS_cltr_symbol",
                    o."LS_cltr_amnt_stable",
                    o."LS_loan_pool_id",
                    o."Tx_Hash"                                   AS open_tx_hash
                FROM "LS_Opening" o
                WHERE o."LS_address_id" = $1
                ),

                -- USDC repayments (part of *sent/opening* cash flow)
                repayments AS (
                SELECT
                    r."LS_contract_id",
                    SUM(
                    CASE
                        WHEN r."LS_payment_symbol" IN ('ALL_BTC','WBTC','CRO') THEN r."LS_payment_amnt_stable" / 100000000.0
                        WHEN r."LS_payment_symbol" = 'ALL_SOL'                 THEN r."LS_payment_amnt_stable" / 1000000000.0
                        WHEN r."LS_payment_symbol" = 'PICA'                    THEN r."LS_payment_amnt_stable" / 1000000000000.0
                        WHEN r."LS_payment_symbol" IN ('WETH','EVMOS','INJ','DYDX','DYM','CUDOS','ALL_ETH')
                                                                        THEN r."LS_payment_amnt_stable" / 1000000000000000000.0
                        ELSE r."LS_payment_amnt_stable" / 1000000.0
                    END
                    ) AS total_repaid_usdc
                FROM "LS_Repayment" r
                GROUP BY r."LS_contract_id"
                ),

                -- User collects (this is the *closing* side cash flow, in USDC normalized)
                collects AS (
                SELECT
                    lc."LS_contract_id",
                    SUM(
                    CASE
                        WHEN lc."LS_symbol" IN ('ALL_BTC','WBTC','CRO') THEN lc."LS_amount_stable" / 100000000.0
                        WHEN lc."LS_symbol" = 'ALL_SOL'                 THEN lc."LS_amount_stable" / 1000000000.0
                        WHEN lc."LS_symbol" = 'PICA'                    THEN lc."LS_amount_stable" / 1000000000000.0
                        WHEN lc."LS_symbol" IN ('WETH','EVMOS','INJ','DYDX','DYM','CUDOS','ALL_ETH')
                                                                        THEN lc."LS_amount_stable" / 1000000000000000000.0
                        ELSE lc."LS_amount_stable" / 1000000.0
                    END
                    )::numeric(38,8) AS total_collected_usdc
                FROM "LS_Loan_Collect" lc
                GROUP BY lc."LS_contract_id"
                ),

                -- Close timestamp (a position is realized only if it has a close)
                closing_ts AS (
                SELECT c."LS_contract_id", c."LS_timestamp" AS close_ts
                FROM "LS_Loan_Closing" c
                ),

                -- Compute opening-sent and closing-received per position
                position_flows AS (
                SELECT
                    o."LS_contract_id"                            AS position_id,
                    -- Sent at opening: normalized collateral + total repayments (USDC)
                    (
                    (CASE
                        WHEN o."LS_cltr_symbol" IN ('ALL_BTC','WBTC','CRO') THEN o."LS_cltr_amnt_stable" / 100000000.0
                        WHEN o."LS_cltr_symbol" = 'ALL_SOL'                 THEN o."LS_cltr_amnt_stable" / 1000000000.0
                        WHEN o."LS_cltr_symbol" = 'PICA'                    THEN o."LS_cltr_amnt_stable" / 1000000000000.0
                        WHEN o."LS_cltr_symbol" IN ('WETH','EVMOS','INJ','DYDX','DYM','CUDOS','ALL_ETH')
                                                                        THEN o."LS_cltr_amnt_stable" / 1000000000000000000.0
                        ELSE o."LS_cltr_amnt_stable" / 1000000.0
                    END)::numeric(38,8)
                    + COALESCE(r.total_repaid_usdc, 0::numeric(38,8))
                    )                                               AS sent_open_usdc,
                    -- Received at closing: normalized collects (USDC). Zero if liquidated without collects.
                    COALESCE(c.total_collected_usdc, 0::numeric(38,8)) AS received_close_usdc
                FROM openings o
                LEFT JOIN repayments r ON r."LS_contract_id" = o."LS_contract_id"
                LEFT JOIN collects   c ON c."LS_contract_id" = o."LS_contract_id"
                INNER JOIN closing_ts ct ON ct."LS_contract_id" = o."LS_contract_id"
                )

                SELECT
                (SUM(received_close_usdc) - SUM(sent_open_usdc))::double precision
                    AS "Total Realized PnL (USDC)"
                FROM position_flows
            "#,
        )
        .bind(address)
        .persistent(false)
        .fetch_one(&self.pool)
        .await?;

        let (amnt,) = value;
        let amnt = amnt.unwrap_or(0.0);

        Ok(amnt)
    }

    pub async fn get_all(&self) -> Result<Vec<LS_Loan_Closing>, Error> {
        sqlx::query_as(
            r#"SELECT * FROM "LS_Loan_Closing" WHERE "Block" <= 3785599"#, //<= 3785599
        )
        .persistent(false)
        .fetch_all(&self.pool)
        .await
    }

    pub async fn get(
        &self,
        contract: String,
    ) -> Result<LS_Loan_Closing, Error> {
        sqlx::query_as(
            r#"SELECT * FROM "LS_Loan_Closing" WHERE "LS_contract_id" = $1"#,
        )
        .bind(contract)
        .persistent(false)
        .fetch_one(&self.pool)
        .await
    }
}
