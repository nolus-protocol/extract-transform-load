use std::str::FromStr;

use sqlx::{Error, QueryBuilder, Transaction};

use crate::{
    model::{CosmosTypes, Raw_Message, Table},
    types::Bucket_Type,
};

use super::{DataBase, QueryResult};

impl Table<Raw_Message> {
    pub async fn insert(
        &self,
        data: Raw_Message,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<QueryResult, Error> {
        sqlx::query(
            r#"
            INSERT INTO "raw_message" ("index", "from", "to", "tx_hash", "type", "value", "block", "fee_amount", "fee_denom", "memo", "timestamp", "rewards")
            VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            "#,
        )
        .bind(data.index)
        .bind(&data.from)
        .bind(&data.to)
        .bind(&data.tx_hash)
        .bind(&data.r#type)
        .bind(&data.value)
        .bind(&data.block)
        .bind(&data.fee_amount)
        .bind(&data.fee_denom)
        .bind(&data.memo)
        .bind(&data.timestamp)
        .bind(&data.rewards)
        .persistent(false)
        .execute(&mut **transaction)
        .await
    }

    pub async fn insert_if_not_exists(
        &self,
        data: Raw_Message,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<QueryResult, Error> {
        sqlx::query(
            r#"
            INSERT INTO "raw_message" ("index", "from", "to", "tx_hash", "type", "value", "block", "fee_amount", "fee_denom", "memo", "timestamp", "rewards")
            VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT ("index", "tx_hash") DO NOTHING
            "#,
        )
        .bind(data.index)
        .bind(&data.from)
        .bind(&data.to)
        .bind(&data.tx_hash)
        .bind(&data.r#type)
        .bind(&data.value)
        .bind(&data.block)
        .bind(&data.fee_amount)
        .bind(&data.fee_denom)
        .bind(&data.memo)
        .bind(&data.timestamp)
        .bind(&data.rewards)
        .persistent(true)
        .execute(&mut **transaction)
        .await
    }

    pub async fn isExists(
        &self,
        data: &Raw_Message,
    ) -> Result<bool, crate::error::Error> {
        let (value,): (i64,) = sqlx::query_as(
            r#"
            SELECT 
                COUNT(*)
            FROM "raw_message" 
            WHERE 
                "index" = $1 AND
                "tx_hash" = $2
            "#,
        )
        .bind(data.index)
        .bind(&data.tx_hash)
        .persistent(false)
        .fetch_one(&self.pool)
        .await?;

        if value > 0 {
            return Ok(true);
        }

        Ok(false)
    }

    pub async fn get(
        &self,
        address: String,
        skip: i64,
        limit: i64,
        filter: Vec<String>,
        to: Vec<String>,
        combine: bool,
    ) -> Result<Vec<Raw_Message>, Error> {
        let mut filters: Vec<String> = Vec::new();

        for f in filter {
            match CosmosTypes::from_str(&f)? {
                CosmosTypes::MsgExecuteContract => {},
                _ => filters.push(f),
            }
        }

        let mut qb = QueryBuilder::new(
            r#"
        SELECT *
        FROM "raw_message"
        WHERE ("from" = "#,
        );

        qb.push_bind(&address)
            .push(r#" OR "to" = "#)
            .push_bind(&address)
            .push(")");

        let has_filters = !filters.is_empty();
        let has_to = !to.is_empty();
        let earn_type = CosmosTypes::MsgExecuteContract.to_string();

        if has_filters && has_to && combine {
            qb.push(" AND (");

            qb.push(" \"type\" = ANY(")
                .push_bind(filters.as_slice())
                .push(")");

            qb.push(" OR (");

            qb.push(" \"type\" = ").push_bind(&earn_type);

            qb.push(" AND \"to\" = ANY(")
                .push_bind(to.as_slice())
                .push(")");

            qb.push(")");
            qb.push(")");
        } else {
            if has_filters {
                qb.push(" AND \"type\" = ANY(")
                    .push_bind(filters.as_slice())
                    .push(")");
            }

            if has_to {
                qb.push(" AND \"to\" = ANY(")
                    .push_bind(to.as_slice())
                    .push(")");
            }
        }

        qb.push(r#" ORDER BY "timestamp" DESC OFFSET "#)
            .push_bind(skip)
            .push(" LIMIT ")
            .push_bind(limit);

        let query = qb.build_query_as::<Raw_Message>();
        let rows = query.persistent(false).fetch_all(&self.pool).await?;

        Ok(rows)
    }

    pub async fn get_tx_volume(
        &self,
        address: String,
    ) -> Result<f64, crate::error::Error> {
        let value: (Option<f64>,) = sqlx::query_as(
            r#"
                WITH
                pool_map AS (
                SELECT * FROM (
                    SELECT 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3'::text AS id, 'ALL_BTC'::text AS symbol
                    UNION ALL SELECT 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm', 'ALL_SOL'
                    UNION ALL SELECT 'nolus1jufcaqm6657xmfltdezzz85quz92rmtd88jk5x0hq9zqseem32ysjdm990', 'ST_ATOM'
                    UNION ALL SELECT 'nolus1lxr7f5xe02jq6cce4puk6540mtu9sg36at2dms5sk69wdtzdrg9qq0t67z', 'AKT'
                    UNION ALL SELECT 'nolus1u0zt8x3mkver0447glfupz9lz6wnt62j70p5fhhtu3fr46gcdd9s5dz9l6', 'ATOM'
                    UNION ALL SELECT 'nolus1py7pxw74qvlgq0n6rfz7mjrhgnls37mh87wasg89n75qt725rams8yr46t', 'OSMO'
                ) p
                ),
                openings AS (
                SELECT
                    o."LS_contract_id",
                    o."LS_address_id",
                    CASE
                    WHEN o."LS_cltr_symbol" IN ('ALL_BTC','WBTC','CRO') THEN o."LS_cltr_amnt_stable" / 100000000.0
                    WHEN o."LS_cltr_symbol" = 'ALL_SOL'                 THEN o."LS_cltr_amnt_stable" / 1000000000.0
                    WHEN o."LS_cltr_symbol" = 'PICA'                    THEN o."LS_cltr_amnt_stable" / 1000000000000.0
                    WHEN o."LS_cltr_symbol" IN ('WETH','EVMOS','INJ','DYDX','DYM','CUDOS','ALL_ETH')
                                                                        THEN o."LS_cltr_amnt_stable" / 1000000000000000000.0
                    ELSE o."LS_cltr_amnt_stable" / 1000000.0
                    END::double precision AS down_payment_usdc,
                    CASE
                    WHEN o."LS_loan_pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN o."LS_loan_amnt_stable" / 100000000.0  -- ALL_BTC
                    WHEN o."LS_loan_pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN o."LS_loan_amnt_stable" / 1000000000.0  -- ALL_SOL
                    WHEN o."LS_loan_pool_id" = 'nolus1lxr7f5xe02jq6cce4puk6540mtu9sg36at2dms5sk69wdtzdrg9qq0t67z' THEN o."LS_loan_amnt_stable" / 1000000.0    -- AKT
                    ELSE o."LS_loan_amnt_asset" / 1000000.0
                    END::double precision AS loan_usdc
                FROM "LS_Opening" o
                WHERE o."LS_address_id" = $1
                ),
                repayments AS (
                SELECT
                    r."LS_contract_id",
                    CASE
                    WHEN r."LS_payment_symbol" IN ('ALL_BTC','WBTC','CRO') THEN r."LS_payment_amnt_stable" / 100000000.0
                    WHEN r."LS_payment_symbol" = 'ALL_SOL'                 THEN r."LS_payment_amnt_stable" / 1000000000.0
                    WHEN r."LS_payment_symbol" = 'PICA'                    THEN r."LS_payment_amnt_stable" / 1000000000000.0
                    WHEN r."LS_payment_symbol" IN ('WETH','EVMOS','INJ','DYDX','DYM','CUDOS','ALL_ETH')
                                                                        THEN r."LS_payment_amnt_stable" / 1000000000000000000.0
                    ELSE r."LS_payment_amnt_stable" / 1000000.0
                    END::double precision AS repayment_usdc
                FROM "LS_Repayment" r
                INNER JOIN openings o USING ("LS_contract_id")
                ),
                closes AS (
                SELECT
                    c."LS_contract_id",
                    CASE
                    WHEN c."LS_amnt_symbol" IN ('ALL_BTC','WBTC','CRO') THEN c."LS_amnt_stable" / 100000000.0
                    WHEN c."LS_amnt_symbol" = 'ALL_SOL'                 THEN c."LS_amnt_stable" / 1000000000.0
                    WHEN c."LS_amnt_symbol" = 'PICA'                    THEN c."LS_amnt_stable" / 1000000000000.0
                    WHEN c."LS_amnt_symbol" IN ('WETH','EVMOS','INJ','DYDX','DYM','CUDOS','ALL_ETH')
                                                                        THEN c."LS_amnt_stable" / 1000000000000000000.0
                    ELSE c."LS_amnt_stable" / 1000000.0
                    END::double precision AS close_usdc
                FROM "LS_Close_Position" c
                INNER JOIN openings o USING ("LS_contract_id")
                )
                SELECT
                SUM(vol) AS "Tx Volume"
                FROM (
                SELECT 'OPEN' AS src, (o.down_payment_usdc + o.loan_usdc) AS vol FROM openings o
                UNION ALL
                SELECT 'REPAYMENT', r.repayment_usdc FROM repayments r
                UNION ALL
                SELECT 'CLOSE', c.close_usdc FROM closes c
                ) x
            "#,
        )
        .persistent(false)
        .bind(address)
        .fetch_one(&self.pool)
        .await?;
        let (amnt,) = value;
        let amnt = amnt.unwrap_or(0.0);

        Ok(amnt)
    }

    pub async fn get_win_rate(
        &self,
        address: String,
    ) -> Result<f64, crate::error::Error> {
        let value: (Option<f64>,) = sqlx::query_as(
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
                    (SUM(r."LS_payment_amnt_stable") / 1000000.0)::numeric(38,8) AS total_repaid_usdc
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
                SELECT c."LS_contract_id", c."LS_timestamp" AS close_ts
                FROM "LS_Loan_Closing" c
                ),

                finalized as (SELECT
                o."LS_contract_id"                                                        AS "Position ID",
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
                ORDER BY ct.close_ts, "Position ID")

                SELECT
                (COUNT(CASE WHEN "Realized PnL (USDC)" > 0 THEN 1 END)::float
                / COUNT(*)::float) * 100 AS "Winrate (%)"
                FROM finalized
            "#,
        )
        .persistent(false)
        .bind(address)
        .fetch_one(&self.pool)
        .await?;
        let (amnt,) = value;
        let amnt = amnt.unwrap_or(0.0);

        Ok(amnt)
    }

    pub async fn get_buckets(
        &self,
        address: String,
    ) -> Result<Vec<Bucket_Type>, crate::error::Error> {
        let data = sqlx::query_as(
            r#"
            WITH
            pool_map AS (
            SELECT * FROM (
              SELECT 'nolus1jufcaqm6657xmfltdezzz85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3'::text AS id, 'ALL_BTC'::text AS symbol
              UNION ALL SELECT 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm', 'ALL_SOL'
              UNION ALL SELECT 'nolus1jufcaqm6657xmfltdezzz85quz92rmtd88jk5x0hq9zqseem32ysjdm990', 'ST_ATOM'
              UNION ALL SELECT 'nolus1lxr7f5xe02jq6cce4puk6540mtu9sg36at2dms5sk69wdtzdrg9qq0t67z', 'AKT'
              UNION ALL SELECT 'nolus1u0zt8x3mkver0447glfupz9lz6wnt62j70p5fhhtu3fr46gcdd9s5dz9l6', 'ATOM'
              UNION ALL SELECT 'nolus1py7pxw74qvlgq0n6rfz7mjrhgnls37mh87wasg89n75qt725rams8yr46t', 'OSMO'
            ) p
            ),
            buckets AS (
            SELECT 1 AS ord, '<0'    AS bucket UNION ALL
            SELECT 2,        '0-50'               UNION ALL
            SELECT 3,        '51–100'             UNION ALL
            SELECT 4,        '101–300'            UNION ALL
            SELECT 5,        '301+'
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
              (SUM(r."LS_payment_amnt_stable") / 1000000.0)::numeric(38,8) AS total_repaid_usdc
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
            SELECT c."LS_contract_id", c."LS_timestamp" AS close_ts
            FROM "LS_Loan_Closing" c
            ),
            finalized AS (
            SELECT
              o."LS_contract_id" AS position_id,
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
              )::double precision AS sent_usdc,
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
              )::double precision AS realized_pnl_usdc
            FROM openings o
            LEFT JOIN repayments r ON r."LS_contract_id" = o."LS_contract_id"
            LEFT JOIN collects   c ON c."LS_contract_id" = o."LS_contract_id"
            INNER JOIN closing_ts ct ON ct."LS_contract_id" = o."LS_contract_id"
            ),
            with_pct AS (
            SELECT
              position_id,
              sent_usdc,
              realized_pnl_usdc,
              CASE
                WHEN sent_usdc = 0 THEN NULL
                ELSE (realized_pnl_usdc / sent_usdc) * 100.0
              END AS pnl_pct
            FROM finalized
            ),
            counts AS (
            SELECT
              CASE
                WHEN pnl_pct < 0 THEN '<0'
                WHEN pnl_pct >= 0   AND pnl_pct < 50  THEN '0-50'
                WHEN pnl_pct >= 50  AND pnl_pct < 100 THEN '51–100'
                WHEN pnl_pct >= 100 AND pnl_pct <= 300 THEN '101–300'
                WHEN pnl_pct > 300 THEN '301+'
              END AS bucket,
              COUNT(*) AS cnt
            FROM with_pct
            WHERE pnl_pct IS NOT NULL
            GROUP BY 1
            ),
            tot AS (
            SELECT COALESCE(SUM(cnt),0) AS total FROM counts
            )
            SELECT
            b.bucket,
            COALESCE(c.cnt, 0) AS positions,
            CASE WHEN t.total > 0
                  THEN ROUND(100.0 * COALESCE(c.cnt,0) / t.total, 2)
                  ELSE 0
            END AS share_percent
            FROM buckets b
            LEFT JOIN counts c USING (bucket)
            CROSS JOIN tot t
            ORDER BY b.ord;
            "#,
        )
        .bind(&address)
        .persistent(false)
        .fetch_all(&self.pool)
        .await?;
        Ok(data)
    }
}
