use std::str::FromStr as _;

use chrono::{DateTime, Utc};
use sqlx::{types::BigDecimal, Error, QueryBuilder};

use crate::model::{
    LP_Pool_State, Supplied_Borrowed_Series, Table, Utilization_Level,
};

use super::{DataBase, QueryResult};

impl Table<LP_Pool_State> {
    pub async fn insert(
        &self,
        data: LP_Pool_State,
    ) -> Result<QueryResult, Error> {
        sqlx::query(
            r#"
            INSERT INTO "LP_Pool_State" (
                "LP_Pool_id",
                "LP_Pool_timestamp",
                "LP_Pool_total_value_locked_stable",
                "LP_Pool_total_value_locked_asset",
                "LP_Pool_total_issued_receipts",
                "LP_Pool_total_borrowed_stable",
                "LP_Pool_total_borrowed_asset",
                "LP_Pool_total_yield_stable",
                "LP_Pool_total_yield_asset",
                "LP_Pool_min_utilization_threshold"
            )
            VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        "#,
        )
        .bind(&data.LP_Pool_id)
        .bind(data.LP_Pool_timestamp)
        .bind(&data.LP_Pool_total_value_locked_stable)
        .bind(&data.LP_Pool_total_value_locked_asset)
        .bind(&data.LP_Pool_total_issued_receipts)
        .bind(&data.LP_Pool_total_borrowed_stable)
        .bind(&data.LP_Pool_total_borrowed_asset)
        .bind(&data.LP_Pool_total_yield_stable)
        .bind(&data.LP_Pool_total_yield_asset)
        .persistent(true)
        .execute(&self.pool)
        .await
    }

    pub async fn insert_many(
        &self,
        data: &Vec<LP_Pool_State>,
    ) -> Result<(), Error> {
        if data.is_empty() {
            return Ok(());
        }

        let mut query_builder: QueryBuilder<DataBase> = QueryBuilder::new(
            r#"
            INSERT INTO "LP_Pool_State" (
                "LP_Pool_id",
                "LP_Pool_timestamp",
                "LP_Pool_total_value_locked_stable",
                "LP_Pool_total_value_locked_asset",
                "LP_Pool_total_issued_receipts",
                "LP_Pool_total_borrowed_stable",
                "LP_Pool_total_borrowed_asset",
                "LP_Pool_total_yield_stable",
                "LP_Pool_total_yield_asset",
                "LP_Pool_min_utilization_threshold"
            )"#,
        );

        query_builder.push_values(data, |mut b, data| {
            b.push_bind(&data.LP_Pool_id)
                .push_bind(data.LP_Pool_timestamp)
                .push_bind(&data.LP_Pool_total_value_locked_stable)
                .push_bind(&data.LP_Pool_total_value_locked_asset)
                .push_bind(&data.LP_Pool_total_issued_receipts)
                .push_bind(&data.LP_Pool_total_borrowed_stable)
                .push_bind(&data.LP_Pool_total_borrowed_asset)
                .push_bind(&data.LP_Pool_total_yield_stable)
                .push_bind(&data.LP_Pool_total_yield_asset)
                .push_bind(&data.LP_Pool_min_utilization_threshold);
        });

        let query = query_builder.build().persistent(true);
        query.execute(&self.pool).await?;
        Ok(())
    }

    pub async fn get_total_value_locked_stable(
        &self,
        datetime: DateTime<Utc>,
    ) -> Result<(BigDecimal, BigDecimal, BigDecimal), crate::error::Error> {
        let value: (
            Option<BigDecimal>,
            Option<BigDecimal>,
            Option<BigDecimal>,
        ) = sqlx::query_as(
            r#"
            SELECT
                SUM("LP_Pool_total_value_locked_stable"),
                SUM("LP_Pool_total_borrowed_stable"),
                SUM("LP_Pool_total_yield_stable")
            FROM "LP_Pool_State" WHERE "LP_Pool_timestamp" = $1
            "#,
        )
        .bind(datetime)
        .persistent(true)
        .fetch_one(&self.pool)
        .await?;
        let (locked, borrowed, yield_amount) = value;
        let locked = locked.unwrap_or(BigDecimal::from_str("0")?);
        let borrowed = borrowed.unwrap_or(BigDecimal::from_str("0")?);
        let yield_amount = yield_amount.unwrap_or(BigDecimal::from_str("0")?);

        Ok((locked, borrowed, yield_amount))
    }

    pub async fn get_supplied_borrowed_series(
        &self,
        protocol: String,
    ) -> Result<Vec<Supplied_Borrowed_Series>, Error> {
        let data = sqlx::query_as(
            r#"
            SELECT
                lps."LP_Pool_timestamp",
                SUM(lps."LP_Pool_total_value_locked_stable" / COALESCE(pc.lpn_decimals, 1000000)::numeric) AS "Supplied",
                SUM(lps."LP_Pool_total_borrowed_stable" / COALESCE(pc.lpn_decimals, 1000000)::numeric) AS "Borrowed"
            FROM
                "LP_Pool_State" lps
            LEFT JOIN pool_config pc ON lps."LP_Pool_id" = pc.pool_id
            WHERE lps."LP_Pool_id" = $1
            GROUP BY
                lps."LP_Pool_timestamp"
            ORDER BY
                lps."LP_Pool_timestamp" DESC
            "#,
        )
        .bind(protocol)
        .persistent(true)
        .fetch_all(&self.pool)
        .await?;
        Ok(data)
    }

    pub async fn get_supplied_borrowed_series_total(
        &self,
        protocols: Vec<String>,
    ) -> Result<Vec<Supplied_Borrowed_Series>, Error> {
        let mut params = String::from("$1");

        for i in 1..protocols.len() {
            params += &format!(", ${}", i + 1);
        }

        let query_str = format!(
            r#"
            SELECT
                lps."LP_Pool_timestamp",
                SUM(lps."LP_Pool_total_value_locked_stable" / COALESCE(pc.lpn_decimals, 1000000)::numeric) AS "Supplied",
                SUM(lps."LP_Pool_total_borrowed_stable" / COALESCE(pc.lpn_decimals, 1000000)::numeric) AS "Borrowed"
            FROM
                "LP_Pool_State" lps
            LEFT JOIN pool_config pc ON lps."LP_Pool_id" = pc.pool_id
            WHERE lps."LP_Pool_id" IN ({})
            GROUP BY
                lps."LP_Pool_timestamp"
            ORDER BY
                lps."LP_Pool_timestamp" DESC
            "#,
            params
        );

        let mut query: sqlx::query::QueryAs<'_, _, _, _> =
            sqlx::query_as(&query_str).persistent(true);

        for i in protocols {
            query = query.bind(i);
        }

        let data = query.persistent(true).fetch_all(&self.pool).await?;
        Ok(data)
    }

    pub async fn get_supplied_borrowed_series_with_window(
        &self,
        protocol: String,
        months: Option<i32>,
        from: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<Vec<Supplied_Borrowed_Series>, Error> {
        let time_filter = match (months, from) {
            (_, Some(from_ts)) => format!(
                r#"AND lps."LP_Pool_timestamp" > '{}'"#,
                from_ts.format("%Y-%m-%d %H:%M:%S")
            ),
            (Some(m), None) => format!(
                r#"AND lps."LP_Pool_timestamp" > NOW() - INTERVAL '{} months'"#,
                m
            ),
            (None, None) => String::new(),
        };

        let query_str = format!(
            r#"
            SELECT
                lps."LP_Pool_timestamp",
                SUM(lps."LP_Pool_total_value_locked_stable" / COALESCE(pc.lpn_decimals, 1000000)::numeric) AS "Supplied",
                SUM(lps."LP_Pool_total_borrowed_stable" / COALESCE(pc.lpn_decimals, 1000000)::numeric) AS "Borrowed"
            FROM
                "LP_Pool_State" lps
            LEFT JOIN pool_config pc ON lps."LP_Pool_id" = pc.pool_id
            WHERE lps."LP_Pool_id" = $1
            {}
            GROUP BY
                lps."LP_Pool_timestamp"
            ORDER BY
                lps."LP_Pool_timestamp" DESC
            "#,
            time_filter
        );

        let data = sqlx::query_as(&query_str)
            .bind(protocol)
            .persistent(true)
            .fetch_all(&self.pool)
            .await?;
        Ok(data)
    }

    pub async fn get_supplied_borrowed_series_total_with_window(
        &self,
        protocols: Vec<String>,
        months: Option<i32>,
        from: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<Vec<Supplied_Borrowed_Series>, Error> {
        let mut params = String::from("$1");

        for i in 1..protocols.len() {
            params += &format!(", ${}", i + 1);
        }

        let time_filter = match (months, from) {
            (_, Some(from_ts)) => format!(
                r#"AND lps."LP_Pool_timestamp" > '{}'"#,
                from_ts.format("%Y-%m-%d %H:%M:%S")
            ),
            (Some(m), None) => format!(
                r#"AND lps."LP_Pool_timestamp" > NOW() - INTERVAL '{} months'"#,
                m
            ),
            (None, None) => String::new(),
        };

        let query_str = format!(
            r#"
            SELECT
                lps."LP_Pool_timestamp",
                SUM(lps."LP_Pool_total_value_locked_stable" / COALESCE(pc.lpn_decimals, 1000000)::numeric) AS "Supplied",
                SUM(lps."LP_Pool_total_borrowed_stable" / COALESCE(pc.lpn_decimals, 1000000)::numeric) AS "Borrowed"
            FROM
                "LP_Pool_State" lps
            LEFT JOIN pool_config pc ON lps."LP_Pool_id" = pc.pool_id
            WHERE lps."LP_Pool_id" IN ({})
            {}
            GROUP BY
                lps."LP_Pool_timestamp"
            ORDER BY
                lps."LP_Pool_timestamp" DESC
            "#,
            params, time_filter
        );

        let mut query: sqlx::query::QueryAs<'_, _, _, _> =
            sqlx::query_as(&query_str).persistent(true);

        for i in protocols {
            query = query.bind(i);
        }

        let data = query.persistent(true).fetch_all(&self.pool).await?;
        Ok(data)
    }

    pub async fn get_utilization_level(
        &self,
        protocol: String,
        skip: i64,
        limit: i64,
    ) -> Result<Vec<Utilization_Level>, Error> {
        let data = sqlx::query_as(
            r#"
                SELECT ("LP_Pool_total_borrowed_stable" / "LP_Pool_total_value_locked_stable")*100 as "Utilization_Level" FROM "LP_Pool_State" WHERE "LP_Pool_id" = $1 ORDER BY "LP_Pool_timestamp" DESC OFFSET $2 LIMIT $3
            "#,
        )
        .bind(protocol)
        .bind(skip)
        .bind(limit)
        .persistent(true)
        .fetch_all(&self.pool)
        .await?;
        Ok(data)
    }

    pub async fn get_utilization_level_old(
        &self,
        skip: i64,
        limit: i64,
    ) -> Result<Vec<Utilization_Level>, Error> {
        let data = sqlx::query_as(
            r#"
                SELECT ("LP_Pool_total_borrowed_stable" / "LP_Pool_total_value_locked_stable")*100 as "Utilization_Level" FROM "LP_Pool_State" ORDER BY "LP_Pool_timestamp" DESC OFFSET $1 LIMIT $2
            "#,
        )
        .bind(skip)
        .bind(limit)
        .persistent(true)
        .fetch_all(&self.pool)
        .await?;
        Ok(data)
    }

    /// Get utilization level with time window filtering
    pub async fn get_utilization_level_with_window(
        &self,
        protocol: String,
        months: Option<i32>,
        from: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<Vec<Utilization_Level>, Error> {
        // Build time conditions dynamically
        let mut conditions = vec![r#""LP_Pool_id" = $1"#.to_string()];

        if let Some(m) = months {
            conditions.push(format!(
                r#""LP_Pool_timestamp" >= NOW() - INTERVAL '{} months'"#,
                m
            ));
        }

        if from.is_some() {
            conditions.push(r#""LP_Pool_timestamp" > $2"#.to_string());
        }

        let where_clause = conditions.join(" AND ");
        let query = format!(
            r#"
            SELECT ("LP_Pool_total_borrowed_stable" / "LP_Pool_total_value_locked_stable")*100 as "Utilization_Level"
            FROM "LP_Pool_State"
            WHERE {}
            ORDER BY "LP_Pool_timestamp" DESC
            "#,
            where_clause
        );

        let mut query_builder =
            sqlx::query_as::<_, Utilization_Level>(&query).bind(&protocol);

        if let Some(from_ts) = from {
            query_builder = query_builder.bind(from_ts);
        }

        let data = query_builder.persistent(true).fetch_all(&self.pool).await?;
        Ok(data)
    }

    pub async fn get_supplied_funds(
        &self,
    ) -> Result<BigDecimal, crate::error::Error> {
        let value: (Option<BigDecimal>,) = sqlx::query_as(
            r#"
              WITH Latest_Pool_Data AS (
                SELECT
                    lps."LP_Pool_id",
                    lps."LP_Pool_total_value_locked_stable",
                    pc.lpn_decimals,
                    RANK() OVER (PARTITION BY lps."LP_Pool_id" ORDER BY lps."LP_Pool_timestamp" DESC) AS rank
                FROM "LP_Pool_State" lps
                INNER JOIN pool_config pc ON lps."LP_Pool_id" = pc.pool_id
            )
            SELECT
                SUM("LP_Pool_total_value_locked_stable" / COALESCE(lpn_decimals, 1000000)::numeric) AS "Total Supplied"
            FROM Latest_Pool_Data
            WHERE rank = 1
            "#,
        )
        .persistent(true)
        .fetch_one(&self.pool)
        .await?;
        let (amnt,) = value;
        let amnt = amnt.unwrap_or(BigDecimal::from_str("0")?);

        Ok(amnt)
    }

    pub async fn get_earnings(
        &self,
        address: String,
    ) -> Result<BigDecimal, crate::error::Error> {
        let value: (Option<BigDecimal>,) = sqlx::query_as(
            r#"
            WITH per_pool AS (
            /* ================= USDC POOLS (earnings already in stable, 6 decimals) ================= */
            -- Pool: nolus1qg5... (USDC_NOBLE)
            SELECT
                'nolus1qg5ega6dykkxc307y25pecuufrjkxkaggkkxh7nad0vhyhtuhw3sqaa3c5'::text AS "LP_Pool_id",
                ls."LP_Lender_id",
                -- current balance in stable (scaled by 1e6)
                (ls."LP_Lender_stable"::numeric / 1000000::numeric)                         AS "current_balance_in_stable",
                -- deposits / withdrawals in stable (scaled by 1e6)
                (dep."deposited_raw"   / 1000000::numeric)                                  AS "deposited_in_stable",
                (wdr."withdrawn_raw"   / 1000000::numeric)                                  AS "withdrawn_in_stable",
                -- earnings in stable: (raw PnL / 1e6)
                (ls."LP_Lender_stable"::numeric
                - (dep."deposited_raw" - wdr."withdrawn_raw")) / 1000000::numeric        AS "earnings_in_stable"
            FROM (
                SELECT
                    "LP_Lender_id",
                    "LP_Lender_stable",
                    "LP_timestamp"
                FROM "LP_Lender_State"
                WHERE "LP_Lender_id" = $1
                AND "LP_Pool_id"   = 'nolus1qg5ega6dykkxc307y25pecuufrjkxkaggkkxh7nad0vhyhtuhw3sqaa3c5'
                ORDER BY "LP_timestamp" DESC
                LIMIT 1
            ) ls
            CROSS JOIN LATERAL (
                SELECT COALESCE(SUM(d."LP_amnt_stable"), 0)::numeric AS "deposited_raw"
                FROM "LP_Deposit" d
                WHERE d."LP_address_id" = $1
                AND d."LP_Pool_id"    = 'nolus1qg5ega6dykkxc307y25pecuufrjkxkaggkkxh7nad0vhyhtuhw3sqaa3c5'
                AND d."LP_timestamp" <= ls."LP_timestamp"
            ) dep
            CROSS JOIN LATERAL (
                SELECT COALESCE(SUM(w."LP_amnt_stable"), 0)::numeric AS "withdrawn_raw"
                FROM "LP_Withdraw" w
                WHERE w."LP_address_id" = $1
                AND w."LP_Pool_id"    = 'nolus1qg5ega6dykkxc307y25pecuufrjkxkaggkkxh7nad0vhyhtuhw3sqaa3c5'
                AND w."LP_timestamp" <= ls."LP_timestamp"
            ) wdr

            UNION ALL

            -- Pool: nolus1ueyt... (USDC_NOBLE)
            SELECT
                'nolus1ueytzwqyadm6r0z8ajse7g6gzum4w3vv04qazctf8ugqrrej6n4sq027cf'::text AS "LP_Pool_id",
                ls."LP_Lender_id",
                (ls."LP_Lender_stable"::numeric / 1000000::numeric)                         AS "current_balance_in_stable",
                (dep."deposited_raw"   / 1000000::numeric)                                  AS "deposited_in_stable",
                (wdr."withdrawn_raw"   / 1000000::numeric)                                  AS "withdrawn_in_stable",
                (ls."LP_Lender_stable"::numeric
                - (dep."deposited_raw" - wdr."withdrawn_raw")) / 1000000::numeric        AS "earnings_in_stable"
            FROM (
                SELECT
                    "LP_Lender_id",
                    "LP_Lender_stable",
                    "LP_timestamp"
                FROM "LP_Lender_State"
                WHERE "LP_Lender_id" = $1
                AND "LP_Pool_id"   = 'nolus1ueytzwqyadm6r0z8ajse7g6gzum4w3vv04qazctf8ugqrrej6n4sq027cf'
                ORDER BY "LP_timestamp" DESC
                LIMIT 1
            ) ls
            CROSS JOIN LATERAL (
                SELECT COALESCE(SUM(d."LP_amnt_stable"), 0)::numeric AS "deposited_raw"
                FROM "LP_Deposit" d
                WHERE d."LP_address_id" = $1
                AND d."LP_Pool_id"    = 'nolus1ueytzwqyadm6r0z8ajse7g6gzum4w3vv04qazctf8ugqrrej6n4sq027cf'
                AND d."LP_timestamp" <= ls."LP_timestamp"
            ) dep
            CROSS JOIN LATERAL (
                SELECT COALESCE(SUM(w."LP_amnt_stable"), 0)::numeric AS "withdrawn_raw"
                FROM "LP_Withdraw" w
                WHERE w."LP_address_id" = $1
                AND w."LP_Pool_id"    = 'nolus1ueytzwqyadm6r0z8ajse7g6gzum4w3vv04qazctf8ugqrrej6n4sq027cf'
                AND w."LP_timestamp" <= ls."LP_timestamp"
            ) wdr

            UNION ALL

            -- Pool: nolus17vse... (USDC_NOBLE)
            SELECT
                'nolus17vsedux675vc44yu7et9m64ndxsy907v7sfgrk7tw3xnjtqemx3q6t3xw6'::text AS "LP_Pool_id",
                ls."LP_Lender_id",
                (ls."LP_Lender_stable"::numeric / 1000000::numeric)                         AS "current_balance_in_stable",
                (dep."deposited_raw"   / 1000000::numeric)                                  AS "deposited_in_stable",
                (wdr."withdrawn_raw"   / 1000000::numeric)                                  AS "withdrawn_in_stable",
                (ls."LP_Lender_stable"::numeric
                - (dep."deposited_raw" - wdr."withdrawn_raw")) / 1000000::numeric        AS "earnings_in_stable"
            FROM (
                SELECT
                    "LP_Lender_id",
                    "LP_Lender_stable",
                    "LP_timestamp"
                FROM "LP_Lender_State"
                WHERE "LP_Lender_id" = $1
                AND "LP_Pool_id"   = 'nolus17vsedux675vc44yu7et9m64ndxsy907v7sfgrk7tw3xnjtqemx3q6t3xw6'
                ORDER BY "LP_timestamp" DESC
                LIMIT 1
            ) ls
            CROSS JOIN LATERAL (
                SELECT COALESCE(SUM(d."LP_amnt_stable"), 0)::numeric AS "deposited_raw"
                FROM "LP_Deposit" d
                WHERE d."LP_address_id" = $1
                AND d."LP_Pool_id"    = 'nolus17vsedux675vc44yu7et9m64ndxsy907v7sfgrk7tw3xnjtqemx3q6t3xw6'
                AND d."LP_timestamp" <= ls."LP_timestamp"
            ) dep
            CROSS JOIN LATERAL (
                SELECT COALESCE(SUM(w."LP_amnt_stable"), 0)::numeric AS "withdrawn_raw"
                FROM "LP_Withdraw" w
                WHERE w."LP_address_id" = $1
                AND w."LP_Pool_id"    = 'nolus17vsedux675vc44yu7et9m64ndxsy907v7sfgrk7tw3xnjtqemx3q6t3xw6'
                AND w."LP_timestamp" <= ls."LP_timestamp"
            ) wdr

            /* ================= VOLATILE POOLS (earnings in asset, then * price) ================= */

            UNION ALL
            -- Pool: nolus1w2yz... (ALL_BTC, 8 decimals)
            SELECT
                'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3'::text AS "LP_Pool_id",
                ls."LP_Lender_id",
                -- current position value in stable
                (ls."LP_Lender_asset"::numeric / 100000000::numeric) * p."MP_price_in_stable"   AS "current_balance_in_stable",
                -- deposits / withdrawals marked-to-market at current price
                (dep."deposited_asset" / 100000000::numeric) * p."MP_price_in_stable"          AS "deposited_in_stable",
                (wdr."withdrawn_asset" / 100000000::numeric) * p."MP_price_in_stable"          AS "withdrawn_in_stable",
                -- earnings in asset → convert to stable
                (
                ls."LP_Lender_asset"::numeric
                    - (dep."deposited_asset" - wdr."withdrawn_asset")
                ) / 100000000::numeric * p."MP_price_in_stable"                                AS "earnings_in_stable"
            FROM (
                SELECT
                    "LP_Lender_id",
                    "LP_Lender_asset",
                    "LP_timestamp"
                FROM "LP_Lender_State"
                WHERE "LP_Lender_id" = $1
                AND "LP_Pool_id"   = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3'
                ORDER BY "LP_timestamp" DESC
                LIMIT 1
            ) ls
            CROSS JOIN LATERAL (
                SELECT COALESCE(SUM(d."LP_amnt_asset"), 0)::numeric AS "deposited_asset"
                FROM "LP_Deposit" d
                WHERE d."LP_address_id" = $1
                AND d."LP_Pool_id"    = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3'
                AND d."LP_timestamp" <= ls."LP_timestamp"
            ) dep
            CROSS JOIN LATERAL (
                SELECT COALESCE(SUM(w."LP_amnt_asset"), 0)::numeric AS "withdrawn_asset"
                FROM "LP_Withdraw" w
                WHERE w."LP_address_id" = $1
                AND w."LP_Pool_id"    = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3'
                AND w."LP_timestamp" <= ls."LP_timestamp"
            ) wdr
            CROSS JOIN (
                SELECT "MP_price_in_stable"
                FROM "MP_Asset"
                WHERE "Protocol"        = 'OSMOSIS-OSMOSIS-USDC_NOBLE'
                AND "MP_asset_symbol" = 'ALL_BTC'
                ORDER BY "MP_asset_timestamp" DESC
                LIMIT 1
            ) p

            UNION ALL
            -- Pool: nolus1qufn... (ALL_SOL, 9 decimals)
            SELECT
                'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm'::text AS "LP_Pool_id",
                ls."LP_Lender_id",
                (ls."LP_Lender_asset"::numeric / 1000000000::numeric) * p."MP_price_in_stable"  AS "current_balance_in_stable",
                (dep."deposited_asset" / 1000000000::numeric) * p."MP_price_in_stable"          AS "deposited_in_stable",
                (wdr."withdrawn_asset" / 1000000000::numeric) * p."MP_price_in_stable"          AS "withdrawn_in_stable",
                (
                ls."LP_Lender_asset"::numeric
                    - (dep."deposited_asset" - wdr."withdrawn_asset")
                ) / 1000000000::numeric * p."MP_price_in_stable"                                AS "earnings_in_stable"
            FROM (
                SELECT
                    "LP_Lender_id",
                    "LP_Lender_asset",
                    "LP_timestamp"
                FROM "LP_Lender_State"
                WHERE "LP_Lender_id" = $1
                AND "LP_Pool_id"   = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm'
                ORDER BY "LP_timestamp" DESC
                LIMIT 1
            ) ls
            CROSS JOIN LATERAL (
                SELECT COALESCE(SUM(d."LP_amnt_asset"), 0)::numeric AS "deposited_asset"
                FROM "LP_Deposit" d
                WHERE d."LP_address_id" = $1
                AND d."LP_Pool_id"    = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm'
                AND d."LP_timestamp" <= ls."LP_timestamp"
            ) dep
            CROSS JOIN LATERAL (
                SELECT COALESCE(SUM(w."LP_amnt_asset"), 0)::numeric AS "withdrawn_asset"
                FROM "LP_Withdraw" w
                WHERE w."LP_address_id" = $1
                AND w."LP_Pool_id"    = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm'
                AND w."LP_timestamp" <= ls."LP_timestamp"
            ) wdr
            CROSS JOIN (
                SELECT "MP_price_in_stable"
                FROM "MP_Asset"
                WHERE "Protocol"        = 'OSMOSIS-OSMOSIS-USDC_NOBLE'
                AND "MP_asset_symbol" = 'ALL_SOL'
                ORDER BY "MP_asset_timestamp" DESC
                LIMIT 1
            ) p

            UNION ALL
            -- Pool: nolus1lxr7... (AKT, 6 decimals)
            SELECT
                'nolus1lxr7f5xe02jq6cce4puk6540mtu9sg36at2dms5sk69wdtzdrg9qq0t67z'::text AS "LP_Pool_id",
                ls."LP_Lender_id",
                (ls."LP_Lender_asset"::numeric / 1000000::numeric) * p."MP_price_in_stable"    AS "current_balance_in_stable",
                (dep."deposited_asset" / 1000000::numeric) * p."MP_price_in_stable"            AS "deposited_in_stable",
                (wdr."withdrawn_asset" / 1000000::numeric) * p."MP_price_in_stable"            AS "withdrawn_in_stable",
                (
                ls."LP_Lender_asset"::numeric
                    - (dep."deposited_asset" - wdr."withdrawn_asset")
                ) / 1000000::numeric * p."MP_price_in_stable"                                  AS "earnings_in_stable"
            FROM (
                SELECT
                    "LP_Lender_id",
                    "LP_Lender_asset",
                    "LP_timestamp"
                FROM "LP_Lender_State"
                WHERE "LP_Lender_id" = $1
                AND "LP_Pool_id"   = 'nolus1lxr7f5xe02jq6cce4puk6540mtu9sg36at2dms5sk69wdtzdrg9qq0t67z'
                ORDER BY "LP_timestamp" DESC
                LIMIT 1
            ) ls
            CROSS JOIN LATERAL (
                SELECT COALESCE(SUM(d."LP_amnt_asset"), 0)::numeric AS "deposited_asset"
                FROM "LP_Deposit" d
                WHERE d."LP_address_id" = $1
                AND d."LP_Pool_id"    = 'nolus1lxr7f5xe02jq6cce4puk6540mtu9sg36at2dms5sk69wdtzdrg9qq0t67z'
                AND d."LP_timestamp" <= ls."LP_timestamp"
            ) dep
            CROSS JOIN LATERAL (
                SELECT COALESCE(SUM(w."LP_amnt_asset"), 0)::numeric AS "withdrawn_asset"
                FROM "LP_Withdraw" w
                WHERE w."LP_address_id" = $1
                AND w."LP_Pool_id"    = 'nolus1lxr7f5xe02jq6cce4puk6540mtu9sg36at2dms5sk69wdtzdrg9qq0t67z'
                AND w."LP_timestamp" <= ls."LP_timestamp"
            ) wdr
            CROSS JOIN (
                SELECT "MP_price_in_stable"
                FROM "MP_Asset"
                WHERE "Protocol"        = 'OSMOSIS-OSMOSIS-USDC_NOBLE'
                AND "MP_asset_symbol" = 'AKT'
                ORDER BY "MP_asset_timestamp" DESC
                LIMIT 1
            ) p

            UNION ALL
            -- Pool: nolus1u0zt... (ATOM, 6 decimals)
            SELECT
                'nolus1u0zt8x3mkver0447glfupz9lz6wnt62j70p5fhhtu3fr46gcdd9s5dz9l6'::text AS "LP_Pool_id",
                ls."LP_Lender_id",
                (ls."LP_Lender_asset"::numeric / 1000000::numeric) * p."MP_price_in_stable"    AS "current_balance_in_stable",
                (dep."deposited_asset" / 1000000::numeric) * p."MP_price_in_stable"            AS "deposited_in_stable",
                (wdr."withdrawn_asset" / 1000000::numeric) * p."MP_price_in_stable"            AS "withdrawn_in_stable",
                (
                ls."LP_Lender_asset"::numeric
                    - (dep."deposited_asset" - wdr."withdrawn_asset")
                ) / 1000000::numeric * p."MP_price_in_stable"                                  AS "earnings_in_stable"
            FROM (
                SELECT
                    "LP_Lender_id",
                    "LP_Lender_asset",
                    "LP_timestamp"
                FROM "LP_Lender_State"
                WHERE "LP_Lender_id" = $1
                AND "LP_Pool_id"   = 'nolus1u0zt8x3mkver0447glfupz9lz6wnt62j70p5fhhtu3fr46gcdd9s5dz9l6'
                ORDER BY "LP_timestamp" DESC
                LIMIT 1
            ) ls
            CROSS JOIN LATERAL (
                SELECT COALESCE(SUM(d."LP_amnt_asset"), 0)::numeric AS "deposited_asset"
                FROM "LP_Deposit" d
                WHERE d."LP_address_id" = $1
                AND d."LP_Pool_id"    = 'nolus1u0zt8x3mkver0447glfupz9lz6wnt62j70p5fhhtu3fr46gcdd9s5dz9l6'
                AND d."LP_timestamp" <= ls."LP_timestamp"
            ) dep
            CROSS JOIN LATERAL (
                SELECT COALESCE(SUM(w."LP_amnt_asset"), 0)::numeric AS "withdrawn_asset"
                FROM "LP_Withdraw" w
                WHERE w."LP_address_id" = $1
                AND w."LP_Pool_id"    = 'nolus1u0zt8x3mkver0447glfupz9lz6wnt62j70p5fhhtu3fr46gcdd9s5dz9l6'
                AND w."LP_timestamp" <= ls."LP_timestamp"
            ) wdr
            CROSS JOIN (
                SELECT "MP_price_in_stable"
                FROM "MP_Asset"
                WHERE "Protocol"        = 'OSMOSIS-OSMOSIS-USDC_NOBLE'
                AND "MP_asset_symbol" = 'ATOM'
                ORDER BY "MP_asset_timestamp" DESC
                LIMIT 1
            ) p

            UNION ALL
            -- Pool: nolus1py7p... (OSMO, 6 decimals)
            SELECT
                'nolus1py7pxw74qvlgq0n6rfz7mjrhgnls37mh87wasg89n75qt725rams8yr46t'::text AS "LP_Pool_id",
                ls."LP_Lender_id",
                (ls."LP_Lender_asset"::numeric / 1000000::numeric) * p."MP_price_in_stable"    AS "current_balance_in_stable",
                (dep."deposited_asset" / 1000000::numeric) * p."MP_price_in_stable"            AS "deposited_in_stable",
                (wdr."withdrawn_asset" / 1000000::numeric) * p."MP_price_in_stable"            AS "withdrawn_in_stable",
                (
                ls."LP_Lender_asset"::numeric
                    - (dep."deposited_asset" - wdr."withdrawn_asset")
                ) / 1000000::numeric * p."MP_price_in_stable"                                  AS "earnings_in_stable"
            FROM (
                SELECT
                    "LP_Lender_id",
                    "LP_Lender_asset",
                    "LP_timestamp"
                FROM "LP_Lender_State"
                WHERE "LP_Lender_id" = $1
                AND "LP_Pool_id"   = 'nolus1py7pxw74qvlgq0n6rfz7mjrhgnls37mh87wasg89n75qt725rams8yr46t'
                ORDER BY "LP_timestamp" DESC
                LIMIT 1
            ) ls
            CROSS JOIN LATERAL (
                SELECT COALESCE(SUM(d."LP_amnt_asset"), 0)::numeric AS "deposited_asset"
                FROM "LP_Deposit" d
                WHERE d."LP_address_id" = $1
                AND d."LP_Pool_id"    = 'nolus1py7pxw74qvlgq0n6rfz7mjrhgnls37mh87wasg89n75qt725rams8yr46t'
                AND d."LP_timestamp" <= ls."LP_timestamp"
            ) dep
            CROSS JOIN LATERAL (
                SELECT COALESCE(SUM(w."LP_amnt_asset"), 0)::numeric AS "withdrawn_asset"
                FROM "LP_Withdraw" w
                WHERE w."LP_address_id" = $1
                AND w."LP_Pool_id"    = 'nolus1py7pxw74qvlgq0n6rfz7mjrhgnls37mh87wasg89n75qt725rams8yr46t'
                AND w."LP_timestamp" <= ls."LP_timestamp"
            ) wdr
            CROSS JOIN (
                SELECT "MP_price_in_stable"
                FROM "MP_Asset"
                WHERE "Protocol"        = 'OSMOSIS-OSMOSIS-USDC_NOBLE'
                AND "MP_asset_symbol" = 'OSMO'
                ORDER BY "MP_asset_timestamp" DESC
                LIMIT 1
            ) p
        )
        SELECT
            COALESCE(SUM(GREATEST("earnings_in_stable", 0)), 0)::numeric AS total_earnings_in_stable
        FROM per_pool
            "#,
        )
        .persistent(true)
        .bind(address)
        .fetch_one(&self.pool)
        .await?;
        let (amnt,) = value;
        let amnt = amnt.unwrap_or(BigDecimal::from_str("0")?);

        Ok(amnt)
    }

    pub async fn get_by_date(
        &self,
        protocol: String,
        date_time: &DateTime<Utc>,
    ) -> Result<LP_Pool_State, Error> {
        sqlx::query_as(
            r#"
                    SELECT *
                    FROM "LP_Pool_State"
                    WHERE
                        "LP_Pool_id" = $1
                        AND
                        "LP_Pool_timestamp" >= $2

                    ORDER BY "LP_Pool_timestamp" ASC LIMIT 1
                    "#,
        )
        .bind(protocol)
        .bind(date_time)
        .persistent(true)
        .fetch_one(&self.pool)
        .await
    }

    /// Get utilization levels for all pools in a single query
    /// Returns the latest utilization level for each pool including borrow APR and earn APR
    pub async fn get_all_utilization_levels(
        &self,
    ) -> Result<Vec<PoolUtilizationLevel>, Error> {
        let data = sqlx::query_as(
            r#"
            WITH Latest_Pool_Aggregation AS (
                SELECT MAX("LP_Pool_timestamp") AS max_ts FROM "LP_Pool_State"
            ),
            Latest_LS_Aggregation AS (
                SELECT MAX("LS_timestamp") AS max_ts FROM "LS_State"
            ),
            LatestStates AS (
                SELECT DISTINCT ON (lps."LP_Pool_id")
                    lps."LP_Pool_id",
                    lps."LP_Pool_total_value_locked_stable",
                    lps."LP_Pool_total_borrowed_stable",
                    lps."LP_Pool_timestamp",
                    pc.lpn_decimals,
                    pc.protocol
                FROM "LP_Pool_State" lps
                LEFT JOIN pool_config pc ON lps."LP_Pool_id" = pc.pool_id
                WHERE lps."LP_Pool_timestamp" = (SELECT max_ts FROM Latest_Pool_Aggregation)
                ORDER BY lps."LP_Pool_id", lps."LP_Pool_timestamp" DESC
            ),
            LatestBorrowAPR AS (
                SELECT DISTINCT ON ("LS_loan_pool_id")
                    "LS_loan_pool_id",
                    "LS_interest" / 10.0 AS borrow_apr
                FROM "LS_Opening"
                ORDER BY "LS_loan_pool_id", "LS_timestamp" DESC
            ),
            PoolUtilization AS (
                SELECT
                    lps."LP_Pool_id",
                    CASE
                        WHEN lps."LP_Pool_total_value_locked_stable" > 0
                        THEN lps."LP_Pool_total_borrowed_stable"::numeric / lps."LP_Pool_total_value_locked_stable"::numeric
                        ELSE 0
                    END AS utilization_rate
                FROM "LP_Pool_State" lps
                WHERE lps."LP_Pool_timestamp" = (SELECT max_ts FROM Latest_Pool_Aggregation)
            ),
            AvgInterestPerPool AS (
                SELECT
                    o."LS_loan_pool_id",
                    AVG(o."LS_interest") / 10.0 AS avg_interest
                FROM "LS_State" s
                CROSS JOIN Latest_LS_Aggregation la
                INNER JOIN "LS_Opening" o ON s."LS_contract_id" = o."LS_contract_id"
                WHERE s."LS_timestamp" = la.max_ts
                GROUP BY o."LS_loan_pool_id"
            ),
            EarnAPRCalc AS (
                SELECT
                    pc.pool_id,
                    CASE
                        WHEN pc.protocol IN ('OSMOSIS-OSMOSIS-ALL_BTC', 'OSMOSIS-OSMOSIS-ATOM') THEN
                            (COALESCE(ai.avg_interest, 0) - 2.5) * COALESCE(pu.utilization_rate, 0)
                        WHEN pc.protocol = 'OSMOSIS-OSMOSIS-ALL_SOL' THEN
                            (COALESCE(ai.avg_interest, 0) - 4.0) * COALESCE(pu.utilization_rate, 0)
                        WHEN pc.protocol IN ('OSMOSIS-OSMOSIS-ST_ATOM', 'OSMOSIS-OSMOSIS-AKT') THEN
                            (COALESCE(ai.avg_interest, 0) - 2.0) * COALESCE(pu.utilization_rate, 0)
                        ELSE
                            (COALESCE(ai.avg_interest, 0) - 4.0) * COALESCE(pu.utilization_rate, 0)
                    END AS apr_simple
                FROM pool_config pc
                LEFT JOIN AvgInterestPerPool ai ON pc.pool_id = ai."LS_loan_pool_id"
                LEFT JOIN PoolUtilization pu ON pc.pool_id = pu."LP_Pool_id"
            )
            SELECT
                COALESCE(ls.protocol, ls."LP_Pool_id") AS protocol,
                CASE
                    WHEN ls."LP_Pool_total_value_locked_stable" > 0
                    THEN (ls."LP_Pool_total_borrowed_stable"::numeric / ls."LP_Pool_total_value_locked_stable"::numeric) * 100
                    ELSE 0
                END AS utilization,
                ls."LP_Pool_total_value_locked_stable" / COALESCE(ls.lpn_decimals, 1000000)::numeric AS supplied,
                ls."LP_Pool_total_borrowed_stable" / COALESCE(ls.lpn_decimals, 1000000)::numeric AS borrowed,
                COALESCE(apr.borrow_apr, 0) AS borrow_apr,
                CASE
                    WHEN ea.apr_simple IS NOT NULL AND ea.apr_simple > 0
                    THEN (POWER((1 + (ea.apr_simple / 100 / 365)), 365) - 1) * 100
                    ELSE 0
                END AS earn_apr
            FROM LatestStates ls
            LEFT JOIN LatestBorrowAPR apr ON ls."LP_Pool_id" = apr."LS_loan_pool_id"
            LEFT JOIN EarnAPRCalc ea ON ls."LP_Pool_id" = ea.pool_id
            WHERE ls.protocol IS NOT NULL
            ORDER BY ls.protocol
            "#,
        )
        .persistent(true)
        .fetch_all(&self.pool)
        .await?;

        Ok(data)
    }
}

/// Represents utilization level data for a single pool
#[derive(Debug, Clone, sqlx::FromRow, serde::Serialize)]
pub struct PoolUtilizationLevel {
    pub protocol: String,
    pub utilization: BigDecimal,
    pub supplied: BigDecimal,
    pub borrowed: BigDecimal,
    pub borrow_apr: BigDecimal,
    pub earn_apr: BigDecimal,
}
