use bigdecimal::Zero as _;
use chrono::{DateTime, Utc};
use sqlx::{types::BigDecimal, Error, QueryBuilder};

use crate::{
    model::{
        LP_Pool_State, Supplied_Borrowed_Series, Table, Utilization_Level,
    },
    types::Max_LP_Ratio,
};

impl Table<LP_Pool_State> {
    pub async fn insert(&self, data: LP_Pool_State) -> Result<(), Error> {
        const SQL: &str = r#"
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
        "#;

        sqlx::query(SQL)
            .bind(&data.LP_Pool_id)
            .bind(data.LP_Pool_timestamp)
            .bind(&data.LP_Pool_total_value_locked_stable)
            .bind(&data.LP_Pool_total_value_locked_asset)
            .bind(&data.LP_Pool_total_issued_receipts)
            .bind(&data.LP_Pool_total_borrowed_stable)
            .bind(&data.LP_Pool_total_borrowed_asset)
            .bind(&data.LP_Pool_total_yield_stable)
            .bind(&data.LP_Pool_total_yield_asset)
            .execute(&self.pool)
            .await
            .map(drop)
    }

    pub async fn insert_many(
        &self,
        data: &Vec<LP_Pool_State>,
    ) -> Result<(), Error> {
        const SQL: &str = r#"
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
        "#;

        if data.is_empty() {
            return Ok(());
        }

        QueryBuilder::new(SQL)
            .push_values(data, |mut b, data| {
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
            })
            .build()
            .persistent(false)
            .execute(&self.pool)
            .await
            .map(drop)
    }

    pub async fn get_total_value_locked_stable(
        &self,
        datetime: DateTime<Utc>,
    ) -> Result<(BigDecimal, BigDecimal, BigDecimal), Error> {
        const SQL: &str = r#"
        SELECT
            SUM("LP_Pool_total_value_locked_stable"),
            SUM("LP_Pool_total_borrowed_stable"),
            SUM("LP_Pool_total_yield_stable")
        FROM "LP_Pool_State"
        WHERE "LP_Pool_timestamp" = $1
        "#;

        sqlx::query_as(SQL)
            .bind(datetime)
            .fetch_optional(&self.pool)
            .await
            .map(|result| {
                result.unwrap_or_else(|| {
                    (BigDecimal::zero(), BigDecimal::zero(), BigDecimal::zero())
                })
            })
    }

    pub async fn get_supplied_borrowed_series(
        &self,
        protocol: String,
    ) -> Result<Vec<Supplied_Borrowed_Series>, Error> {
        const SQL: &str = r#"
        SELECT
            "LP_Pool_timestamp",
            SUM(
                "LP_Pool_total_value_locked_stable" / (
                    CASE
                        WHEN "LP_Pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN
                            100000000
                        WHEN "LP_Pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN
                            1000000000
                        ELSE
                            1000000
                    END
                )
            ) AS "Supplied",
            SUM(
                "LP_Pool_total_borrowed_stable" / (
                    CASE
                        WHEN "LP_Pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN
                            100000000
                        WHEN "LP_Pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN
                            1000000000
                        ELSE
                            1000000
                    END
                )
            ) AS "Borrowed"
        FROM "LP_Pool_State"
        WHERE "LP_Pool_id" = $1
        GROUP BY "LP_Pool_timestamp"
        ORDER BY "LP_Pool_timestamp" DESC
        "#;

        sqlx::query_as(SQL)
            .bind(protocol)
            .fetch_all(&self.pool)
            .await
    }

    pub async fn get_supplied_borrowed_series_total(
        &self,
        protocols: Vec<String>,
    ) -> Result<Vec<Supplied_Borrowed_Series>, Error> {
        const PRE_BINDS_SQL: &str = r#"
        SELECT
            "LP_Pool_timestamp",
            SUM(
                "LP_Pool_total_value_locked_stable" / (
                    CASE
                        WHEN "LP_Pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN
                            100000000
                        WHEN "LP_Pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN
                            1000000000
                        ELSE
                            1000000
                    END
                )
            ) AS "Supplied",
            SUM(
                "LP_Pool_total_borrowed_stable" / (
                    CASE
                        WHEN "LP_Pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN
                            100000000
                        WHEN "LP_Pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN
                            1000000000
                        ELSE
                            1000000
                    END
                )
            ) AS "Borrowed"
        FROM "LP_Pool_State"
        WHERE "LP_Pool_id" IN (
        "#;

        const POST_BINDS_SQL: &str = r#"
        )
        GROUP BY "LP_Pool_timestamp"
        ORDER BY "LP_Pool_timestamp" DESC
        "#;

        protocols
            .into_iter()
            .fold(&mut QueryBuilder::new(PRE_BINDS_SQL), QueryBuilder::push)
            .push(POST_BINDS_SQL)
            .build_query_as()
            .fetch_all(&self.pool)
            .await
    }

    pub async fn get_utilization_level(
        &self,
        protocol: String,
        skip: i64,
        limit: i64,
    ) -> Result<Vec<Utilization_Level>, Error> {
        const SQL: &str = r#"
        SELECT
            (
                (
                    "LP_Pool_total_borrowed_stable" / "LP_Pool_total_value_locked_stable"
                ) * 100
            ) AS "Utilization_Level"
        FROM "LP_Pool_State"
        WHERE "LP_Pool_id" = $1
        ORDER BY "LP_Pool_timestamp" DESC
        OFFSET $2
        LIMIT $3
        "#;

        sqlx::query_as(SQL)
            .bind(protocol)
            .bind(skip)
            .bind(limit)
            .fetch_all(&self.pool)
            .await
    }

    pub async fn get_utilization_level_old(
        &self,
        skip: i64,
        limit: i64,
    ) -> Result<Vec<Utilization_Level>, Error> {
        const SQL: &str = r#"
        SELECT
            (
                (
                    "LP_Pool_total_borrowed_stable" / "LP_Pool_total_value_locked_stable"
                ) * 100
            ) as "Utilization_Level"
        FROM "LP_Pool_State"
        ORDER BY "LP_Pool_timestamp" DESC
        OFFSET $1
        LIMIT $2
        "#;

        sqlx::query_as(SQL)
            .bind(skip)
            .bind(limit)
            .fetch_all(&self.pool)
            .await
    }

    pub async fn get_max_ls_interest_7d(
        &self,
        lpp_address: String,
    ) -> Result<Vec<Max_LP_Ratio>, Error> {
        const SQL: &str = r#"
        SELECT
            DATE("LP_Pool_timestamp") AS "date",
            MAX(
                "LP_Pool_total_borrowed_stable" / "LP_Pool_total_value_locked_stable"
            ) AS "ratio"
        FROM "LP_Pool_State"
        WHERE
            "LP_Pool_timestamp" >= CURRENT_DATE - INTERVAL('7 days') AND
            "LP_Pool_id" = $1
        GROUP BY "date"
        ORDER BY "date" DESC
        "#;

        sqlx::query_as(SQL)
            .bind(lpp_address)
            .fetch_all(&self.pool)
            .await
    }

    pub async fn get_supplied_funds(&self) -> Result<BigDecimal, Error> {
        const SQL: &str = r#"
        WITH "Latest_Pool_Data" AS (
            SELECT 
                "LP_Pool_id",
                "LP_Pool_total_value_locked_stable",
                RANK() OVER (
                    PARTITION BY "LP_Pool_id"
                    ORDER BY "LP_Pool_timestamp" DESC
                ) AS "rank"
            FROM "LP_Pool_State"
            WHERE "LP_Pool_id" IN (
                'nolus1qg5ega6dykkxc307y25pecuufrjkxkaggkkxh7nad0vhyhtuhw3sqaa3c5', -- USDC_AXL_OSMOSIS
                'nolus1qqcr7exupnymvg6m63eqwu8pd4n5x6r5t3pyyxdy7r97rcgajmhqy3gn94', -- USDC_AXL_NEUTRON
                'nolus1ueytzwqyadm6r0z8ajse7g6gzum4w3vv04qazctf8ugqrrej6n4sq027cf', -- USDC_OSMOSIS
                'nolus17vsedux675vc44yu7et9m64ndxsy907v7sfgrk7tw3xnjtqemx3q6t3xw6', -- USDC_NEUTRON
                'nolus1jufcaqm6657xmfltdezzz85quz92rmtd88jk5x0hq9zqseem32ysjdm990', -- ST_ATOM_OSMOSIS
                'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3', -- ALL_BTC_OSMOSIS (÷ 100M)
                'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm', -- ALL_SOL_OSMOSIS (÷ 1B)
                'nolus1lxr7f5xe02jq6cce4puk6540mtu9sg36at2dms5sk69wdtzdrg9qq0t67z'  -- AKT_OSMOSIS
                'nolus1u0zt8x3mkver0447glfupz9lz6wnt62j70p5fhhtu3fr46gcdd9s5dz9l6'  -- ATOM_OSMOSIS
            )
        )
        SELECT 
            SUM(
                "LP_Pool_total_value_locked_stable" / (
                    CASE 
                        WHEN "LP_Pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN
                            100000000 -- ALL_BTC_OSMOSIS
                        WHEN "LP_Pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN
                            1000000000 -- ALL_SOL_OSMOSIS
                        ELSE
                            1000000 -- All other pools
                    END
                )
            ) AS "Total Supplied"
        FROM "Latest_Pool_Data"
        WHERE "rank" = 1
        "#;

        sqlx::query_as(SQL)
            .fetch_optional(&self.pool)
            .await
            .map(|maybe_row| {
                maybe_row.map_or_else(BigDecimal::zero, |(result,)| result)
            })
    }
}
