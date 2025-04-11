use std::str::FromStr as _;

use chrono::{DateTime, Utc};
use sqlx::{types::BigDecimal, Error, QueryBuilder};

use crate::{
    model::{
        LP_Pool_State, Supplied_Borrowed_Series, Table, Utilization_Level,
    },
    types::Max_LP_Ratio,
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
        .persistent(false)
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

        let query = query_builder.build().persistent(false);
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
        .persistent(false)
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
                "LP_Pool_State"."LP_Pool_timestamp", 
                SUM(CASE 
                    WHEN "LP_Pool_State"."LP_Pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN "LP_Pool_State"."LP_Pool_total_value_locked_stable" / 100000000 
                    WHEN "LP_Pool_State"."LP_Pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN "LP_Pool_State"."LP_Pool_total_value_locked_stable" / 1000000000 
                    ELSE "LP_Pool_State"."LP_Pool_total_value_locked_stable" / 1000000 
                END) AS "Supplied", 
                SUM(CASE 
                    WHEN "LP_Pool_State"."LP_Pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN "LP_Pool_State"."LP_Pool_total_borrowed_stable" / 100000000 
                    WHEN "LP_Pool_State"."LP_Pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN "LP_Pool_State"."LP_Pool_total_borrowed_stable" / 1000000000 
                    ELSE "LP_Pool_State"."LP_Pool_total_borrowed_stable" / 1000000 
                END) AS "Borrowed" 
            FROM
                "LP_Pool_State"
            WHERE "LP_Pool_State"."LP_Pool_id" = $1
            GROUP BY 
                "LP_Pool_State"."LP_Pool_timestamp"
            ORDER BY 
                "LP_Pool_State"."LP_Pool_timestamp" DESC
            "#,
        )
        .bind(protocol)
        .persistent(false)
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
                "LP_Pool_State"."LP_Pool_timestamp", 
                SUM(CASE 
                    WHEN "LP_Pool_State"."LP_Pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN "LP_Pool_State"."LP_Pool_total_value_locked_stable" / 100000000 
                    WHEN "LP_Pool_State"."LP_Pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN "LP_Pool_State"."LP_Pool_total_value_locked_stable" / 1000000000 
                    ELSE "LP_Pool_State"."LP_Pool_total_value_locked_stable" / 1000000 
                END) AS "Supplied", 
                SUM(CASE 
                    WHEN "LP_Pool_State"."LP_Pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN "LP_Pool_State"."LP_Pool_total_borrowed_stable" / 100000000 
                    WHEN "LP_Pool_State"."LP_Pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN "LP_Pool_State"."LP_Pool_total_borrowed_stable" / 1000000000 
                    ELSE "LP_Pool_State"."LP_Pool_total_borrowed_stable" / 1000000 
                END) AS "Borrowed" 
            FROM
                "LP_Pool_State"
            WHERE "LP_Pool_State"."LP_Pool_id" IN ({})
            GROUP BY 
                "LP_Pool_State"."LP_Pool_timestamp"
            ORDER BY 
                "LP_Pool_State"."LP_Pool_timestamp" DESC
            "#,
            params
        );

        let mut query: sqlx::query::QueryAs<'_, _, _, _> =
            sqlx::query_as(&query_str).persistent(false);

        for i in protocols {
            query = query.bind(i);
        }

        let data = query.persistent(false).fetch_all(&self.pool).await?;
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
        .persistent(false)
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
        .persistent(false)
        .fetch_all(&self.pool)
        .await?;
        Ok(data)
    }

    pub async fn get_max_ls_interest_7d(
        &self,
        lpp_address: String,
    ) -> Result<Vec<Max_LP_Ratio>, Error> {
        let data = sqlx::query_as(
            r#"
                SELECT
                    DATE("LP_Pool_timestamp") AS "date",
                    MAX(
                    "LP_Pool_total_borrowed_stable" / "LP_Pool_total_value_locked_stable"
                    ) AS "ratio"
                FROM
                    "LP_Pool_State"
                WHERE
                    "LP_Pool_timestamp" >= CURRENT_DATE - INTERVAL '7 days'
                    AND "LP_Pool_id" = $1
                GROUP BY
                    "date"
                ORDER BY "date" DESC
            "#,
        )
        .bind(lpp_address)
        .persistent(false)
        .fetch_all(&self.pool)
        .await?;
        Ok(data)
    }

    pub async fn get_supplied_funds(
        &self,
    ) -> Result<BigDecimal, crate::error::Error> {
        let value: (Option<BigDecimal>,) = sqlx::query_as(
            r#"
              WITH Latest_Pool_Data AS (
                SELECT 
                    "LP_Pool_id",
                    "LP_Pool_total_value_locked_stable",
                    RANK() OVER (PARTITION BY "LP_Pool_id" ORDER BY "LP_Pool_timestamp" DESC) AS rank
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
                    CASE 
                        WHEN "LP_Pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' 
                            THEN "LP_Pool_total_value_locked_stable" / 100000000 -- ALL_BTC_OSMOSIS
                        WHEN "LP_Pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' 
                            THEN "LP_Pool_total_value_locked_stable" / 1000000000 -- ALL_SOL_OSMOSIS
                        ELSE "LP_Pool_total_value_locked_stable" / 1000000 -- All other pools
                    END
                ) AS "Total Supplied"
            FROM Latest_Pool_Data
            WHERE rank = 1
            "#,
        )
        .persistent(false)
        .fetch_one(&self.pool)
        .await?;
        let (amnt,) = value;
        let amnt = amnt.unwrap_or(BigDecimal::from_str("0")?);

        Ok(amnt)
    }
}
