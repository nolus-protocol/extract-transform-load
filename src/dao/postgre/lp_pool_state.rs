use std::iter;

use chrono::{DateTime, Utc};
use sqlx::{error::Error, types::BigDecimal, QueryBuilder};

use crate::{
    custom_uint::UInt63,
    model::{
        LP_Pool_State, Supplied_Borrowed_Series, Table, Utilization_Level,
    },
    types::Max_LP_Ratio,
};

impl Table<LP_Pool_State> {
    pub async fn insert(
        &self,
        LP_Pool_State {
            LP_Pool_id,
            LP_Pool_timestamp,
            LP_Pool_total_value_locked_stable,
            LP_Pool_total_value_locked_asset,
            LP_Pool_total_issued_receipts,
            LP_Pool_total_borrowed_stable,
            LP_Pool_total_borrowed_asset,
            LP_Pool_total_yield_stable,
            LP_Pool_total_yield_asset,
            LP_Pool_min_utilization_threshold: _,
        }: &LP_Pool_State,
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
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, DEFAULT)
        "#;

        sqlx::query(SQL)
            .bind(LP_Pool_id)
            .bind(LP_Pool_timestamp)
            .bind(LP_Pool_total_value_locked_stable)
            .bind(LP_Pool_total_value_locked_asset)
            .bind(LP_Pool_total_issued_receipts)
            .bind(LP_Pool_total_borrowed_stable)
            .bind(LP_Pool_total_borrowed_asset)
            .bind(LP_Pool_total_yield_stable)
            .bind(LP_Pool_total_yield_asset)
            // // FIXME ADDED LINE !!!
            // .bind(LP_Pool_min_utilization_threshold)
            .execute(&self.pool)
            .await
            .map(drop)
    }

    pub async fn insert_many<'r, T>(&self, data: T) -> Result<(), Error>
    where
        T: IntoIterator<Item = &'r LP_Pool_State>,
    {
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

        let mut iter = data.into_iter();

        let Some(first) = iter.next() else {
            return Ok(());
        };

        QueryBuilder::new(SQL)
            .push_values(
                iter::once(first).map(iter),
                |mut b,
                 &LP_Pool_State {
                     ref LP_Pool_id,
                     LP_Pool_timestamp,
                     ref LP_Pool_total_value_locked_stable,
                     ref LP_Pool_total_value_locked_asset,
                     ref LP_Pool_total_issued_receipts,
                     ref LP_Pool_total_borrowed_stable,
                     ref LP_Pool_total_borrowed_asset,
                     ref LP_Pool_total_yield_stable,
                     ref LP_Pool_total_yield_asset,
                     ref LP_Pool_min_utilization_threshold,
                 }| {
                    b.push_bind(LP_Pool_id)
                        .push_bind(LP_Pool_timestamp)
                        .push_bind(LP_Pool_total_value_locked_stable)
                        .push_bind(LP_Pool_total_value_locked_asset)
                        .push_bind(LP_Pool_total_issued_receipts)
                        .push_bind(LP_Pool_total_borrowed_stable)
                        .push_bind(LP_Pool_total_borrowed_asset)
                        .push_bind(LP_Pool_total_yield_stable)
                        .push_bind(LP_Pool_total_yield_asset)
                        .push_bind(LP_Pool_min_utilization_threshold);
                },
            )
            .build()
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
            COALESCE(SUM("LP_Pool_total_value_locked_stable"), 0),
            COALESCE(SUM("LP_Pool_total_borrowed_stable"), 0),
            COALESCE(SUM("LP_Pool_total_yield_stable", 0))
        FROM "LP_Pool_State"
        WHERE "LP_Pool_timestamp" = $1
        "#;

        sqlx::query_as(SQL)
            .bind(datetime)
            .fetch_one(&self.pool)
            .await
    }

    pub async fn get_supplied_borrowed_series(
        &self,
        protocol: &str,
    ) -> Result<Vec<Supplied_Borrowed_Series>, Error> {
        const SQL: &str = r#"
        SELECT
            "LP_Pool_State"."LP_Pool_timestamp",
            SUM(
                CASE 
                    WHEN "LP_Pool_State"."LP_Pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN "LP_Pool_State"."LP_Pool_total_value_locked_stable" / 100000000
                    WHEN "LP_Pool_State"."LP_Pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN "LP_Pool_State"."LP_Pool_total_value_locked_stable" / 1000000000
                    ELSE "LP_Pool_State"."LP_Pool_total_value_locked_stable" / 1000000
                END
            ) AS "Supplied",
            SUM(
                CASE
                    WHEN "LP_Pool_State"."LP_Pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN "LP_Pool_State"."LP_Pool_total_borrowed_stable" / 100000000
                    WHEN "LP_Pool_State"."LP_Pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN "LP_Pool_State"."LP_Pool_total_borrowed_stable" / 1000000000
                    ELSE "LP_Pool_State"."LP_Pool_total_borrowed_stable" / 1000000
                END
            ) AS "Borrowed"
        FROM "LP_Pool_State"
        WHERE "LP_Pool_State"."LP_Pool_id" = $1
        GROUP BY "LP_Pool_State"."LP_Pool_timestamp"
        ORDER BY "LP_Pool_State"."LP_Pool_timestamp" DESC
        "#;

        sqlx::query_as(SQL)
            .bind(protocol)
            .fetch_all(&self.pool)
            .await
    }

    pub async fn get_supplied_borrowed_series_total<'r, I, T>(
        &self,
        protocols: I,
    ) -> Result<Vec<Supplied_Borrowed_Series>, Error>
    where
        I: IntoIterator<Item = &'r T>,
        T: AsRef<str> + ?Sized + 'r,
    {
        const BEFORE_BINDS: &str = r#"
        SELECT
            "LP_Pool_State"."LP_Pool_timestamp",
            SUM(
                CASE
                    WHEN "LP_Pool_State"."LP_Pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN "LP_Pool_State"."LP_Pool_total_value_locked_stable" / 100000000
                    WHEN "LP_Pool_State"."LP_Pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN "LP_Pool_State"."LP_Pool_total_value_locked_stable" / 1000000000
                    ELSE "LP_Pool_State"."LP_Pool_total_value_locked_stable" / 1000000
                END
            ) AS "Supplied",
            SUM(
                CASE
                    WHEN "LP_Pool_State"."LP_Pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN "LP_Pool_State"."LP_Pool_total_borrowed_stable" / 100000000
                    WHEN "LP_Pool_State"."LP_Pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN "LP_Pool_State"."LP_Pool_total_borrowed_stable" / 1000000000
                    ELSE "LP_Pool_State"."LP_Pool_total_borrowed_stable" / 1000000
                END
            ) AS "Borrowed"
        FROM "LP_Pool_State"
        WHERE "LP_Pool_State"."LP_Pool_id" IN (
        "#;

        const AFTER_BINDS: &str = r#"
        )
        GROUP BY "LP_Pool_State"."LP_Pool_timestamp"
        ORDER BY "LP_Pool_State"."LP_Pool_timestamp" DESC
        "#;

        let mut iter = protocols.into_iter();

        let Some(first) = iter.next() else {
            return Ok(vec![]);
        };

        iter::once(first)
            .chain(iter)
            .map(AsRef::as_ref)
            .fold(
                &mut QueryBuilder::new(BEFORE_BINDS),
                QueryBuilder::push_bind,
            )
            .push(AFTER_BINDS)
            .build_query_as()
            .fetch_all(&self.pool)
            .await
    }

    pub async fn get_utilization_level(
        &self,
        protocol: &str,
        skip: UInt63,
        limit: UInt63,
    ) -> Result<Vec<Utilization_Level>, Error> {
        const SQL: &str = r#"
        SELECT ("LP_Pool_total_borrowed_stable" / "LP_Pool_total_value_locked_stable") * 100 as "Utilization_Level"
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
        skip: UInt63,
        limit: UInt63,
    ) -> Result<Vec<Utilization_Level>, Error> {
        const SQL: &str = r#"
        SELECT ("LP_Pool_total_borrowed_stable" / "LP_Pool_total_value_locked_stable") * 100 as "Utilization_Level"
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
        lpp_address: &str,
    ) -> Result<Vec<Max_LP_Ratio>, Error> {
        const SQL: &'static str = r#"
        SELECT
            DATE("LP_Pool_timestamp") AS "date",
            MAX("LP_Pool_total_borrowed_stable" / "LP_Pool_total_value_locked_stable") AS "ratio"
        FROM "LP_Pool_State"
        WHERE
            "LP_Pool_timestamp" >= CURRENT_DATE - INTERVAL '7 days' AND
            "LP_Pool_id" = $1
        GROUP BY "date"
        ORDER BY "date" DESC
        "#;

        sqlx::query_as(SQL)
            .bind(lpp_address)
            .fetch_all(&self.pool)
            .await
    }
}
