use std::str::FromStr as _;

use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::{types::BigDecimal, Error, FromRow, QueryBuilder, Transaction};

#[derive(Debug, FromRow)]
pub struct MonthlyActiveWallets {
    pub month: String,
    pub unique_addresses: i64,
}

#[derive(Debug, Clone, FromRow)]
pub struct LoanGranted {
    pub asset: String,
    pub loan: BigDecimal,
}

#[derive(Debug, Clone, FromRow)]
pub struct HistoricallyOpened {
    pub contract_id: String,
    pub user: String,
    pub leased_asset: String,
    pub opening_date: DateTime<Utc>,
    pub position_type: String,
    pub down_payment_amount: BigDecimal,
    pub down_payment_asset: String,
    pub loan: BigDecimal,
    pub total_position_amount_lpn: BigDecimal,
    pub price: Option<BigDecimal>,
    pub open: bool,
    pub liquidation_price: Option<BigDecimal>,
}

#[derive(Debug, Clone, FromRow, Serialize)]
pub struct RealizedPnlWallet {
    pub contract_id: String,
    pub user: String,
    pub leased_asset: String,
    pub down_payment_asset: String,
    pub opening_date: DateTime<Utc>,
    pub close_timestamp: DateTime<Utc>,
    pub down_payment_stable: BigDecimal,
    pub manual_repayments_stable: BigDecimal,
    pub total_outflow_stable: BigDecimal,
    pub liquidations_stable: BigDecimal,
    pub liquidation_events: i64,
    pub returned_lpn: Option<String>,
    pub returned_amount_lpn_units: Option<BigDecimal>,
    pub returned_amount_stable: Option<BigDecimal>,
    pub realized_pnl_stable: Option<BigDecimal>,
}

use crate::{
    model::{
        Borrow_APR, LS_Amount, LS_History, LS_Opening, LS_Realized_Pnl_Data,
        Leased_Asset, Leases_Monthly, Table,
    },
};

use super::{DataBase, QueryResult};

impl Table<LS_Opening> {
    pub async fn isExists(
        &self,
        ls_opening: &LS_Opening,
    ) -> Result<bool, crate::error::Error> {
        let (value,): (i64,) = sqlx::query_as(
            r#"
            SELECT
                COUNT(*)
            FROM "LS_Opening"
            WHERE
                "LS_contract_id" = $1
            "#,
        )
        .bind(&ls_opening.LS_contract_id)
        .persistent(true)
        .fetch_one(&self.pool)
        .await?;

        if value > 0 {
            return Ok(true);
        }

        Ok(false)
    }

    pub async fn insert(
        &self,
        data: LS_Opening,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<QueryResult, Error> {
        sqlx::query(
            r#"
            INSERT INTO "LS_Opening" (
                "LS_contract_id",
                "LS_address_id",
                "LS_asset_symbol",
                "LS_interest",
                "LS_timestamp",
                "LS_loan_pool_id",
                "LS_loan_amnt_stable",
                "LS_loan_amnt_asset",
                "LS_cltr_symbol",
                "LS_cltr_amnt_stable",
                "LS_cltr_amnt_asset",
                "LS_native_amnt_stable",
                "LS_native_amnt_nolus",
                "Tx_Hash",
                "LS_loan_amnt",
                "LS_lpn_loan_amnt",
                "LS_position_type",
                "LS_lpn_symbol",
                "LS_lpn_decimals",
                "LS_opening_price",
                "LS_liquidation_price_at_open"
            )
            VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)
            "#,
        )
        .bind(&data.LS_contract_id)
        .bind(&data.LS_address_id)
        .bind(&data.LS_asset_symbol)
        .bind(data.LS_interest)
        .bind(data.LS_timestamp)
        .bind(&data.LS_loan_pool_id)
        .bind(&data.LS_loan_amnt_stable)
        .bind(&data.LS_loan_amnt_asset)
        .bind(&data.LS_cltr_symbol)
        .bind(&data.LS_cltr_amnt_stable)
        .bind(&data.LS_cltr_amnt_asset)
        .bind(&data.LS_native_amnt_stable)
        .bind(&data.LS_native_amnt_nolus)
        .bind(&data.Tx_Hash)
        .bind(&data.LS_loan_amnt)
        .bind(&data.LS_lpn_loan_amnt)
        .bind(&data.LS_position_type)
        .bind(&data.LS_lpn_symbol)
        .bind(&data.LS_lpn_decimals)
        .bind(&data.LS_opening_price)
        .bind(&data.LS_liquidation_price_at_open)
        .persistent(true)
        .execute(&mut **transaction)
        .await
    }

    /// Inserts a record if it doesn't already exist, using ON CONFLICT DO NOTHING.
    /// This is more efficient than calling isExists() followed by insert().
    pub async fn insert_if_not_exists(
        &self,
        data: LS_Opening,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<QueryResult, Error> {
        sqlx::query(
            r#"
            INSERT INTO "LS_Opening" (
                "LS_contract_id",
                "LS_address_id",
                "LS_asset_symbol",
                "LS_interest",
                "LS_timestamp",
                "LS_loan_pool_id",
                "LS_loan_amnt_stable",
                "LS_loan_amnt_asset",
                "LS_cltr_symbol",
                "LS_cltr_amnt_stable",
                "LS_cltr_amnt_asset",
                "LS_native_amnt_stable",
                "LS_native_amnt_nolus",
                "Tx_Hash",
                "LS_loan_amnt",
                "LS_lpn_loan_amnt",
                "LS_position_type",
                "LS_lpn_symbol",
                "LS_lpn_decimals",
                "LS_opening_price",
                "LS_liquidation_price_at_open"
            )
            VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)
            ON CONFLICT ("LS_contract_id") DO NOTHING
            "#,
        )
        .bind(&data.LS_contract_id)
        .bind(&data.LS_address_id)
        .bind(&data.LS_asset_symbol)
        .bind(data.LS_interest)
        .bind(data.LS_timestamp)
        .bind(&data.LS_loan_pool_id)
        .bind(&data.LS_loan_amnt_stable)
        .bind(&data.LS_loan_amnt_asset)
        .bind(&data.LS_cltr_symbol)
        .bind(&data.LS_cltr_amnt_stable)
        .bind(&data.LS_cltr_amnt_asset)
        .bind(&data.LS_native_amnt_stable)
        .bind(&data.LS_native_amnt_nolus)
        .bind(&data.Tx_Hash)
        .bind(&data.LS_loan_amnt)
        .bind(&data.LS_lpn_loan_amnt)
        .bind(&data.LS_position_type)
        .bind(&data.LS_lpn_symbol)
        .bind(&data.LS_lpn_decimals)
        .bind(&data.LS_opening_price)
        .bind(&data.LS_liquidation_price_at_open)
        .persistent(true)
        .execute(&mut **transaction)
        .await
    }

    pub async fn insert_many(
        &self,
        data: &Vec<LS_Opening>,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<(), Error> {
        if data.is_empty() {
            return Ok(());
        }

        let mut query_builder: QueryBuilder<DataBase> = QueryBuilder::new(
            r#"
            INSERT INTO "LS_Opening" (
                "LS_contract_id",
                "LS_address_id",
                "LS_asset_symbol",
                "LS_interest",
                "LS_timestamp",
                "LS_loan_pool_id",
                "LS_loan_amnt_stable",
                "LS_loan_amnt_asset",
                "LS_cltr_symbol",
                "LS_cltr_amnt_stable",
                "LS_cltr_amnt_asset",
                "LS_native_amnt_stable",
                "LS_native_amnt_nolus",
                "Tx_Hash",
                "LS_loan_amnt",
                "LS_lpn_loan_amnt",
                "LS_position_type",
                "LS_lpn_symbol",
                "LS_lpn_decimals",
                "LS_opening_price",
                "LS_liquidation_price_at_open"
            )"#,
        );

        query_builder.push_values(data, |mut b, ls| {
            b.push_bind(&ls.LS_contract_id)
                .push_bind(&ls.LS_address_id)
                .push_bind(&ls.LS_asset_symbol)
                .push_bind(ls.LS_interest)
                .push_bind(ls.LS_timestamp)
                .push_bind(&ls.LS_loan_pool_id)
                .push_bind(&ls.LS_loan_amnt_stable)
                .push_bind(&ls.LS_loan_amnt_asset)
                .push_bind(&ls.LS_cltr_symbol)
                .push_bind(&ls.LS_cltr_amnt_stable)
                .push_bind(&ls.LS_cltr_amnt_asset)
                .push_bind(&ls.LS_native_amnt_stable)
                .push_bind(&ls.LS_native_amnt_nolus)
                .push_bind(&ls.Tx_Hash)
                .push_bind(&ls.LS_loan_amnt)
                .push_bind(&ls.LS_lpn_loan_amnt)
                .push_bind(&ls.LS_position_type)
                .push_bind(&ls.LS_lpn_symbol)
                .push_bind(&ls.LS_lpn_decimals)
                .push_bind(&ls.LS_opening_price)
                .push_bind(&ls.LS_liquidation_price_at_open);
        });

        let query = query_builder.build().persistent(true);
        query.execute(&mut **transaction).await?;
        Ok(())
    }

    pub async fn count(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<i64, crate::error::Error> {
        let (value,): (i64,) = sqlx::query_as(
            r#"
            SELECT
                COUNT(*)
            FROM "LS_Opening" WHERE "LS_timestamp" > $1 AND "LS_timestamp" <= $2
            "#,
        )
        .bind(from)
        .bind(to)
        .persistent(true)
        .fetch_one(&self.pool)
        .await?;
        Ok(value)
    }

    pub async fn get_cltr_amnt_opened_stable_sum(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<BigDecimal, crate::error::Error> {
        let value: (Option<BigDecimal>,) = sqlx::query_as(
            r#"
            SELECT
                SUM("LS_cltr_amnt_stable")
            FROM "LS_Opening" WHERE "LS_timestamp" > $1 AND "LS_timestamp" <= $2
            "#,
        )
        .bind(from)
        .bind(to)
        .persistent(true)
        .fetch_one(&self.pool)
        .await?;
        let (amnt,) = value;
        let amnt = amnt.unwrap_or(BigDecimal::from_str("0")?);

        Ok(amnt)
    }

    pub async fn get_loan_amnt_stable_sum(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<BigDecimal, crate::error::Error> {
        let value: (Option<BigDecimal>,) = sqlx::query_as(
            r#"
            SELECT
                SUM("LS_loan_amnt_stable")
            FROM "LS_Opening" WHERE "LS_timestamp" > $1 AND "LS_timestamp" <= $2
            "#,
        )
        .bind(from)
        .bind(to)
        .persistent(true)
        .fetch_one(&self.pool)
        .await?;
        let (amnt,) = value;
        let amnt = amnt.unwrap_or(BigDecimal::from_str("0")?);

        Ok(amnt)
    }

    pub async fn get_ls_cltr_amnt_stable_sum(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<BigDecimal, crate::error::Error> {
        let value: (Option<BigDecimal>,) = sqlx::query_as(
            r#"
            SELECT
                SUM("LS_cltr_amnt_stable")
            FROM "LS_Opening"
            LEFT JOIN
                "LS_Closing"
            ON
                "LS_Opening"."LS_contract_id" = "LS_Closing"."LS_contract_id"
            WHERE "LS_Closing"."LS_timestamp" > $1 AND "LS_Closing"."LS_timestamp" <= $2
            "#,
        )
        .bind(from)
        .bind(to)
        .persistent(true)
        .fetch_one(&self.pool)
        .await?;
        let (amnt,) = value;
        let amnt = amnt.unwrap_or(BigDecimal::from_str("0")?);

        Ok(amnt)
    }

    pub async fn get_ls_amnt_stable_sum(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<BigDecimal, crate::error::Error> {
        let value: (Option<BigDecimal>,) = sqlx::query_as(
            r#"
            SELECT
                SUM("LS_loan_amnt_stable" + "LS_cltr_amnt_stable")
            FROM "LS_Opening"
            LEFT JOIN
                "LS_Closing"
            ON
                "LS_Opening"."LS_contract_id" = "LS_Closing"."LS_contract_id"
            WHERE "LS_Closing"."LS_timestamp" > $1 AND "LS_Closing"."LS_timestamp" <= $2
            "#,
        )
        .bind(from)
        .bind(to)
        .persistent(true)
        .fetch_one(&self.pool)
        .await?;
        let (amnt,) = value;
        let amnt = amnt.unwrap_or(BigDecimal::from_str("0")?);

        Ok(amnt)
    }

    pub async fn get_borrow_apr(
        &self,
        protocol: String,
        skip: i64,
        limit: i64,
    ) -> Result<Vec<Borrow_APR>, Error> {
        let data = sqlx::query_as(
            r#"
            SELECT "LS_interest" / 10.0 AS "APR" FROM "LS_Opening" WHERE "LS_loan_pool_id" = $1 ORDER BY "LS_timestamp" DESC OFFSET $2 LIMIT $3
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

    /// Get borrow APR with time window filtering
    pub async fn get_borrow_apr_with_window(
        &self,
        protocol: String,
        months: Option<i32>,
        from: Option<DateTime<Utc>>,
    ) -> Result<Vec<Borrow_APR>, Error> {
        // Build time conditions dynamically
        let mut conditions = vec![r#""LS_loan_pool_id" = $1"#.to_string()];

        if let Some(m) = months {
            conditions.push(format!(
                r#""LS_timestamp" >= NOW() - INTERVAL '{} months'"#,
                m
            ));
        }

        if from.is_some() {
            conditions.push(r#""LS_timestamp" > $2"#.to_string());
        }

        let where_clause = conditions.join(" AND ");
        let query = format!(
            r#"
            SELECT "LS_interest" / 10.0 AS "APR"
            FROM "LS_Opening"
            WHERE {}
            ORDER BY "LS_timestamp" DESC
            "#,
            where_clause
        );

        let mut query_builder = sqlx::query_as::<_, Borrow_APR>(&query).bind(&protocol);

        if let Some(from_ts) = from {
            query_builder = query_builder.bind(from_ts);
        }

        let data = query_builder.persistent(true).fetch_all(&self.pool).await?;
        Ok(data)
    }

    pub async fn get_leased_assets(
        &self,
        protocol: String,
    ) -> Result<Vec<Leased_Asset>, Error> {
        let data = sqlx::query_as(
            r#"
            SELECT "LS_asset_symbol" AS "Asset", SUM("LS_loan_amnt_asset" / 1000000) AS "Loan" FROM "LS_Opening" WHERE "LS_loan_pool_id" = $1 GROUP BY "Asset"
            "#,
        )
        .bind(protocol)
        .persistent(true)
        .fetch_all(&self.pool)
        .await?;
        Ok(data)
    }

    pub async fn get_leased_assets_total(
        &self,
    ) -> Result<Vec<Leased_Asset>, Error> {
        let data = sqlx::query_as(
            r#"
            WITH LatestTimestamps AS (
            SELECT
                "LS_contract_id",
                MAX("LS_timestamp") AS "MaxTimestamp"
            FROM
                "LS_State"
            WHERE
                "LS_timestamp" > (now() - INTERVAL '2 hours')
            GROUP BY
                "LS_contract_id"
            ),
            Opened AS (
                SELECT
                    s."LS_contract_id",
                    s."LS_amnt_stable",
                    CASE
                        WHEN lo."LS_loan_pool_id" = 'nolus1jufcaqm6657xmfltdezzz85quz92rmtd88jk5x0hq9zqseem32ysjdm990' THEN 'ST_ATOM (Short)'
                        WHEN lo."LS_loan_pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN 'ALL_BTC (Short)'
                        WHEN lo."LS_loan_pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN 'ALL_SOL (Short)'
                        WHEN lo."LS_loan_pool_id" = 'nolus1lxr7f5xe02jq6cce4puk6540mtu9sg36at2dms5sk69wdtzdrg9qq0t67z' THEN 'AKT (Short)'
                        WHEN lo."LS_loan_pool_id" = 'nolus1u0zt8x3mkver0447glfupz9lz6wnt62j70p5fhhtu3fr46gcdd9s5dz9l6' THEN 'ATOM (Short)'
                        ELSE lo."LS_asset_symbol"
                    END AS "Asset Type"
                FROM
                    "LS_State" s
                INNER JOIN
                    LatestTimestamps lt ON s."LS_contract_id" = lt."LS_contract_id" AND s."LS_timestamp" = lt."MaxTimestamp"
                INNER JOIN
                    "LS_Opening" lo ON lo."LS_contract_id" = s."LS_contract_id"
                WHERE
                    s."LS_amnt_stable" > 0
            ),
            Lease_Value_Table AS (
                SELECT
                    op."Asset Type" AS "Asset",
                    CASE
                        WHEN "Asset Type" IN ('ALL_BTC', 'WBTC', 'CRO') THEN "LS_amnt_stable" / 100000000
                        WHEN "Asset Type" IN ('ALL_SOL') THEN "LS_amnt_stable" / 1000000000
                        WHEN "Asset Type" IN ('PICA') THEN "LS_amnt_stable" / 1000000000000
                        WHEN "Asset Type" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS', 'ALL_ETH') THEN "LS_amnt_stable" / 1000000000000000000
                    ELSE "LS_amnt_stable" / 1000000
                END AS "Lease Value"
                FROM
                    Opened op
            )
            SELECT
                "Asset",
                SUM("Lease Value") AS "Loan"
            FROM
                Lease_Value_Table
            GROUP BY
                "Asset"
            ORDER BY
                "Loan" DESC;
            "#,
        )
        .persistent(true)
        .fetch_all(&self.pool)
        .await?;
        Ok(data)
    }

    pub async fn get_earn_apr_interest(
        &self,
        protocol: String,
        max_interest: f32,
    ) -> Result<BigDecimal, crate::error::Error> {
        let sql = format!(
            r#"
                 WITH Last_Hour_States AS (
                SELECT
                    *
                FROM
                    "LS_State"
                WHERE
                    "LS_timestamp" >= NOW() - INTERVAL '2 hours'
                ),
                Last_Hour_Pool_State AS (
                SELECT
                    (
                    "LP_Pool_total_borrowed_stable" / NULLIF("LP_Pool_total_value_locked_stable", 0)
                    ) AS utilization_rate
                FROM
                    "LP_Pool_State"
                WHERE
                    "LP_Pool_id" = '{}'
                ORDER BY
                    "LP_Pool_timestamp" DESC
                LIMIT
                    1
                ),
                APRCalc AS (
                SELECT
                    (AVG(o."LS_interest") / 10.0 - {}) * (
                    SELECT
                        utilization_rate
                    FROM
                        Last_Hour_Pool_State
                    ) AS apr
                FROM
                    Last_Hour_States s
                    JOIN "LS_Opening" o ON s."LS_contract_id" = o."LS_contract_id"
                WHERE
                    o."LS_loan_pool_id" = '{}'
                )
                SELECT
                    (POWER((1 + ("apr" / 100 / 365)), 365) - 1) * 100 AS "PERCENT"
                FROM APRCalc

            "#,
            protocol.to_owned(),
            max_interest,
            protocol.to_owned()
        );
        let value: Option<(BigDecimal,)> = sqlx::query_as(&sql)
            .persistent(true)
            .fetch_optional(&self.pool)
            .await?;

        let amnt = value.unwrap_or((BigDecimal::from_str("0")?,));

        Ok(amnt.0)
    }

    pub async fn get_earn_apr(
        &self,
        protocol: String,
    ) -> Result<BigDecimal, crate::error::Error> {
        let value: Option<(BigDecimal,)> = sqlx::query_as(
            r#"
                WITH Last_Hour_States AS (
                SELECT
                    *
                FROM
                    "LS_State"
                WHERE
                    "LS_timestamp" >= NOW() - INTERVAL '2 hours'
                ),
                Last_Hour_Pool_State AS (
                SELECT
                    (
                    "LP_Pool_total_borrowed_stable" / NULLIF("LP_Pool_total_value_locked_stable", 0)
                    ) AS utilization_rate
                FROM
                    "LP_Pool_State"
                WHERE
                    "LP_Pool_id" = $1
                ORDER BY
                    "LP_Pool_timestamp" DESC
                LIMIT
                    1
                ),
                APRCalc AS (
                SELECT
                    (AVG(o."LS_interest") / 10.0 - 4) * (
                    SELECT
                        utilization_rate
                    FROM
                        Last_Hour_Pool_State
                    ) AS apr
                FROM
                    Last_Hour_States s
                    JOIN "LS_Opening" o ON s."LS_contract_id" = o."LS_contract_id"
                WHERE
                    o."LS_loan_pool_id" = $1
                )
                SELECT
                    (POWER((1 + ("apr" / 100 / 365)), 365) - 1) * 100 AS "PERCENT"
                FROM APRCalc
            "#,
        )
        .bind(&protocol)
        .persistent(true)
        .fetch_optional(&self.pool)
        .await?;
        let amnt = value.unwrap_or((BigDecimal::from_str("0")?,));

        Ok(amnt.0)
    }

    pub async fn get(
        &self,
        LS_contract_id: String,
    ) -> Result<Option<LS_Opening>, Error> {
        sqlx::query_as(
            r#"
             SELECT * FROM "LS_Opening" WHERE "LS_contract_id" = $1
            "#,
        )
        .bind(LS_contract_id)
        .persistent(true)
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn get_borrowed(
        &self,
        protocol: String,
    ) -> Result<BigDecimal, crate::error::Error> {
        let value: Option<(BigDecimal,)>   = sqlx::query_as(
            r#"
                SELECT SUM("LS_loan_amnt_asset" / 1000000) AS "Loan" FROM "LS_Opening" WHERE "LS_loan_pool_id" = $1
            "#,
        )
        .bind(protocol)
        .persistent(true)
        .fetch_optional(&self.pool)
        .await?;
        let amnt = value.unwrap_or((BigDecimal::from_str("0")?,));

        Ok(amnt.0)
    }

    pub async fn get_borrowed_total(
        &self,
    ) -> Result<BigDecimal, crate::error::Error> {
        let value: Option<(BigDecimal,)> = sqlx::query_as(
            r#"
                SELECT SUM("LS_loan_amnt_asset" / 1000000) AS "Loan" FROM "LS_Opening"
            "#,
        )
        .persistent(true)
        .fetch_optional(&self.pool)
        .await?;
        let amnt = value.unwrap_or((BigDecimal::from_str("0")?,));

        Ok(amnt.0)
    }

    pub async fn get_leases(
        &self,
        leases: Vec<&str>,
    ) -> Result<Vec<LS_Opening>, Error> {
        let mut params = String::from("$1");

        for i in 1..leases.len() {
            params += &format!(", ${}", i + 1);
        }

        let query_str = format!(
            r#"
            SELECT * FROM "LS_Opening" WHERE "LS_contract_id" IN({})
        "#,
            params
        );
        let mut query: sqlx::query::QueryAs<'_, _, _, _> =
            sqlx::query_as(&query_str);

        for i in leases {
            query = query.bind(i);
        }

        let data = query.persistent(true).fetch_all(&self.pool).await?;
        Ok(data)
    }

    pub async fn get_total_tx_value(
        &self,
    ) -> Result<BigDecimal, crate::error::Error> {
        let value: Option<(Option<BigDecimal>,)>  = sqlx::query_as(
          r#"
                WITH Opened_Leases AS (
                    SELECT
                    CASE
                        WHEN "LS_cltr_symbol" IN ('ALL_BTC', 'WBTC', 'CRO') THEN "LS_cltr_amnt_stable" / 100000000
                        WHEN "LS_cltr_symbol" IN ('ALL_SOL') THEN "LS_cltr_amnt_stable" / 1000000000
                        WHEN "LS_cltr_symbol" IN ('PICA') THEN "LS_cltr_amnt_stable" / 1000000000000
                        WHEN "LS_cltr_symbol" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS', 'ALL_ETH') THEN "LS_cltr_amnt_stable" / 1000000000000000000
                        ELSE "LS_cltr_amnt_stable" / 1000000
                    END AS "Down Payment Amount",
                    CASE
                        WHEN "LS_loan_pool_id" = 'nolus1jufcaqm6657xmfltdezzz85quz92rmtd88jk5x0hq9zqseem32ysjdm990' THEN "LS_loan_amnt_stable" / 1000000
                        WHEN "LS_loan_pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN "LS_loan_amnt_stable" / 100000000
                        WHEN "LS_loan_pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN "LS_loan_amnt_stable" / 1000000000
                        WHEN "LS_loan_pool_id" = 'nolus1lxr7f5xe02jq6cce4puk6540mtu9sg36at2dms5sk69wdtzdrg9qq0t67z' THEN "LS_loan_amnt_stable" / 1000000
                        ELSE "LS_loan_amnt_asset" / 1000000
                        END AS "Loan"
                    FROM "LS_Opening"
                    ),
                    LP_Deposits AS (
                    SELECT
                        CASE
                        WHEN "LP_address_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN "LP_amnt_stable" / 100000000    -- Example for ALL_BTC or similar
                        WHEN "LP_address_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN "LP_amnt_stable" / 1000000000   -- Example for ALL_SOL
                        ELSE "LP_amnt_stable" / 1000000    -- Default divisor
                        END AS "Volume"
                    FROM "LP_Deposit"
                    ),

                    LP_Withdrawals AS (
                    SELECT
                        CASE
                        WHEN "LP_address_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN "LP_amnt_stable" / 100000000    -- Example for ALL_BTC or similar
                        WHEN "LP_address_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN "LP_amnt_stable" / 1000000000   -- Example for ALL_SOL
                        ELSE "LP_amnt_stable" / 1000000    -- Default divisor
                        END AS "Volume"
                    FROM "LP_Withdraw"
                    ),
                    LS_Close AS (
                    SELECT
                        CASE
                        WHEN "LS_payment_symbol" IN ('ALL_BTC', 'WBTC', 'CRO') THEN "LS_payment_amnt_stable" / 100000000
                        WHEN "LS_payment_symbol" IN ('ALL_SOL') THEN "LS_payment_amnt_stable" / 1000000000
                        WHEN "LS_payment_symbol" IN ('PICA') THEN "LS_payment_amnt_stable" / 1000000000000
                        WHEN "LS_payment_symbol" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS', 'ALL_ETH') THEN "LS_payment_amnt_stable" / 1000000000000000000
                        ELSE "LS_payment_amnt_stable" / 1000000
                        END AS "Volume"
                    FROM "LS_Close_Position"
                    ),
                    LS_Repayment AS (
                    SELECT
                        CASE
                        WHEN "LS_payment_symbol" IN ('ALL_BTC', 'WBTC', 'CRO') THEN "LS_payment_amnt_stable" / 100000000
                        WHEN "LS_payment_symbol" IN ('ALL_SOL') THEN "LS_payment_amnt_stable" / 1000000000
                        WHEN "LS_payment_symbol" IN ('PICA') THEN "LS_payment_amnt_stable" / 1000000000000
                        WHEN "LS_payment_symbol" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS', 'ALL_ETH') THEN "LS_payment_amnt_stable" / 1000000000000000000
                        ELSE "LS_payment_amnt_stable" / 1000000
                        END AS "Volume"
                    FROM "LS_Repayment"
                    )

                    SELECT
                        SUM ("Volume") AS "Tx Value"
                    FROM (
                        SELECT ("Down Payment Amount" + "Loan") AS "Volume" FROM Opened_Leases
                        UNION ALL
                        SELECT "Volume" FROM LP_Deposits
                        UNION ALL
                        SELECT "Volume" FROM LP_Withdrawals
                        UNION ALL
                    SELECT "Volume" FROM LS_Close
                        UNION ALL
                        SELECT "Volume" FROM LS_Repayment
                    ) AS combined_data
              "#,
          )
          .persistent(true)
          .fetch_optional(&self.pool)
          .await?;

        let default = BigDecimal::from_str("0")?;
        let amount = if let Some(v) = value {
            v.0
        } else {
            Some(default.to_owned())
        };

        Ok(amount.unwrap_or(default.to_owned()))
    }

    pub async fn get_leases_addresses(
        &self,
        address: String,
        search: Option<String>,
        skip: i64,
        limit: i64,
    ) -> Result<Vec<(String,)>, Error> {
        let data = sqlx::query_as(
            r#"
                SELECT
                    a."LS_contract_id"
                FROM "LS_Opening" a
                WHERE
                    a."LS_address_id" = $1
                    AND (
                        $2::text IS NULL
                        OR a."LS_contract_id"::text ILIKE '%' || $2 || '%'
                    )
                ORDER BY a."LS_timestamp" DESC
                OFFSET $3 LIMIT $4
                "#,
        )
        .bind(address)
        .bind(search)
        .bind(skip)
        .bind(limit)
        .persistent(true)
        .fetch_all(&self.pool)
        .await?;
        Ok(data)
    }

    pub async fn get_leases_by_address(
        &self,
        address: String,
        skip: i64,
        limit: i64,
    ) -> Result<Vec<LS_Opening>, Error> {
        let data = sqlx::query_as(
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
                    a."LS_native_amnt_nolus",
                    a."Tx_Hash",
                    a."LS_loan_amnt",
                    a."LS_lpn_loan_amnt",
                    a."LS_position_type",
                    a."LS_lpn_symbol",
                    a."LS_lpn_decimals",
                    a."LS_opening_price",
                    a."LS_liquidation_price_at_open"
                FROM
                    "LS_Opening" as a
                LEFT JOIN
                    "LS_Closing" as b
                ON a."LS_contract_id" = b."LS_contract_id"
                WHERE
                    a."LS_address_id" = $1
                ORDER BY "LS_timestamp" DESC
                OFFSET $2 LIMIT $3
            "#,
        )
        .bind(address)
        .bind(skip)
        .bind(limit)
        .persistent(true)
        .fetch_all(&self.pool)
        .await?;
        Ok(data)
    }

    //TODO: delete
    pub async fn get_leases_data(
        &self,
        leases: Vec<String>,
    ) -> Result<Vec<LS_Opening>, Error> {
        if leases.is_empty() {
            return Ok(vec![]);
        }

        // Build parameterized query to avoid SQL injection
        let mut params = String::from("$1");
        for i in 1..leases.len() {
            params += &format!(", ${}", i + 1);
        }

        let query_str = format!(
            r#"SELECT * FROM "LS_Opening" WHERE "LS_contract_id" IN ({})"#,
            params
        );

        let mut query: sqlx::query::QueryAs<'_, _, LS_Opening, _> =
            sqlx::query_as(&query_str);

        for lease in &leases {
            query = query.bind(lease);
        }

        let data = query.persistent(true).fetch_all(&self.pool).await?;
        Ok(data)
    }

    pub async fn update_ls_loan_amnt(
        &self,
        ls_opening: &LS_Opening,
    ) -> Result<(), crate::error::Error> {
        sqlx::query(
            r#"
                    UPDATE
                        "LS_Opening"
                    SET
                        "LS_loan_amnt" = $1
                    WHERE
                        "LS_contract_id" = $2
                "#,
        )
        .bind(&ls_opening.LS_loan_amnt)
        .bind(&ls_opening.LS_contract_id)
        .persistent(true)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn update_ls_lpn_loan_amnt(
        &self,
        ls_opening: &LS_Opening,
    ) -> Result<(), crate::error::Error> {
        sqlx::query(
            r#"
                    UPDATE
                        "LS_Opening"
                    SET
                        "LS_lpn_loan_amnt" = $1
                    WHERE
                        "LS_contract_id" = $2
                "#,
        )
        .bind(&ls_opening.LS_lpn_loan_amnt)
        .bind(&ls_opening.LS_contract_id)
        .persistent(true)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_lease_history(
        &self,
        contract_id: String,
    ) -> Result<Vec<LS_History>, crate::error::Error> {
        let data = sqlx::query_as(
            r#"
                SELECT * FROM (
                    SELECT
                        "LS_payment_symbol" as "symbol",
                        "LS_payment_amnt" as "amount",
                        NULL as "ls_amnt_symbol",
                        NULL as "ls_amnt",
                        "LS_timestamp" as "time",
                        'repay' as "type",
                        NULL as "additional"

                    FROM "LS_Repayment"
                    WHERE "LS_contract_id" = $1

                    UNION ALL

                    SELECT
                        "LS_payment_symbol" as "symbol",
                        "LS_payment_amnt" as "amount",
                        "LS_amnt_symbol" as "ls_amnt_symbol",
                        "LS_amnt" as "ls_amnt",
                        "LS_timestamp" as "time",
                        'market-close' as "type",
                        NULL as "additional"

                    FROM "LS_Close_Position"
                    WHERE "LS_contract_id" = $1

                    UNION ALL

                    SELECT
                        "LS_payment_symbol" as "symbol",
                        "LS_payment_amnt" as "amount",
                        "LS_amnt_symbol" as "ls_amnt_symbol",
                        "LS_amnt" as "ls_amnt",
                        "LS_timestamp" as "time",
                        'liquidation' as "type",
                        "LS_transaction_type" as "additional"
                    FROM "LS_Liquidation"
                    WHERE "LS_contract_id" = $1
                ) AS combined_data
                ORDER BY time ASC;
            "#,
        )
        .bind(contract_id)
        .persistent(true)
        .fetch_all(&self.pool)
        .await?;

        Ok(data)
    }

    pub async fn get_leases_monthly(
        &self,
    ) -> Result<Vec<Leases_Monthly>, Error> {
        let data = sqlx::query_as(
            r#"
            WITH Historically_Opened_Base AS (
            SELECT
                DISTINCT ON (lso."LS_contract_id") lso."LS_contract_id" AS "Contract ID",
                lso."LS_address_id" AS "User",
                CASE
                        WHEN "LS_loan_pool_id" = 'nolus1jufcaqm6657xmfltdezzz85quz92rmtd88jk5x0hq9zqseem32ysjdm990' THEN 'ST_ATOM'
                        WHEN "LS_loan_pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN 'ALL_BTC'
                        WHEN "LS_loan_pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN 'ALL_SOL'
                        WHEN "LS_loan_pool_id" = 'nolus1lxr7f5xe02jq6cce4puk6540mtu9sg36at2dms5sk69wdtzdrg9qq0t67z' THEN 'AKT'
                        ELSE "LS_asset_symbol"
                    END AS "Leased Asset",
                DATE_TRUNC('month', "LS_timestamp") AS "Date",
                CASE
                WHEN "LS_cltr_symbol" IN ('ALL_BTC', 'WBTC', 'CRO') THEN "LS_cltr_amnt_stable" / 100000000
                WHEN "LS_cltr_symbol" IN ('ALL_SOL') THEN "LS_cltr_amnt_stable" / 1000000000
                WHEN "LS_cltr_symbol" IN ('PICA') THEN "LS_cltr_amnt_stable" / 1000000000000
                WHEN "LS_cltr_symbol" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS', 'ALL_ETH') THEN "LS_cltr_amnt_stable" / 1000000000000000000
                ELSE "LS_cltr_amnt_stable" / 1000000
                END AS "Down Payment Amount",
                CASE
                        WHEN "LS_loan_pool_id" = 'nolus1jufcaqm6657xmfltdezzz85quz92rmtd88jk5x0hq9zqseem32ysjdm990' THEN "LS_loan_amnt_stable" / 1000000
                        WHEN "LS_loan_pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN "LS_loan_amnt_stable" / 100000000
                        WHEN "LS_loan_pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN "LS_loan_amnt_stable" / 1000000000
                        WHEN "LS_loan_pool_id" = 'nolus1lxr7f5xe02jq6cce4puk6540mtu9sg36at2dms5sk69wdtzdrg9qq0t67z' THEN "LS_loan_amnt_stable" / 1000000
                        ELSE "LS_loan_amnt_asset" / 1000000
                    END
            AS "Loan Amount"
            FROM
                "LS_Opening" lso
            )
            SELECT
            "Date",
            SUM("Down Payment Amount") + SUM("Loan Amount") AS "Amount"
            FROM
            Historically_Opened_Base
            GROUP BY
            "Date"
            ORDER BY
            "Date" DESC
            "#,
        )
        .persistent(true)
        .fetch_all(&self.pool)
        .await?;
        Ok(data)
    }

    pub async fn get_position_value(
        &self,
        address: String,
    ) -> Result<Vec<LS_Amount>, Error> {
        let data = sqlx::query_as(
            r#"
           SELECT
            s."LS_timestamp" AS "time",
            SUM(
                CASE
                WHEN o."LS_asset_symbol" IN ('WBTC', 'CRO', 'ALL_BTC') THEN s."LS_amnt_stable" / 100000000
                WHEN o."LS_asset_symbol" IN ('ALL_SOL') THEN s."LS_amnt_stable" / 1000000000
                WHEN o."LS_asset_symbol" IN ('PICA') THEN s."LS_amnt_stable" / 1000000000000
                WHEN o."LS_asset_symbol" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS', 'ALL_ETH') THEN s."LS_amnt_stable" / 1000000000000000000
                ELSE s."LS_amnt_stable" / 1000000
                END
            ) AS "amount"
            FROM "LS_State" s
            INNER JOIN "LS_Opening" o ON o."LS_contract_id" = s."LS_contract_id"
            WHERE o."LS_address_id" = $1
            AND s."LS_timestamp" >= NOW() - INTERVAL '20 days'
            GROUP BY s."LS_timestamp"
            ORDER BY s."LS_timestamp"
            "#,
        )
        .bind(address)
        .persistent(true)
        .fetch_all(&self.pool)
        .await?;
        Ok(data)
    }

    pub async fn get_debt_value(
        &self,
        address: String,
    ) -> Result<Vec<LS_Amount>, Error> {
        let data = sqlx::query_as(
            r#"
            SELECT
            s."LS_timestamp" AS "time",
            SUM(
                (
                s."LS_principal_stable" +
                s."LS_prev_margin_stable" +
                s."LS_current_margin_stable" +
                s."LS_prev_interest_stable" +
                s."LS_current_interest_stable"
                )
                /
                CASE
                -- Handle short positions by loan pool ID
                WHEN o."LS_asset_symbol" = 'USDC_NOBLE' AND o."LS_loan_pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN 100000000  -- BTC
                WHEN o."LS_asset_symbol" = 'USDC_NOBLE' AND o."LS_loan_pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN 1000000000 -- SOL

                -- Default to 1e6 (e.g., for USDC, ATOM, OSMO, etc.)
                ELSE 1000000
                END
            ) AS "amount"
            FROM "LS_State" s
            INNER JOIN "LS_Opening" o ON o."LS_contract_id" = s."LS_contract_id"
            WHERE o."LS_address_id" = $1
            AND s."LS_timestamp" >= NOW() - INTERVAL '20 days'
            GROUP BY s."LS_timestamp"
            ORDER BY s."LS_timestamp"
            "#,
        )
        .bind(address)
        .persistent(true)
        .fetch_all(&self.pool)
        .await?;
        Ok(data)
    }

    pub async fn get_realized_pnl_data(
        &self,
        address: String,
    ) -> Result<Vec<LS_Realized_Pnl_Data>, Error> {
        let data = sqlx::query_as(
            r#"
                WITH
                -- Map loan pools -> shorted asset
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

                -- Openings for the wallet + derived position type and shorted asset (if any)
                openings AS (
                SELECT
                    o."LS_contract_id",
                    o."LS_timestamp",
                    o."LS_asset_symbol",
                    o."LS_loan_amnt",
                    o."LS_cltr_symbol",
                    o."LS_cltr_amnt_stable",
                    o."LS_loan_pool_id",
                    o."Tx_Hash" AS open_tx_hash,
                    CASE WHEN o."LS_asset_symbol" IN ('USDC','USDC_NOBLE') THEN 'Short' ELSE 'Long' END AS pos_type,
                    pm.symbol AS short_symbol
                FROM "LS_Opening" o
                LEFT JOIN pool_map pm
                    ON pm.id = o."LS_loan_pool_id"
                WHERE o."LS_address_id" = $1
                ),

                -- Sum of repayments per contract (USDC units)
                repayments AS (
                SELECT
                    r."LS_contract_id",
                    SUM(r."LS_payment_amnt_stable") / 1000000.0 AS total_repaid_stable
                FROM "LS_Repayment" r
                GROUP BY r."LS_contract_id"
                ),

                -- Sum of user collects per contract (normalize by LS_symbol)
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
                    ) AS total_collect_normalized
                FROM "LS_Loan_Collect" lc
                GROUP BY lc."LS_contract_id"
                ),

                -- Fully liquidated contracts
                liqs AS (
                SELECT li."LS_contract_id"
                FROM "LS_Liquidation" li
                WHERE li."LS_loan_close" = TRUE
                ),

                -- Close timestamps (one per contract)
                closing_ts AS (
                SELECT c."LS_contract_id", c."LS_timestamp" AS close_ts
                FROM "LS_Loan_Closing" c
                ),

                -- Closing TxHash candidates
                repayment_close_tx AS (
                SELECT r."LS_contract_id", MAX(r."Tx_Hash") AS tx_hash
                FROM "LS_Repayment" r
                WHERE r."LS_loan_close" = TRUE
                GROUP BY r."LS_contract_id"
                ),
                closepos_tx AS (
                SELECT cp."LS_contract_id", MAX(cp."Tx_Hash") AS tx_hash
                FROM "LS_Close_Position" cp
                WHERE cp."LS_loan_close" = TRUE
                GROUP BY cp."LS_contract_id"
                ),
                liquidation_tx AS (
                SELECT li."LS_contract_id", MAX(li."Tx_Hash") AS tx_hash
                FROM "LS_Liquidation" li
                WHERE li."LS_loan_close" = TRUE
                GROUP BY li."LS_contract_id"
                ),

                -- Only positions that are user-closed (has collects) OR fully liquidated
                closable_positions AS (
                SELECT o.*
                FROM openings o
                WHERE EXISTS (SELECT 1 FROM collects c WHERE c."LS_contract_id" = o."LS_contract_id")
                    OR EXISTS (SELECT 1 FROM liqs     l WHERE l."LS_contract_id" = o."LS_contract_id")
                ),

                -- Opening row
                opening_rows AS (
                SELECT
                    o."LS_timestamp" AS "Date",
                    o."LS_contract_id" AS "Position ID",
                    (
                    CASE
                        WHEN o."LS_cltr_symbol" IN ('ALL_BTC','WBTC','CRO') THEN o."LS_cltr_amnt_stable" / 100000000.0
                        WHEN o."LS_cltr_symbol" = 'ALL_SOL'                 THEN o."LS_cltr_amnt_stable" / 1000000000.0
                        WHEN o."LS_cltr_symbol" = 'PICA'                    THEN o."LS_cltr_amnt_stable" / 1000000000000.0
                        WHEN o."LS_cltr_symbol" IN ('WETH','EVMOS','INJ','DYDX','DYM','CUDOS','ALL_ETH')
                                                                        THEN o."LS_cltr_amnt_stable" / 1000000000000000000.0
                        ELSE o."LS_cltr_amnt_stable" / 1000000.0
                    END
                    + COALESCE(r.total_repaid_stable, 0.0)
                    ) AS "Sent Amount",
                    'USDC' AS "Sent Currency",
                    (CASE
                    WHEN o."LS_asset_symbol" IN ('ALL_BTC','WBTC','CRO') THEN o."LS_loan_amnt" / 100000000.0
                    WHEN o."LS_asset_symbol" = 'ALL_SOL'                 THEN o."LS_loan_amnt" / 1000000000.0
                    WHEN o."LS_asset_symbol" = 'PICA'                    THEN o."LS_loan_amnt" / 1000000000000.0
                    WHEN o."LS_asset_symbol" IN ('WETH','EVMOS','INJ','DYDX','DYM','CUDOS','ALL_ETH')
                                                                        THEN o."LS_loan_amnt" / 1000000000000000000.0
                    ELSE o."LS_loan_amnt" / 1000000.0
                    END) AS "Received Amount",
                    CASE WHEN o."LS_asset_symbol" IN ('USDC','USDC_NOBLE') THEN 'USDC' ELSE o."LS_asset_symbol" END AS "Received Currency",
                    0.0 AS "Fee Amount",
                    'USDC' AS "Fee Currency",
                    'margin trading' AS "Label",
                    CASE
                    WHEN o.pos_type = 'Short' THEN CONCAT(COALESCE(o.short_symbol,'Unknown'),' short opening')
                    ELSE CONCAT(o."LS_asset_symbol",' long opening')
                    END AS "Description",
                    o.open_tx_hash AS "TxHash"
                FROM closable_positions o
                LEFT JOIN repayments r ON r."LS_contract_id" = o."LS_contract_id"
                ),

                -- Closing row
                closing_rows AS (
                SELECT
                    cts.close_ts AS "Date",
                    o."LS_contract_id" AS "Position ID",
                    (CASE
                    WHEN o."LS_asset_symbol" IN ('ALL_BTC','WBTC','CRO') THEN o."LS_loan_amnt" / 100000000.0
                    WHEN o."LS_asset_symbol" = 'ALL_SOL'                 THEN o."LS_loan_amnt" / 1000000000.0
                    WHEN o."LS_asset_symbol" = 'PICA'                    THEN o."LS_loan_amnt" / 1000000000000.0
                    WHEN o."LS_asset_symbol" IN ('WETH','EVMOS','INJ','DYDX','DYM','CUDOS','ALL_ETH')
                                                                        THEN o."LS_loan_amnt" / 1000000000000000000.0
                    ELSE o."LS_loan_amnt" / 1000000.0
                    END) AS "Sent Amount",
                    CASE WHEN o."LS_asset_symbol" IN ('USDC','USDC_NOBLE') THEN 'USDC' ELSE o."LS_asset_symbol" END AS "Sent Currency",
                    COALESCE(c.total_collect_normalized,0.0) AS "Received Amount",
                    'USDC' AS "Received Currency",
                    0.0 AS "Fee Amount",
                    'USDC' AS "Fee Currency",
                    'margin trading' AS "Label",
                    CASE
                    WHEN COALESCE(c.total_collect_normalized,0.0) > 0
                        THEN CASE WHEN o.pos_type='Short'
                                THEN CONCAT(COALESCE(o.short_symbol,'Unknown'),' short closing')
                                ELSE CONCAT(o."LS_asset_symbol",' long closing')
                            END
                    ELSE CASE WHEN o.pos_type='Short'
                                THEN CONCAT(COALESCE(o.short_symbol,'Unknown'),' short liquidation')
                                ELSE CONCAT(o."LS_asset_symbol",' long liquidation')
                        END
                    END AS "Description",
                    COALESCE(rct.tx_hash,cpt.tx_hash,lqt.tx_hash) AS "TxHash"
                FROM closable_positions o
                INNER JOIN closing_ts cts ON cts."LS_contract_id"=o."LS_contract_id"
                LEFT JOIN collects c ON c."LS_contract_id"=o."LS_contract_id"
                LEFT JOIN repayment_close_tx rct ON rct."LS_contract_id"=o."LS_contract_id"
                LEFT JOIN closepos_tx cpt ON cpt."LS_contract_id"=o."LS_contract_id"
                LEFT JOIN liquidation_tx lqt ON lqt."LS_contract_id"=o."LS_contract_id"
                )

                -- Final output
                SELECT
                "Date",
                "Position ID",
                "Sent Amount",
                "Sent Currency",
                "Received Amount",
                "Received Currency",
                "Fee Amount",
                "Fee Currency",
                "Label",
                "Description",
                "TxHash"
                FROM (
                SELECT * FROM opening_rows
                UNION ALL
                SELECT * FROM closing_rows
                ) x
                ORDER BY "Date","Position ID","Sent Currency","Received Currency";
            "#,
        )
        .bind(address)
        .persistent(true)
        .fetch_all(&self.pool)
        .await?;
        Ok(data)
    }

    pub async fn get_addresses(
        &self,
        address: String,
    ) -> Result<Vec<(String,)>, Error> {
        let data = sqlx::query_as(
            r#"
            SELECT "LS_contract_id" FROM "LS_Opening" WHERE "LS_address_id" = $1
            "#,
        )
        .bind(address)
        .persistent(true)
        .fetch_all(&self.pool)
        .await?;
        Ok(data)
    }

    pub async fn get_monthly_active_wallets(
        &self,
    ) -> Result<Vec<MonthlyActiveWallets>, Error> {
        let data = sqlx::query_as(
            r#"
            WITH Market_Close_With_Owners AS (
                SELECT
                    cp."LS_timestamp",
                    cp."LS_contract_id",
                    lo."LS_address_id"
                FROM
                    "LS_Close_Position" cp
                INNER JOIN
                    "LS_Opening" lo
                ON
                    cp."LS_contract_id" = lo."LS_contract_id"
            ),
            Repayment_With_Owners AS (
                SELECT
                    lr."LS_timestamp",
                    lr."LS_contract_id",
                    lo."LS_address_id"
                FROM
                    "LS_Repayment" lr
                INNER JOIN
                    "LS_Opening" lo
                ON
                    lr."LS_contract_id" = lo."LS_contract_id"
            )
            SELECT
                TO_CHAR(combined_timestamp, 'YYYY-MM') AS month,
                COUNT(DISTINCT address) AS unique_addresses
            FROM (
                SELECT "LS_timestamp" AS combined_timestamp, "LS_address_id" AS address FROM "LS_Opening"
                UNION ALL
                SELECT "LP_timestamp" AS combined_timestamp, "LP_address_id" AS address FROM "LP_Deposit"
                UNION ALL
                SELECT "LP_timestamp" AS combined_timestamp, "LP_address_id" AS address FROM "LP_Withdraw"
                UNION ALL
                SELECT "LS_timestamp" AS combined_timestamp, "LS_address_id" AS address FROM Market_Close_With_Owners
                UNION ALL
                SELECT "LS_timestamp" AS combined_timestamp, "LS_address_id" AS address FROM Repayment_With_Owners
            ) AS combined_data
            GROUP BY month
            ORDER BY month ASC
            "#,
        )
        .persistent(true)
        .fetch_all(&self.pool)
        .await?;

        Ok(data)
    }

    pub async fn get_daily_opened_closed(
        &self,
    ) -> Result<Vec<(DateTime<Utc>, BigDecimal, BigDecimal)>, Error> {
        let data = sqlx::query_as(
            r#"
            WITH DateSeries AS (
                SELECT generate_series(
                    DATE(MIN(earliest_date)),
                    DATE(MAX(latest_date)),
                    '1 day'::interval
                ) AS "Date"
                FROM (
                    SELECT MIN("LS_timestamp") AS earliest_date, MAX("LS_timestamp") AS latest_date FROM "LS_Close_Position"
                    UNION ALL
                    SELECT MIN("LS_timestamp") AS earliest_date, MAX("LS_timestamp") AS latest_date FROM "LS_Repayment"
                    UNION ALL
                    SELECT MIN("LS_timestamp") AS earliest_date, MAX("LS_timestamp") AS latest_date FROM "LS_Opening"
                    UNION ALL
                    SELECT MIN("LS_timestamp") AS earliest_date, MAX("LS_timestamp") AS latest_date FROM "LS_Liquidation"
                ) AS combined_dates
            ),
            Close_Loans AS (
                SELECT
                    "LS_contract_id",
                    "LS_timestamp",
                    "LS_principal_stable"
                FROM "LS_Close_Position"
                UNION ALL
                SELECT
                    "LS_contract_id",
                    "LS_timestamp",
                    "LS_principal_stable"
                FROM "LS_Repayment"
                UNION ALL
                SELECT
                    "LS_contract_id",
                    "LS_timestamp",
                    "LS_principal_stable"
                FROM "LS_Liquidation"
            ),
            DailyClosedLoans AS (
                SELECT
                    ds."Date" AS "LocalDate",
                    COALESCE(SUM(cl."LS_principal_stable" / 1000000.0), 0) AS "ClosedLoans"
                FROM
                    DateSeries ds
                LEFT JOIN
                    Close_Loans cl
                    ON DATE(cl."LS_timestamp") = ds."Date"
                GROUP BY
                    ds."Date"
            ),
            DailyOpenedLoans AS (
                SELECT
                    ds."Date" AS "LocalDate",
                    COALESCE(SUM(CASE
                        WHEN lo."LS_loan_pool_id" = 'nolus1jufcaqm6657xmfltdezzz85quz92rmtd88jk5x0hq9zqseem32ysjdm990' THEN lo."LS_loan_amnt_stable" / 1000000.0
                        WHEN lo."LS_loan_pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN lo."LS_loan_amnt_stable" / 100000000.0
                        WHEN lo."LS_loan_pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN lo."LS_loan_amnt_stable" / 1000000000.0
                        WHEN lo."LS_loan_pool_id" = 'nolus1lxr7f5xe02jq6cce4puk6540mtu9sg36at2dms5sk69wdtzdrg9qq0t67z' THEN lo."LS_loan_amnt_stable" / 1000000.0
                        WHEN lo."LS_loan_pool_id" = 'nolus1u0zt8x3mkver0447glfupz9lz6wnt62j70p5fhhtu3fr46gcdd9s5dz9l6' THEN lo."LS_loan_amnt_stable" / 1000000.0
                        ELSE lo."LS_loan_amnt_asset" / 1000000.0
                    END), 0) AS "OpenedLoans"
                FROM
                    DateSeries ds
                LEFT JOIN
                    "LS_Opening" lo ON DATE(lo."LS_timestamp") = ds."Date"
                GROUP BY
                    ds."Date"
            )
            SELECT
                COALESCE(closed."LocalDate", opened."LocalDate") AS "Date",
                COALESCE(closed."ClosedLoans", 0) AS "ClosedLoans",
                COALESCE(opened."OpenedLoans", 0) AS "OpenedLoans"
            FROM
                DailyClosedLoans closed
            FULL OUTER JOIN
                DailyOpenedLoans opened ON closed."LocalDate" = opened."LocalDate"
            ORDER BY
                "Date" ASC
            "#,
        )
        .persistent(true)
        .fetch_all(&self.pool)
        .await?;
        Ok(data)
    }

    pub async fn get_loans_granted(
        &self,
    ) -> Result<Vec<LoanGranted>, crate::error::Error> {
        let data = sqlx::query_as(
            r#"
            SELECT
                CASE
                    WHEN "LS_loan_pool_id" = 'nolus1jufcaqm6657xmfltdezzz85quz92rmtd88jk5x0hq9zqseem32ysjdm990' THEN 'ST_ATOM (Short)'
                    WHEN "LS_loan_pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN 'ALL_BTC (Short)'
                    WHEN "LS_loan_pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN 'ALL_SOL (Short)'
                    WHEN "LS_loan_pool_id" = 'nolus1lxr7f5xe02jq6cce4puk6540mtu9sg36at2dms5sk69wdtzdrg9qq0t67z' THEN 'AKT (Short)'
                    WHEN "LS_loan_pool_id" = 'nolus1u0zt8x3mkver0447glfupz9lz6wnt62j70p5fhhtu3fr46gcdd9s5dz9l6' THEN 'ATOM (Short)'
                    ELSE "LS_asset_symbol"
                END AS asset,
                SUM(
                    CASE 
                        WHEN "LS_loan_pool_id" = 'nolus1jufcaqm6657xmfltdezzz85quz92rmtd88jk5x0hq9zqseem32ysjdm990' THEN "LS_loan_amnt_stable" / 1000000
                        WHEN "LS_loan_pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN "LS_loan_amnt_stable" / 100000000
                        WHEN "LS_loan_pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN "LS_loan_amnt_stable" / 1000000000
                        WHEN "LS_loan_pool_id" = 'nolus1lxr7f5xe02jq6cce4puk6540mtu9sg36at2dms5sk69wdtzdrg9qq0t67z' THEN "LS_loan_amnt_stable" / 1000000
                        WHEN "LS_loan_pool_id" = 'nolus1u0zt8x3mkver0447glfupz9lz6wnt62j70p5fhhtu3fr46gcdd9s5dz9l6' THEN "LS_loan_amnt_stable" / 1000000
                        ELSE "LS_loan_amnt_asset" / 1000000
                    END
                ) AS loan
            FROM "LS_Opening"
            GROUP BY asset
            ORDER BY loan ASC
            "#,
        )
        .persistent(true)
        .fetch_all(&self.pool)
        .await?;

        Ok(data)
    }

    pub async fn get_historically_opened(
        &self,
        skip: i64,
        limit: i64,
    ) -> Result<Vec<HistoricallyOpened>, crate::error::Error> {
        let data = sqlx::query_as(
            r#"
            WITH Historically_Opened_Base AS (
                SELECT DISTINCT ON (lso."LS_contract_id")
                    lso."LS_contract_id" AS "Contract ID",
                    lso."LS_address_id" AS "User",
                    CASE
                        WHEN "LS_loan_pool_id" = 'nolus1jufcaqm6657xmfltdezzz85quz92rmtd88jk5x0hq9zqseem32ysjdm990' THEN 'ST_ATOM'
                        WHEN "LS_loan_pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN 'ALL_BTC'
                        WHEN "LS_loan_pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN 'ALL_SOL'
                        WHEN "LS_loan_pool_id" = 'nolus1lxr7f5xe02jq6cce4puk6540mtu9sg36at2dms5sk69wdtzdrg9qq0t67z' THEN 'AKT'
                        ELSE "LS_asset_symbol"
                    END AS "Leased Asset",
                    "LS_timestamp" AS "Opening Date",
                    CASE
                        WHEN "LS_loan_pool_id" IN (
                            'nolus1jufcaqm6657xmfltdezzz85quz92rmtd88jk5x0hq9zqseem32ysjdm990',
                            'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3',
                            'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm',
                            'nolus1lxr7f5xe02jq6cce4puk6540mtu9sg36at2dms5sk69wdtzdrg9qq0t67z'
                        ) THEN 'Short' ELSE 'Long'
                    END AS "Type",
                    CASE
                        WHEN "LS_cltr_symbol" IN ('ALL_BTC','WBTC','CRO') THEN "LS_cltr_amnt_stable" / 100000000
                        WHEN "LS_cltr_symbol" IN ('ALL_SOL') THEN "LS_cltr_amnt_stable" / 1000000000
                        WHEN "LS_cltr_symbol" IN ('PICA') THEN "LS_cltr_amnt_stable" / 1000000000000
                        WHEN "LS_cltr_symbol" IN ('WETH','EVMOS','INJ','DYDX','DYM','CUDOS','ALL_ETH') THEN "LS_cltr_amnt_stable" / 1000000000000000000
                        ELSE "LS_cltr_amnt_stable" / 1000000
                    END AS "Down Payment Amount",
                    "LS_cltr_symbol" AS "Down Payment Asset",
                    CASE
                        WHEN "LS_loan_pool_id" = 'nolus1jufcaqm6657xmfltdezzz85quz92rmtd88jk5x0hq9zqseem32ysjdm990' THEN "LS_loan_amnt_stable" / 1000000
                        WHEN "LS_loan_pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN "LS_loan_amnt_stable" / 100000000
                        WHEN "LS_loan_pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN "LS_loan_amnt_stable" / 1000000000
                        WHEN "LS_loan_pool_id" = 'nolus1lxr7f5xe02jq6cce4puk6540mtu9sg36at2dms5sk69wdtzdrg9qq0t67z' THEN "LS_loan_amnt_stable" / 1000000
                        ELSE "LS_loan_amnt_asset" / 1000000
                    END AS "Loan",
                    CASE
                        WHEN "LS_loan_pool_id" IN (
                            'nolus1qg5ega6dykkxc307y25pecuufrjkxkaggkkxh7nad0vhyhtuhw3sqaa3c5',
                            'nolus1ueytzwqyadm6r0z8ajse7g6gzum4w3vv04qazctf8ugqrrej6n4sq027cf',
                            'nolus1qqcr7exupnymvg6m63eqwu8pd4n5x6r5t3pyyxdy7r97rcgajmhqy3gn94',
                            'nolus17vsedux675vc44yu7et9m64ndxsy907v7sfgrk7tw3xnjtqemx3q6t3xw6'
                        ) THEN
                            CASE
                                WHEN "LS_asset_symbol" IN ('ALL_BTC','WBTC','CRO') THEN "LS_lpn_loan_amnt" / 100000000
                                WHEN "LS_asset_symbol" IN ('ALL_SOL') THEN "LS_lpn_loan_amnt" / 1000000000
                                WHEN "LS_asset_symbol" IN ('PICA') THEN "LS_lpn_loan_amnt" / 1000000000000
                                WHEN "LS_asset_symbol" IN ('WETH','EVMOS','INJ','DYDX','DYM','CUDOS','ALL_ETH') THEN "LS_lpn_loan_amnt" / 1000000000000000000
                                ELSE "LS_lpn_loan_amnt" / 1000000
                            END
                        ELSE "LS_lpn_loan_amnt" / 1000000
                    END AS "Total Position Amount (LPN)"
                FROM "LS_Opening" lso
            ),
            Opened_With_Price AS (
                SELECT
                    hob.*,
                    p.price AS "Price",
                    EXISTS (
                        SELECT 1
                        FROM "LS_State" s
                        WHERE s."LS_contract_id" = hob."Contract ID"
                            AND s."LS_timestamp" >= NOW() - interval '1 hour'
                    ) AS "Open"
                FROM Historically_Opened_Base hob
                LEFT JOIN LATERAL (
                    SELECT a."MP_price_in_stable" AS price
                    FROM "MP_Asset" a
                    WHERE a."MP_asset_symbol" = hob."Leased Asset"
                        AND a."MP_asset_timestamp" <= hob."Opening Date"
                    ORDER BY a."MP_asset_timestamp" DESC
                    LIMIT 1
                ) p ON true
            )
            SELECT
                "Contract ID" AS contract_id,
                "User" AS user,
                "Leased Asset" AS leased_asset,
                "Opening Date" AS opening_date,
                "Type" AS position_type,
                "Down Payment Amount" AS down_payment_amount,
                "Down Payment Asset" AS down_payment_asset,
                "Loan" AS loan,
                "Total Position Amount (LPN)" AS total_position_amount_lpn,
                "Price" AS price,
                "Open" AS open,
                CASE
                    WHEN "Type" = 'Long' THEN ("Loan" / 0.9) / ("Down Payment Amount" + "Loan") * "Price"
                    WHEN "Type" = 'Short' THEN ("Down Payment Amount" + "Loan") / ("Total Position Amount (LPN)" / 0.9)
                END AS liquidation_price
            FROM Opened_With_Price
            ORDER BY "Opening Date" DESC
            OFFSET $1 LIMIT $2
            "#,
        )
        .bind(skip)
        .bind(limit)
        .persistent(true)
        .fetch_all(&self.pool)
        .await?;

        Ok(data)
    }

    /// Get historically opened positions with optional time window filter
    /// If months is Some(n), only returns positions opened in the last n months
    /// If months is None, returns all positions
    /// 
    /// OPTIMIZED: Uses pre-computed columns (LS_position_type, LS_opening_price, LS_liquidation_price_at_open)
    /// when available, with fallback to computed values for rows not yet backfilled.
    pub async fn get_historically_opened_with_window(
        &self,
        months: Option<i32>,
        from: Option<DateTime<Utc>>,
    ) -> Result<Vec<HistoricallyOpened>, crate::error::Error> {
        // Build time conditions dynamically
        let mut conditions = Vec::new();

        if let Some(m) = months {
            conditions.push(format!(
                "o.\"LS_timestamp\" >= NOW() - INTERVAL '{} months'",
                m
            ));
        }

        if from.is_some() {
            conditions.push("o.\"LS_timestamp\" > $1".to_string());
        }

        let time_condition = if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };

        // Optimized query using pre-computed columns with fallbacks
        let query = format!(
            r#"
            SELECT
                o."LS_contract_id" AS contract_id,
                o."LS_address_id" AS "user",
                -- Use pool_config label for short positions, otherwise asset symbol
                COALESCE(
                    CASE 
                        WHEN o."LS_position_type" = 'Short' THEN pc."label"
                        ELSE NULL
                    END,
                    CASE
                        WHEN o."LS_loan_pool_id" = 'nolus1jufcaqm6657xmfltdezzz85quz92rmtd88jk5x0hq9zqseem32ysjdm990' THEN 'ST_ATOM'
                        WHEN o."LS_loan_pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN 'ALL_BTC'
                        WHEN o."LS_loan_pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN 'ALL_SOL'
                        WHEN o."LS_loan_pool_id" = 'nolus1lxr7f5xe02jq6cce4puk6540mtu9sg36at2dms5sk69wdtzdrg9qq0t67z' THEN 'AKT'
                        ELSE o."LS_asset_symbol"
                    END
                ) AS leased_asset,
                o."LS_timestamp" AS opening_date,
                -- Use pre-computed position_type or fallback to computed
                COALESCE(
                    o."LS_position_type",
                    CASE
                        WHEN o."LS_loan_pool_id" IN (
                            'nolus1jufcaqm6657xmfltdezzz85quz92rmtd88jk5x0hq9zqseem32ysjdm990',
                            'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3',
                            'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm',
                            'nolus1lxr7f5xe02jq6cce4puk6540mtu9sg36at2dms5sk69wdtzdrg9qq0t67z'
                        ) THEN 'Short' ELSE 'Long'
                    END
                ) AS position_type,
                -- Normalized down payment amount
                CASE
                    WHEN o."LS_cltr_symbol" IN ('ALL_BTC','WBTC','CRO') THEN o."LS_cltr_amnt_stable" / 100000000
                    WHEN o."LS_cltr_symbol" IN ('ALL_SOL') THEN o."LS_cltr_amnt_stable" / 1000000000
                    WHEN o."LS_cltr_symbol" IN ('PICA') THEN o."LS_cltr_amnt_stable" / 1000000000000
                    WHEN o."LS_cltr_symbol" IN ('WETH','EVMOS','INJ','DYDX','DYM','CUDOS','ALL_ETH') THEN o."LS_cltr_amnt_stable" / 1000000000000000000
                    ELSE o."LS_cltr_amnt_stable" / 1000000
                END AS down_payment_amount,
                o."LS_cltr_symbol" AS down_payment_asset,
                -- Normalized loan amount
                CASE
                    WHEN o."LS_loan_pool_id" = 'nolus1jufcaqm6657xmfltdezzz85quz92rmtd88jk5x0hq9zqseem32ysjdm990' THEN o."LS_loan_amnt_stable" / 1000000
                    WHEN o."LS_loan_pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN o."LS_loan_amnt_stable" / 100000000
                    WHEN o."LS_loan_pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN o."LS_loan_amnt_stable" / 1000000000
                    WHEN o."LS_loan_pool_id" = 'nolus1lxr7f5xe02jq6cce4puk6540mtu9sg36at2dms5sk69wdtzdrg9qq0t67z' THEN o."LS_loan_amnt_stable" / 1000000
                    ELSE o."LS_loan_amnt_asset" / 1000000
                END AS loan,
                -- Total position amount in LPN
                COALESCE(o."LS_lpn_loan_amnt" / COALESCE(o."LS_lpn_decimals", 1000000)::numeric, 0) AS total_position_amount_lpn,
                -- Use pre-computed opening_price or fallback to LATERAL join
                COALESCE(
                    o."LS_opening_price",
                    (
                        SELECT m."MP_price_in_stable"
                        FROM "MP_Asset" m
                        WHERE m."MP_asset_symbol" = o."LS_asset_symbol"
                          AND m."MP_asset_timestamp" <= o."LS_timestamp"
                        ORDER BY m."MP_asset_timestamp" DESC
                        LIMIT 1
                    )
                ) AS price,
                -- Check if position is still open
                EXISTS (
                    SELECT 1
                    FROM "LS_State" s
                    WHERE s."LS_contract_id" = o."LS_contract_id"
                      AND s."LS_timestamp" >= NOW() - interval '1 hour'
                ) AS open,
                -- Use pre-computed liquidation_price or fallback to computed
                COALESCE(
                    o."LS_liquidation_price_at_open",
                    CASE
                        WHEN COALESCE(o."LS_position_type", 
                            CASE WHEN o."LS_loan_pool_id" IN (
                                'nolus1jufcaqm6657xmfltdezzz85quz92rmtd88jk5x0hq9zqseem32ysjdm990',
                                'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3',
                                'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm',
                                'nolus1lxr7f5xe02jq6cce4puk6540mtu9sg36at2dms5sk69wdtzdrg9qq0t67z'
                            ) THEN 'Short' ELSE 'Long' END
                        ) = 'Long' THEN
                            (o."LS_loan_amnt_stable" / 1000000.0 / 0.9) / 
                            NULLIF((o."LS_cltr_amnt_stable" + o."LS_loan_amnt_stable") / 1000000.0, 0) *
                            COALESCE(o."LS_opening_price", (
                                SELECT m."MP_price_in_stable"
                                FROM "MP_Asset" m
                                WHERE m."MP_asset_symbol" = o."LS_asset_symbol"
                                  AND m."MP_asset_timestamp" <= o."LS_timestamp"
                                ORDER BY m."MP_asset_timestamp" DESC
                                LIMIT 1
                            ))
                        WHEN COALESCE(o."LS_position_type",
                            CASE WHEN o."LS_loan_pool_id" IN (
                                'nolus1jufcaqm6657xmfltdezzz85quz92rmtd88jk5x0hq9zqseem32ysjdm990',
                                'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3',
                                'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm',
                                'nolus1lxr7f5xe02jq6cce4puk6540mtu9sg36at2dms5sk69wdtzdrg9qq0t67z'
                            ) THEN 'Short' ELSE 'Long' END
                        ) = 'Short' THEN
                            ((o."LS_cltr_amnt_stable" + o."LS_loan_amnt_stable") / 1000000.0) /
                            NULLIF(o."LS_lpn_loan_amnt" / 1000000.0 / 0.9, 0)
                    END
                ) AS liquidation_price
            FROM "LS_Opening" o
            LEFT JOIN "pool_config" pc ON o."LS_loan_pool_id" = pc."pool_id"
            {}
            ORDER BY o."LS_timestamp" DESC
            "#,
            time_condition
        );

        let mut query_builder = sqlx::query_as::<_, HistoricallyOpened>(&query);

        if let Some(from_ts) = from {
            query_builder = query_builder.bind(from_ts);
        }

        let data = query_builder.persistent(true).fetch_all(&self.pool).await?;

        Ok(data)
    }

    /// Get all historically opened positions without pagination - for streaming CSV export
    pub async fn get_all_historically_opened(
        &self,
    ) -> Result<Vec<HistoricallyOpened>, crate::error::Error> {
        let data = sqlx::query_as(
            r#"
            WITH Historically_Opened_Base AS (
                SELECT DISTINCT ON (lso."LS_contract_id")
                    lso."LS_contract_id" AS "Contract ID",
                    lso."LS_address_id" AS "User",
                    CASE
                        WHEN "LS_loan_pool_id" = 'nolus1jufcaqm6657xmfltdezzz85quz92rmtd88jk5x0hq9zqseem32ysjdm990' THEN 'ST_ATOM'
                        WHEN "LS_loan_pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN 'ALL_BTC'
                        WHEN "LS_loan_pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN 'ALL_SOL'
                        WHEN "LS_loan_pool_id" = 'nolus1lxr7f5xe02jq6cce4puk6540mtu9sg36at2dms5sk69wdtzdrg9qq0t67z' THEN 'AKT'
                        ELSE "LS_asset_symbol"
                    END AS "Leased Asset",
                    "LS_timestamp" AS "Opening Date",
                    CASE
                        WHEN "LS_loan_pool_id" IN (
                            'nolus1jufcaqm6657xmfltdezzz85quz92rmtd88jk5x0hq9zqseem32ysjdm990',
                            'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3',
                            'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm',
                            'nolus1lxr7f5xe02jq6cce4puk6540mtu9sg36at2dms5sk69wdtzdrg9qq0t67z'
                        ) THEN 'Short' ELSE 'Long'
                    END AS "Type",
                    CASE
                        WHEN "LS_cltr_symbol" IN ('ALL_BTC','WBTC','CRO') THEN "LS_cltr_amnt_stable" / 100000000
                        WHEN "LS_cltr_symbol" IN ('ALL_SOL') THEN "LS_cltr_amnt_stable" / 1000000000
                        WHEN "LS_cltr_symbol" IN ('PICA') THEN "LS_cltr_amnt_stable" / 1000000000000
                        WHEN "LS_cltr_symbol" IN ('WETH','EVMOS','INJ','DYDX','DYM','CUDOS','ALL_ETH') THEN "LS_cltr_amnt_stable" / 1000000000000000000
                        ELSE "LS_cltr_amnt_stable" / 1000000
                    END AS "Down Payment Amount",
                    "LS_cltr_symbol" AS "Down Payment Asset",
                    CASE
                        WHEN "LS_loan_pool_id" = 'nolus1jufcaqm6657xmfltdezzz85quz92rmtd88jk5x0hq9zqseem32ysjdm990' THEN "LS_loan_amnt_stable" / 1000000
                        WHEN "LS_loan_pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN "LS_loan_amnt_stable" / 100000000
                        WHEN "LS_loan_pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN "LS_loan_amnt_stable" / 1000000000
                        WHEN "LS_loan_pool_id" = 'nolus1lxr7f5xe02jq6cce4puk6540mtu9sg36at2dms5sk69wdtzdrg9qq0t67z' THEN "LS_loan_amnt_stable" / 1000000
                        ELSE "LS_loan_amnt_asset" / 1000000
                    END AS "Loan",
                    CASE
                        WHEN "LS_loan_pool_id" IN (
                            'nolus1qg5ega6dykkxc307y25pecuufrjkxkaggkkxh7nad0vhyhtuhw3sqaa3c5',
                            'nolus1ueytzwqyadm6r0z8ajse7g6gzum4w3vv04qazctf8ugqrrej6n4sq027cf',
                            'nolus1qqcr7exupnymvg6m63eqwu8pd4n5x6r5t3pyyxdy7r97rcgajmhqy3gn94',
                            'nolus17vsedux675vc44yu7et9m64ndxsy907v7sfgrk7tw3xnjtqemx3q6t3xw6'
                        ) THEN
                            CASE
                                WHEN "LS_asset_symbol" IN ('ALL_BTC','WBTC','CRO') THEN "LS_lpn_loan_amnt" / 100000000
                                WHEN "LS_asset_symbol" IN ('ALL_SOL') THEN "LS_lpn_loan_amnt" / 1000000000
                                WHEN "LS_asset_symbol" IN ('PICA') THEN "LS_lpn_loan_amnt" / 1000000000000
                                WHEN "LS_asset_symbol" IN ('WETH','EVMOS','INJ','DYDX','DYM','CUDOS','ALL_ETH') THEN "LS_lpn_loan_amnt" / 1000000000000000000
                                ELSE "LS_lpn_loan_amnt" / 1000000
                            END
                        ELSE "LS_lpn_loan_amnt" / 1000000
                    END AS "Total Position Amount (LPN)"
                FROM "LS_Opening" lso
            ),
            Opened_With_Price AS (
                SELECT
                    hob.*,
                    p.price AS "Price",
                    EXISTS (
                        SELECT 1
                        FROM "LS_State" s
                        WHERE s."LS_contract_id" = hob."Contract ID"
                            AND s."LS_timestamp" >= NOW() - interval '1 hour'
                    ) AS "Open"
                FROM Historically_Opened_Base hob
                LEFT JOIN LATERAL (
                    SELECT a."MP_price_in_stable" AS price
                    FROM "MP_Asset" a
                    WHERE a."MP_asset_symbol" = hob."Leased Asset"
                        AND a."MP_asset_timestamp" <= hob."Opening Date"
                    ORDER BY a."MP_asset_timestamp" DESC
                    LIMIT 1
                ) p ON true
            )
            SELECT
                "Contract ID" AS contract_id,
                "User" AS user,
                "Leased Asset" AS leased_asset,
                "Opening Date" AS opening_date,
                "Type" AS position_type,
                "Down Payment Amount" AS down_payment_amount,
                "Down Payment Asset" AS down_payment_asset,
                "Loan" AS loan,
                "Total Position Amount (LPN)" AS total_position_amount_lpn,
                "Price" AS price,
                "Open" AS open,
                CASE
                    WHEN "Type" = 'Long' THEN ("Loan" / 0.9) / ("Down Payment Amount" + "Loan") * "Price"
                    WHEN "Type" = 'Short' THEN ("Down Payment Amount" + "Loan") / ("Total Position Amount (LPN)" / 0.9)
                END AS liquidation_price
            FROM Opened_With_Price
            ORDER BY "Opening Date" DESC
            "#,
        )
        .persistent(true)
        .fetch_all(&self.pool)
        .await?;

        Ok(data)
    }

    pub async fn get_realized_pnl_by_wallet(
        &self,
        skip: i64,
        limit: i64,
    ) -> Result<Vec<RealizedPnlWallet>, crate::error::Error> {
        let data = sqlx::query_as(
            r#"
            WITH openings_raw AS (
                SELECT
                    o."LS_contract_id" AS "Contract ID",
                    o."LS_address_id" AS "User",
                    o."LS_timestamp" AS "Opening Date",
                    o."LS_asset_symbol" AS "Leased Asset",
                    o."LS_cltr_symbol" AS "Down Payment Asset",
                    o."LS_cltr_amnt_stable" / 1000000 AS "Down Payment (Stable)",
                    CASE o."LS_loan_pool_id"
                        WHEN 'nolus17vsedux675vc44yu7et9m64ndxsy907v7sfgrk7tw3xnjtqemx3q6t3xw6' THEN 'USDC_NOBLE'
                        WHEN 'nolus1qg5ega6dykkxc307y25pecuufrjkxkaggkkxh7nad0vhyhtuhw3sqaa3c5' THEN 'USDC'
                        WHEN 'nolus1qqcr7exupnymvg6m63eqwu8pd4n5x6r5t3pyyxdy7r97rcgajmhqy3gn94' THEN 'USDC_AXELAR'
                        WHEN 'nolus1ueytzwqyadm6r0z8ajse7g6gzum4w3vv04qazctf8ugqrrej6n4sq027cf' THEN 'USDC_NOBLE'
                        WHEN 'nolus1jufcaqm6657xmfltdezzz85quz92rmtd88jk5x0hq9zqseem32ysjdm990' THEN 'ST_ATOM'
                        WHEN 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN 'ALL_BTC'
                        WHEN 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN 'ALL_SOL'
                        WHEN 'nolus1lxr7f5xe02jq6cce4puk6540mtu9sg36at2dms5sk69wdtzdrg9qq0t67z' THEN 'AKT'
                        WHEN 'nolus1u0zt8x3mkver0447glfupz9lz6wnt62j70p5fhhtu3fr46gcdd9s5dz9l6' THEN 'ATOM'
                        WHEN 'nolus1py7pxw74qvlgq0n6rfz7mjrhgnls37mh87wasg89n75qt725rams8yr46t' THEN 'OSMO'
                        ELSE 'USDC_NOBLE'
                    END AS "LPN_Symbol"
                FROM "LS_Opening" o
                WHERE o."LS_timestamp" >= NOW() - INTERVAL '1 year'
            ),
            openings AS (
                SELECT
                    r.*,
                    CASE WHEN r."LPN_Symbol" IN ('USDC_NOBLE','USDC','USDC_AXELAR') THEN TRUE ELSE FALSE END AS "LPN_IsStable"
                FROM openings_raw r
            ),
            loan_close AS (
                SELECT
                    lc."LS_contract_id" AS "Contract ID",
                    MAX(lc."LS_timestamp") AS "Close Timestamp"
                FROM "LS_Loan_Closing" lc
                JOIN openings o ON o."Contract ID" = lc."LS_contract_id"
                GROUP BY lc."LS_contract_id"
            ),
            close_rows_lpn AS (
                SELECT
                    c."LS_contract_id" AS "Contract ID",
                    c."LS_timestamp" AS "Close Timestamp",
                    c."LS_change" AS "LS_change_raw",
                    o."LPN_Symbol",
                    o."LPN_IsStable"
                FROM "LS_Close_Position" c
                JOIN openings o ON o."Contract ID" = c."LS_contract_id"
            ),
            norm_lpn AS (
                SELECT
                    cr."Contract ID",
                    cr."Close Timestamp",
                    cr."LPN_Symbol",
                    cr."LPN_IsStable",
                    CASE
                        WHEN cr."LPN_Symbol" IN ('ALL_BTC','WBTC','CRO') THEN cr."LS_change_raw" / 100000000
                        WHEN cr."LPN_Symbol" IN ('ALL_SOL') THEN cr."LS_change_raw" / 1000000000
                        WHEN cr."LPN_Symbol" IN ('PICA') THEN cr."LS_change_raw" / 1000000000000
                        WHEN cr."LPN_Symbol" IN ('WETH','EVMOS','INJ','DYDX','DYM','CUDOS','ALL_ETH') THEN cr."LS_change_raw" / 1000000000000000000
                        ELSE cr."LS_change_raw" / 1000000
                    END AS "Units"
                FROM close_rows_lpn cr
            ),
            priced_lpn AS (
                SELECT
                    nl."Contract ID",
                    nl."Close Timestamp",
                    nl."LPN_Symbol",
                    nl."Units" AS "Returned Amount (LPN Units)",
                    CASE
                        WHEN nl."LPN_Symbol" IN ('USDC_NOBLE','USDC','USDC_AXELAR') OR nl."LPN_IsStable"
                            THEN nl."Units"
                        ELSE nl."Units" * p."MP_price_in_stable"
                    END AS "Returned Amount (Stable)"
                FROM norm_lpn nl
                LEFT JOIN LATERAL (
                    SELECT "MP_price_in_stable"
                    FROM "MP_Asset" m
                    WHERE NOT nl."LPN_IsStable"
                        AND m."MP_asset_symbol" = nl."LPN_Symbol"
                        AND m."MP_asset_timestamp" BETWEEN nl."Close Timestamp" - INTERVAL '30 minutes'
                                                    AND nl."Close Timestamp" + INTERVAL '30 minutes'
                    ORDER BY ABS(EXTRACT(EPOCH FROM (m."MP_asset_timestamp" - nl."Close Timestamp"))) ASC
                    LIMIT 1
                ) p ON TRUE
            ),
            agg_lpn AS (
                SELECT
                    "Contract ID",
                    MAX("Close Timestamp") AS "Close Timestamp (LPN)",
                    MIN("LPN_Symbol") AS "Returned LPN",
                    SUM("Returned Amount (LPN Units)") AS "Returned Amount (LPN Units)",
                    SUM("Returned Amount (Stable)") AS "Returned Amount (Stable)"
                FROM priced_lpn
                GROUP BY "Contract ID"
            ),
            leased_rows AS (
                SELECT
                    lc."LS_contract_id" AS "Contract ID",
                    lc."LS_timestamp" AS "Close Timestamp",
                    lc."LS_amnt" AS "leased_raw",
                    o."Leased Asset"
                FROM "LS_Loan_Closing" lc
                JOIN openings o ON o."Contract ID" = lc."LS_contract_id"
            ),
            norm_leased AS (
                SELECT
                    lr."Contract ID",
                    lr."Close Timestamp",
                    lr."Leased Asset",
                    CASE
                        WHEN lr."Leased Asset" IN ('ALL_BTC','WBTC','CRO') THEN lr."leased_raw" / 100000000
                        WHEN lr."Leased Asset" IN ('ALL_SOL') THEN lr."leased_raw" / 1000000000
                        WHEN lr."Leased Asset" IN ('PICA') THEN lr."leased_raw" / 1000000000000
                        WHEN lr."Leased Asset" IN ('WETH','EVMOS','INJ','DYDX','DYM','CUDOS','ALL_ETH') THEN lr."leased_raw" / 1000000000000000000
                        ELSE lr."leased_raw" / 1000000
                    END AS "Units"
                FROM leased_rows lr
            ),
            priced_leased AS (
                SELECT
                    nl."Contract ID",
                    nl."Close Timestamp",
                    nl."Leased Asset",
                    nl."Units",
                    CASE
                        WHEN nl."Leased Asset" IN ('USDC_NOBLE','USDC','USDC_AXELAR')
                            THEN nl."Units"
                        ELSE nl."Units" * p."MP_price_in_stable"
                    END AS "Returned Amount (Stable)"
                FROM norm_leased nl
                LEFT JOIN LATERAL (
                    SELECT "MP_price_in_stable"
                    FROM "MP_Asset" m
                    WHERE m."MP_asset_symbol" = nl."Leased Asset"
                        AND m."MP_asset_timestamp" BETWEEN nl."Close Timestamp" - INTERVAL '30 minutes'
                                                    AND nl."Close Timestamp" + INTERVAL '30 minutes'
                    ORDER BY ABS(EXTRACT(EPOCH FROM (m."MP_asset_timestamp" - nl."Close Timestamp"))) ASC
                    LIMIT 1
                ) p ON TRUE
            ),
            agg_leased AS (
                SELECT
                    "Contract ID",
                    MAX("Close Timestamp") AS "Close Timestamp (Leased)",
                    MIN("Leased Asset") AS "Returned LPN",
                    SUM("Units") AS "Returned Amount (LPN Units)",
                    SUM("Returned Amount (Stable)") AS "Returned Amount (Stable)"
                FROM priced_leased
                GROUP BY "Contract ID"
            ),
            inflow AS (
                SELECT
                    lc."Contract ID",
                    lc."Close Timestamp",
                    COALESCE(al."Returned LPN", agl."Returned LPN") AS "Returned LPN",
                    COALESCE(al."Returned Amount (LPN Units)", agl."Returned Amount (LPN Units)") AS "Returned Amount (LPN Units)",
                    COALESCE(al."Returned Amount (Stable)", agl."Returned Amount (Stable)") AS "Returned Amount (Stable)"
                FROM loan_close lc
                LEFT JOIN agg_lpn al ON al."Contract ID" = lc."Contract ID"
                LEFT JOIN agg_leased agl ON agl."Contract ID" = lc."Contract ID"
            ),
            repays AS (
                SELECT 
                    r."LS_contract_id" AS "Contract ID",
                    SUM(r."LS_payment_amnt_stable") / 1000000 AS "Manual Repayments (Stable)"
                FROM "LS_Repayment" r 
                JOIN openings o ON o."Contract ID" = r."LS_contract_id"
                GROUP BY r."LS_contract_id"
            ),
            liqs AS (
                SELECT 
                    l."LS_contract_id" AS "Contract ID",
                    SUM(l."LS_payment_amnt_stable") / 1000000 AS "Liquidations (Stable)",
                    COUNT(*) AS "Liquidation Events"
                FROM "LS_Liquidation" l 
                JOIN openings o ON o."Contract ID" = l."LS_contract_id"
                GROUP BY l."LS_contract_id"
            )
            SELECT
                o."Contract ID" AS contract_id,
                o."User" AS user,
                o."Leased Asset" AS leased_asset,
                o."Down Payment Asset" AS down_payment_asset,
                o."Opening Date" AS opening_date,
                i."Close Timestamp" AS close_timestamp,
                o."Down Payment (Stable)" AS down_payment_stable,
                COALESCE(r."Manual Repayments (Stable)", 0) AS manual_repayments_stable,
                (o."Down Payment (Stable)" + COALESCE(r."Manual Repayments (Stable)", 0)) AS total_outflow_stable,
                COALESCE(l."Liquidations (Stable)", 0) AS liquidations_stable,
                COALESCE(l."Liquidation Events", 0) AS liquidation_events,
                i."Returned LPN" AS returned_lpn,
                i."Returned Amount (LPN Units)" AS returned_amount_lpn_units,
                i."Returned Amount (Stable)" AS returned_amount_stable,
                CASE 
                    WHEN i."Returned Amount (Stable)" IS NULL THEN NULL
                    ELSE i."Returned Amount (Stable)" - (o."Down Payment (Stable)" + COALESCE(r."Manual Repayments (Stable)", 0))
                END AS realized_pnl_stable
            FROM openings o
            JOIN inflow i ON i."Contract ID" = o."Contract ID"
            LEFT JOIN repays r ON r."Contract ID" = o."Contract ID"
            LEFT JOIN liqs l ON l."Contract ID" = o."Contract ID"
            ORDER BY i."Close Timestamp" DESC
            OFFSET $1 LIMIT $2
            "#,
        )
        .bind(skip)
        .bind(limit)
        .persistent(true)
        .fetch_all(&self.pool)
        .await?;

        Ok(data)
    }

    /// Get realized PnL by wallet with time window filtering
    pub async fn get_realized_pnl_by_wallet_with_window(
        &self,
        months: Option<i32>,
        from: Option<DateTime<Utc>>,
    ) -> Result<Vec<RealizedPnlWallet>, crate::error::Error> {
        // Build time conditions dynamically
        let mut conditions = Vec::new();

        if let Some(m) = months {
            conditions.push(format!(
                "o.\"LS_timestamp\" >= NOW() - INTERVAL '{} months'",
                m
            ));
        }

        if from.is_some() {
            conditions.push("o.\"LS_timestamp\" > $1".to_string());
        }

        let time_condition = if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };

        let query = format!(
            r#"
            WITH openings_raw AS (
                SELECT
                    o."LS_contract_id" AS "Contract ID",
                    o."LS_address_id" AS "User",
                    o."LS_timestamp" AS "Opening Date",
                    o."LS_asset_symbol" AS "Leased Asset",
                    o."LS_cltr_symbol" AS "Down Payment Asset",
                    o."LS_cltr_amnt_stable" / 1000000 AS "Down Payment (Stable)",
                    CASE o."LS_loan_pool_id"
                        WHEN 'nolus17vsedux675vc44yu7et9m64ndxsy907v7sfgrk7tw3xnjtqemx3q6t3xw6' THEN 'USDC_NOBLE'
                        WHEN 'nolus1qg5ega6dykkxc307y25pecuufrjkxkaggkkxh7nad0vhyhtuhw3sqaa3c5' THEN 'USDC'
                        WHEN 'nolus1qqcr7exupnymvg6m63eqwu8pd4n5x6r5t3pyyxdy7r97rcgajmhqy3gn94' THEN 'USDC_AXELAR'
                        WHEN 'nolus1ueytzwqyadm6r0z8ajse7g6gzum4w3vv04qazctf8ugqrrej6n4sq027cf' THEN 'USDC_NOBLE'
                        WHEN 'nolus1jufcaqm6657xmfltdezzz85quz92rmtd88jk5x0hq9zqseem32ysjdm990' THEN 'ST_ATOM'
                        WHEN 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN 'ALL_BTC'
                        WHEN 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN 'ALL_SOL'
                        WHEN 'nolus1lxr7f5xe02jq6cce4puk6540mtu9sg36at2dms5sk69wdtzdrg9qq0t67z' THEN 'AKT'
                        WHEN 'nolus1u0zt8x3mkver0447glfupz9lz6wnt62j70p5fhhtu3fr46gcdd9s5dz9l6' THEN 'ATOM'
                        WHEN 'nolus1py7pxw74qvlgq0n6rfz7mjrhgnls37mh87wasg89n75qt725rams8yr46t' THEN 'OSMO'
                        ELSE 'USDC_NOBLE'
                    END AS "LPN_Symbol"
                FROM "LS_Opening" o
                {}
            ),
                    openings AS (
                        SELECT
                            r.*,
                            CASE WHEN r."LPN_Symbol" IN ('USDC_NOBLE','USDC','USDC_AXELAR') THEN TRUE ELSE FALSE END AS "LPN_IsStable"
                        FROM openings_raw r
                    ),
                    loan_close AS (
                        SELECT
                            lc."LS_contract_id" AS "Contract ID",
                            MAX(lc."LS_timestamp") AS "Close Timestamp"
                        FROM "LS_Loan_Closing" lc
                        JOIN openings o ON o."Contract ID" = lc."LS_contract_id"
                        GROUP BY lc."LS_contract_id"
                    ),
                    close_rows_lpn AS (
                        SELECT
                            c."LS_contract_id" AS "Contract ID",
                            c."LS_timestamp" AS "Close Timestamp",
                            c."LS_change" AS "LS_change_raw",
                            o."LPN_Symbol",
                            o."LPN_IsStable"
                        FROM "LS_Close_Position" c
                        JOIN openings o ON o."Contract ID" = c."LS_contract_id"
                    ),
                    norm_lpn AS (
                        SELECT
                            cr."Contract ID",
                            cr."Close Timestamp",
                            cr."LPN_Symbol",
                            cr."LPN_IsStable",
                            CASE
                                WHEN cr."LPN_Symbol" IN ('ALL_BTC','WBTC','CRO') THEN cr."LS_change_raw" / 100000000
                                WHEN cr."LPN_Symbol" IN ('ALL_SOL') THEN cr."LS_change_raw" / 1000000000
                                WHEN cr."LPN_Symbol" IN ('PICA') THEN cr."LS_change_raw" / 1000000000000
                                WHEN cr."LPN_Symbol" IN ('WETH','EVMOS','INJ','DYDX','DYM','CUDOS','ALL_ETH') THEN cr."LS_change_raw" / 1000000000000000000
                                ELSE cr."LS_change_raw" / 1000000
                            END AS "Units"
                        FROM close_rows_lpn cr
                    ),
                    priced_lpn AS (
                        SELECT
                            nl."Contract ID",
                            nl."Close Timestamp",
                            nl."LPN_Symbol",
                            nl."Units" AS "Returned Amount (LPN Units)",
                            CASE
                                WHEN nl."LPN_Symbol" IN ('USDC_NOBLE','USDC','USDC_AXELAR') OR nl."LPN_IsStable"
                                    THEN nl."Units"
                                ELSE nl."Units" * p."MP_price_in_stable"
                            END AS "Returned Amount (Stable)"
                        FROM norm_lpn nl
                        LEFT JOIN LATERAL (
                            SELECT "MP_price_in_stable"
                            FROM "MP_Asset" m
                            WHERE NOT nl."LPN_IsStable"
                                AND m."MP_asset_symbol" = nl."LPN_Symbol"
                                AND m."MP_asset_timestamp" BETWEEN nl."Close Timestamp" - INTERVAL '30 minutes'
                                                            AND nl."Close Timestamp" + INTERVAL '30 minutes'
                            ORDER BY ABS(EXTRACT(EPOCH FROM (m."MP_asset_timestamp" - nl."Close Timestamp"))) ASC
                            LIMIT 1
                        ) p ON TRUE
                    ),
                    agg_lpn AS (
                        SELECT
                            "Contract ID",
                            MAX("Close Timestamp") AS "Close Timestamp (LPN)",
                            MIN("LPN_Symbol") AS "Returned LPN",
                            SUM("Returned Amount (LPN Units)") AS "Returned Amount (LPN Units)",
                            SUM("Returned Amount (Stable)") AS "Returned Amount (Stable)"
                        FROM priced_lpn
                        GROUP BY "Contract ID"
                    ),
                    leased_rows AS (
                        SELECT
                            lc."LS_contract_id" AS "Contract ID",
                            lc."LS_timestamp" AS "Close Timestamp",
                            lc."LS_amnt" AS "leased_raw",
                            o."Leased Asset"
                        FROM "LS_Loan_Closing" lc
                        JOIN openings o ON o."Contract ID" = lc."LS_contract_id"
                    ),
                    norm_leased AS (
                        SELECT
                            lr."Contract ID",
                            lr."Close Timestamp",
                            lr."Leased Asset",
                            CASE
                                WHEN lr."Leased Asset" IN ('ALL_BTC','WBTC','CRO') THEN lr."leased_raw" / 100000000
                                WHEN lr."Leased Asset" IN ('ALL_SOL') THEN lr."leased_raw" / 1000000000
                                WHEN lr."Leased Asset" IN ('PICA') THEN lr."leased_raw" / 1000000000000
                                WHEN lr."Leased Asset" IN ('WETH','EVMOS','INJ','DYDX','DYM','CUDOS','ALL_ETH') THEN lr."leased_raw" / 1000000000000000000
                                ELSE lr."leased_raw" / 1000000
                            END AS "Units"
                        FROM leased_rows lr
                    ),
                    priced_leased AS (
                        SELECT
                            nl."Contract ID",
                            nl."Close Timestamp",
                            nl."Leased Asset",
                            nl."Units",
                            CASE
                                WHEN nl."Leased Asset" IN ('USDC_NOBLE','USDC','USDC_AXELAR')
                                    THEN nl."Units"
                                ELSE nl."Units" * p."MP_price_in_stable"
                            END AS "Returned Amount (Stable)"
                        FROM norm_leased nl
                        LEFT JOIN LATERAL (
                            SELECT "MP_price_in_stable"
                            FROM "MP_Asset" m
                            WHERE m."MP_asset_symbol" = nl."Leased Asset"
                                AND m."MP_asset_timestamp" BETWEEN nl."Close Timestamp" - INTERVAL '30 minutes'
                                                            AND nl."Close Timestamp" + INTERVAL '30 minutes'
                            ORDER BY ABS(EXTRACT(EPOCH FROM (m."MP_asset_timestamp" - nl."Close Timestamp"))) ASC
                            LIMIT 1
                        ) p ON TRUE
                    ),
                    agg_leased AS (
                        SELECT
                            "Contract ID",
                            MAX("Close Timestamp") AS "Close Timestamp (Leased)",
                            MIN("Leased Asset") AS "Returned LPN",
                            SUM("Units") AS "Returned Amount (LPN Units)",
                            SUM("Returned Amount (Stable)") AS "Returned Amount (Stable)"
                        FROM priced_leased
                        GROUP BY "Contract ID"
                    ),
                    inflow AS (
                        SELECT
                            lc."Contract ID",
                            lc."Close Timestamp",
                            COALESCE(al."Returned LPN", agl."Returned LPN") AS "Returned LPN",
                            COALESCE(al."Returned Amount (LPN Units)", agl."Returned Amount (LPN Units)") AS "Returned Amount (LPN Units)",
                            COALESCE(al."Returned Amount (Stable)", agl."Returned Amount (Stable)") AS "Returned Amount (Stable)"
                        FROM loan_close lc
                        LEFT JOIN agg_lpn al ON al."Contract ID" = lc."Contract ID"
                        LEFT JOIN agg_leased agl ON agl."Contract ID" = lc."Contract ID"
                    ),
                    repays AS (
                        SELECT 
                            r."LS_contract_id" AS "Contract ID",
                            SUM(r."LS_payment_amnt_stable") / 1000000 AS "Manual Repayments (Stable)"
                        FROM "LS_Repayment" r 
                        JOIN openings o ON o."Contract ID" = r."LS_contract_id"
                        GROUP BY r."LS_contract_id"
                    ),
                    liqs AS (
                        SELECT 
                            l."LS_contract_id" AS "Contract ID",
                            SUM(l."LS_payment_amnt_stable") / 1000000 AS "Liquidations (Stable)",
                            COUNT(*) AS "Liquidation Events"
                        FROM "LS_Liquidation" l 
                        JOIN openings o ON o."Contract ID" = l."LS_contract_id"
                        GROUP BY l."LS_contract_id"
                    )
                    SELECT
                        o."Contract ID" AS contract_id,
                        o."User" AS user,
                        o."Leased Asset" AS leased_asset,
                        o."Down Payment Asset" AS down_payment_asset,
                        o."Opening Date" AS opening_date,
                        i."Close Timestamp" AS close_timestamp,
                        o."Down Payment (Stable)" AS down_payment_stable,
                        COALESCE(r."Manual Repayments (Stable)", 0) AS manual_repayments_stable,
                        (o."Down Payment (Stable)" + COALESCE(r."Manual Repayments (Stable)", 0)) AS total_outflow_stable,
                        COALESCE(l."Liquidations (Stable)", 0) AS liquidations_stable,
                        COALESCE(l."Liquidation Events", 0) AS liquidation_events,
                        i."Returned LPN" AS returned_lpn,
                        i."Returned Amount (LPN Units)" AS returned_amount_lpn_units,
                        i."Returned Amount (Stable)" AS returned_amount_stable,
                        CASE 
                            WHEN i."Returned Amount (Stable)" IS NULL THEN NULL
                            ELSE i."Returned Amount (Stable)" - (o."Down Payment (Stable)" + COALESCE(r."Manual Repayments (Stable)", 0))
                        END AS realized_pnl_stable
                FROM openings o
                JOIN inflow i ON i."Contract ID" = o."Contract ID"
                LEFT JOIN repays r ON r."Contract ID" = o."Contract ID"
                LEFT JOIN liqs l ON l."Contract ID" = o."Contract ID"
                ORDER BY i."Close Timestamp" DESC
            "#,
            time_condition
        );

        let mut query_builder = sqlx::query_as::<_, RealizedPnlWallet>(&query);

        if let Some(from_ts) = from {
            query_builder = query_builder.bind(from_ts);
        }

        let data = query_builder.persistent(true).fetch_all(&self.pool).await?;

        Ok(data)
    }
}
