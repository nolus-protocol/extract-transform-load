use std::{collections::HashMap, str::FromStr as _};

use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::{types::BigDecimal, Error, FromRow, QueryBuilder, Transaction};

#[derive(Debug, Clone, FromRow)]
pub struct BorrowedByProtocol {
    pub protocol: String,
    pub loan: BigDecimal,
}

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

use crate::model::{
    Borrow_APR, LS_Amount, LS_History, LS_Opening, LS_Realized_Pnl_Data,
    Leased_Asset, Leases_Monthly, Table,
};

use super::{DataBase, QueryResult};

impl Table<LS_Opening> {
    /// Inserts a record if it doesn't already exist, using ON CONFLICT DO NOTHING.
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
        .bind(data.LS_lpn_decimals)
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
                .push_bind(ls.LS_lpn_decimals)
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

        let mut query_builder =
            sqlx::query_as::<_, Borrow_APR>(&query).bind(&protocol);

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
            SELECT o."LS_asset_symbol" AS "Asset", SUM(o."LS_loan_amnt_asset" / pc.lpn_decimals::numeric) AS "Loan"
            FROM "LS_Opening" o
            INNER JOIN pool_config pc ON pc.pool_id = o."LS_loan_pool_id"
            WHERE o."LS_loan_pool_id" = $1
            GROUP BY o."LS_asset_symbol"
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
            WITH LatestAggregation AS (
                SELECT MAX("LS_timestamp") AS max_ts FROM "LS_State"
            ),
            Opened AS (
                SELECT
                    s."LS_contract_id",
                    s."LS_amnt_stable",
                    lo."LS_asset_symbol",
                    CASE
                        WHEN pc.position_type = 'Short' THEN CONCAT(pc.label, ' (Short)')
                        ELSE lo."LS_asset_symbol"
                    END AS "Asset Type",
                    cr.decimal_digits AS asset_decimals
                FROM
                    "LS_State" s
                CROSS JOIN
                    LatestAggregation la
                INNER JOIN
                    "LS_Opening" lo ON lo."LS_contract_id" = s."LS_contract_id"
                LEFT JOIN
                    pool_config pc ON lo."LS_loan_pool_id" = pc.pool_id
                LEFT JOIN
                    currency_registry cr ON cr.ticker = lo."LS_asset_symbol"
                WHERE
                    s."LS_timestamp" = la.max_ts
                    AND s."LS_amnt_stable" > 0
            ),
            Lease_Value_Table AS (
                SELECT
                    op."Asset Type" AS "Asset",
                    op."LS_amnt_stable" / POWER(10, op.asset_decimals) AS "Lease Value"
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
                 WITH Latest_Aggregation AS (
                SELECT MAX("LS_timestamp") AS max_ts FROM "LS_State"
                ),
                Last_Hour_States AS (
                SELECT
                    *
                FROM
                    "LS_State"
                WHERE
                    "LS_timestamp" = (SELECT max_ts FROM Latest_Aggregation)
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
                WITH Latest_Aggregation AS (
                SELECT MAX("LS_timestamp") AS max_ts FROM "LS_State"
                ),
                Last_Hour_States AS (
                SELECT
                    *
                FROM
                    "LS_State"
                WHERE
                    "LS_timestamp" = (SELECT max_ts FROM Latest_Aggregation)
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
                SELECT SUM(o."LS_loan_amnt_asset" / pc.lpn_decimals::numeric) AS "Loan"
                FROM "LS_Opening" o
                INNER JOIN pool_config pc ON pc.pool_id = o."LS_loan_pool_id"
                WHERE o."LS_loan_pool_id" = $1
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
                SELECT SUM(o."LS_loan_amnt_asset" / pc.lpn_decimals::numeric) AS "Loan"
                FROM "LS_Opening" o
                INNER JOIN pool_config pc ON pc.pool_id = o."LS_loan_pool_id"
            "#,
        )
        .persistent(true)
        .fetch_optional(&self.pool)
        .await?;
        let amnt = value.unwrap_or((BigDecimal::from_str("0")?,));

        Ok(amnt.0)
    }

    /// Fetch borrowed amounts for all protocols in a single query.
    /// Returns a HashMap mapping pool_id -> borrowed amount.
    pub async fn get_borrowed_by_protocols(
        &self,
    ) -> Result<HashMap<String, BigDecimal>, crate::error::Error> {
        let rows: Vec<BorrowedByProtocol> = sqlx::query_as(
            r#"
                SELECT
                    o."LS_loan_pool_id" AS protocol,
                    COALESCE(SUM(o."LS_loan_amnt_asset" / pc.lpn_decimals::numeric), 0) AS loan
                FROM "LS_Opening" o
                INNER JOIN pool_config pc ON pc.pool_id = o."LS_loan_pool_id"
                GROUP BY o."LS_loan_pool_id"
            "#,
        )
        .persistent(true)
        .fetch_all(&self.pool)
        .await?;

        let mut result = HashMap::new();
        for row in rows {
            result.insert(row.protocol, row.loan);
        }
        Ok(result)
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
                        lo."LS_cltr_amnt_stable" / POWER(10, cr_cltr.decimal_digits) AS "Down Payment Amount",
                        lo."LS_loan_amnt_stable" / pc.lpn_decimals::numeric AS "Loan"
                    FROM "LS_Opening" lo
                    INNER JOIN pool_config pc ON lo."LS_loan_pool_id" = pc.pool_id
                    INNER JOIN currency_registry cr_cltr ON cr_cltr.ticker = lo."LS_cltr_symbol"
                    ),
                    LP_Deposits AS (
                    SELECT
                        d."LP_amnt_stable" / pc.lpn_decimals::numeric AS "Volume"
                    FROM "LP_Deposit" d
                    INNER JOIN pool_config pc ON d."LP_address_id" = pc.pool_id
                    ),

                    LP_Withdrawals AS (
                    SELECT
                        w."LP_amnt_stable" / pc.lpn_decimals::numeric AS "Volume"
                    FROM "LP_Withdraw" w
                    INNER JOIN pool_config pc ON w."LP_address_id" = pc.pool_id
                    ),
                    LS_Close AS (
                    SELECT
                        c."LS_payment_amnt_stable" / POWER(10, cr.decimal_digits) AS "Volume"
                    FROM "LS_Close_Position" c
                    INNER JOIN currency_registry cr ON cr.ticker = c."LS_payment_symbol"
                    ),
                    LS_Repayment AS (
                    SELECT
                        r."LS_payment_amnt_stable" / POWER(10, cr.decimal_digits) AS "Volume"
                    FROM "LS_Repayment" r
                    INNER JOIN currency_registry cr ON cr.ticker = r."LS_payment_symbol"
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
                    WHEN pc.position_type = 'Short' THEN pc.label
                    ELSE lso."LS_asset_symbol"
                END AS "Leased Asset",
                DATE_TRUNC('month', lso."LS_timestamp") AS "Date",
                lso."LS_cltr_amnt_stable" / POWER(10, cr_cltr.decimal_digits) AS "Down Payment Amount",
                lso."LS_loan_amnt_stable" / pc.lpn_decimals::numeric AS "Loan Amount"
            FROM
                "LS_Opening" lso
            LEFT JOIN
                pool_config pc ON lso."LS_loan_pool_id" = pc.pool_id
            LEFT JOIN
                currency_registry cr_cltr ON cr_cltr.ticker = lso."LS_cltr_symbol"
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
            SUM(s."LS_amnt_stable" / POWER(10, cr_asset.decimal_digits)) AS "amount"
            FROM "LS_State" s
            INNER JOIN "LS_Opening" o ON o."LS_contract_id" = s."LS_contract_id"
            INNER JOIN currency_registry cr_asset ON cr_asset.ticker = o."LS_asset_symbol"
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
                / pc.lpn_decimals::numeric
            ) AS "amount"
            FROM "LS_State" s
            INNER JOIN "LS_Opening" o ON o."LS_contract_id" = s."LS_contract_id"
            INNER JOIN pool_config pc ON pc.pool_id = o."LS_loan_pool_id"
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
                    COALESCE(pc.position_type, CASE WHEN o."LS_asset_symbol" IN ('USDC','USDC_NOBLE') THEN 'Short' ELSE 'Long' END) AS pos_type,
                    pc.label AS short_symbol
                FROM "LS_Opening" o
                INNER JOIN pool_config pc
                    ON pc.pool_id = o."LS_loan_pool_id"
                WHERE o."LS_address_id" = $1
                ),

                -- Sum of repayments per contract (stable units)
                repayments AS (
                SELECT
                    r."LS_contract_id",
                    SUM(r."LS_payment_amnt_stable") / pc.stable_currency_decimals::numeric AS total_repaid_stable
                FROM "LS_Repayment" r
                LEFT JOIN "LS_Opening" o ON o."LS_contract_id" = r."LS_contract_id"
                INNER JOIN pool_config pc ON pc.pool_id = o."LS_loan_pool_id"
                GROUP BY r."LS_contract_id", pc.stable_currency_decimals
                ),

                -- Sum of user collects per contract (normalize by LS_symbol)
                collects AS (
                SELECT
                    lc."LS_contract_id",
                    SUM(lc."LS_amount_stable" / POWER(10, cr.decimal_digits)) AS total_collect_normalized
                FROM "LS_Loan_Collect" lc
                INNER JOIN currency_registry cr ON cr.ticker = lc."LS_symbol"
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
                    o."LS_cltr_amnt_stable" / POWER(10, cr_cltr.decimal_digits)
                    + COALESCE(r.total_repaid_stable, 0.0)
                    ) AS "Sent Amount",
                    'USDC' AS "Sent Currency",
                    o."LS_loan_amnt" / POWER(10, cr_asset.decimal_digits) AS "Received Amount",
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
                INNER JOIN currency_registry cr_cltr ON cr_cltr.ticker = o."LS_cltr_symbol"
                INNER JOIN currency_registry cr_asset ON cr_asset.ticker = o."LS_asset_symbol"
                ),

                -- Closing row
                closing_rows AS (
                SELECT
                    cts.close_ts AS "Date",
                    o."LS_contract_id" AS "Position ID",
                    o."LS_loan_amnt" / POWER(10, cr_asset.decimal_digits) AS "Sent Amount",
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
                INNER JOIN currency_registry cr_asset ON cr_asset.ticker = o."LS_asset_symbol"
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
        self.get_monthly_active_wallets_with_window(None, None)
            .await
    }

    pub async fn get_monthly_active_wallets_with_window(
        &self,
        months: Option<i32>,
        from: Option<DateTime<Utc>>,
    ) -> Result<Vec<MonthlyActiveWallets>, Error> {
        let time_filter = if let Some(m) = months {
            format!(
                "WHERE combined_timestamp >= NOW() - INTERVAL '{} months'",
                m
            )
        } else {
            String::new()
        };

        let from_filter = if from.is_some() {
            if time_filter.is_empty() {
                "WHERE combined_timestamp > $1".to_string()
            } else {
                " AND combined_timestamp > $1".to_string()
            }
        } else {
            String::new()
        };

        let query = format!(
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
            ),
            combined_data AS (
                SELECT "LS_timestamp" AS combined_timestamp, "LS_address_id" AS address FROM "LS_Opening"
                UNION ALL
                SELECT "LP_timestamp" AS combined_timestamp, "LP_address_id" AS address FROM "LP_Deposit"
                UNION ALL
                SELECT "LP_timestamp" AS combined_timestamp, "LP_address_id" AS address FROM "LP_Withdraw"
                UNION ALL
                SELECT "LS_timestamp" AS combined_timestamp, "LS_address_id" AS address FROM Market_Close_With_Owners
                UNION ALL
                SELECT "LS_timestamp" AS combined_timestamp, "LS_address_id" AS address FROM Repayment_With_Owners
            )
            SELECT
                TO_CHAR(combined_timestamp, 'YYYY-MM') AS month,
                COUNT(DISTINCT address) AS unique_addresses
            FROM combined_data
            {} {}
            GROUP BY month
            ORDER BY month ASC
            "#,
            time_filter, from_filter
        );

        let data = if let Some(from_ts) = from {
            sqlx::query_as(&query)
                .bind(from_ts)
                .fetch_all(&self.pool)
                .await?
        } else {
            sqlx::query_as(&query).fetch_all(&self.pool).await?
        };

        Ok(data)
    }

    pub async fn get_daily_opened_closed(
        &self,
    ) -> Result<Vec<(DateTime<Utc>, BigDecimal, BigDecimal)>, Error> {
        self.get_daily_opened_closed_with_window(None, None).await
    }

    /// Get daily opened/closed loans with time window filtering
    pub async fn get_daily_opened_closed_with_window(
        &self,
        months: Option<i32>,
        from: Option<DateTime<Utc>>,
    ) -> Result<Vec<(DateTime<Utc>, BigDecimal, BigDecimal)>, Error> {
        // Build time filter clause
        let time_filter = if let Some(m) = months {
            format!("WHERE \"LS_timestamp\" >= NOW() - INTERVAL '{} months'", m)
        } else {
            String::new()
        };

        let from_filter = if from.is_some() {
            if time_filter.is_empty() {
                "WHERE \"LS_timestamp\" > $1".to_string()
            } else {
                " AND \"LS_timestamp\" > $1".to_string()
            }
        } else {
            String::new()
        };

        let combined_filter = format!("{}{}", time_filter, from_filter);

        let query = format!(
            r#"
            WITH FilteredClosePosition AS (
                SELECT "LS_contract_id", "LS_timestamp", "LS_principal_stable"
                FROM "LS_Close_Position"
                {}
            ),
            FilteredRepayment AS (
                SELECT "LS_contract_id", "LS_timestamp", "LS_principal_stable"
                FROM "LS_Repayment"
                {}
            ),
            FilteredOpening AS (
                SELECT *
                FROM "LS_Opening"
                {}
            ),
            FilteredLiquidation AS (
                SELECT "LS_contract_id", "LS_timestamp", "LS_principal_stable"
                FROM "LS_Liquidation"
                {}
            ),
            DateSeries AS (
                SELECT generate_series(
                    DATE(MIN(earliest_date)),
                    DATE(MAX(latest_date)),
                    '1 day'::interval
                ) AS "Date"
                FROM (
                    SELECT MIN("LS_timestamp") AS earliest_date, MAX("LS_timestamp") AS latest_date FROM FilteredClosePosition
                    UNION ALL
                    SELECT MIN("LS_timestamp") AS earliest_date, MAX("LS_timestamp") AS latest_date FROM FilteredRepayment
                    UNION ALL
                    SELECT MIN("LS_timestamp") AS earliest_date, MAX("LS_timestamp") AS latest_date FROM FilteredOpening
                    UNION ALL
                    SELECT MIN("LS_timestamp") AS earliest_date, MAX("LS_timestamp") AS latest_date FROM FilteredLiquidation
                ) AS combined_dates
            ),
            Close_Loans AS (
                SELECT "LS_contract_id", "LS_timestamp", "LS_principal_stable" FROM FilteredClosePosition
                UNION ALL
                SELECT "LS_contract_id", "LS_timestamp", "LS_principal_stable" FROM FilteredRepayment
                UNION ALL
                SELECT "LS_contract_id", "LS_timestamp", "LS_principal_stable" FROM FilteredLiquidation
            ),
            DailyClosedLoans AS (
                SELECT
                    ds."Date" AS "LocalDate",
                    COALESCE(SUM(cl."LS_principal_stable" / pc.stable_currency_decimals::numeric), 0) AS "ClosedLoans"
                FROM
                    DateSeries ds
                LEFT JOIN
                    Close_Loans cl
                    ON DATE(cl."LS_timestamp") = ds."Date"
                LEFT JOIN
                    "LS_Opening" o ON o."LS_contract_id" = cl."LS_contract_id"
                LEFT JOIN
                    pool_config pc ON pc.pool_id = o."LS_loan_pool_id"
                GROUP BY
                    ds."Date"
            ),
            DailyOpenedLoans AS (
                SELECT
                    ds."Date" AS "LocalDate",
                    COALESCE(SUM(lo."LS_loan_amnt_stable" / pc.lpn_decimals::numeric), 0) AS "OpenedLoans"
                FROM
                    DateSeries ds
                LEFT JOIN
                    FilteredOpening lo ON DATE(lo."LS_timestamp") = ds."Date"
                LEFT JOIN
                    pool_config pc ON lo."LS_loan_pool_id" = pc.pool_id
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
            combined_filter, combined_filter, combined_filter, combined_filter
        );

        let mut query_builder = sqlx::query_as::<
            _,
            (DateTime<Utc>, BigDecimal, BigDecimal),
        >(&query);

        if let Some(from_ts) = from {
            query_builder = query_builder.bind(from_ts);
        }

        let data = query_builder.persistent(true).fetch_all(&self.pool).await?;
        Ok(data)
    }

    pub async fn get_loans_granted(
        &self,
    ) -> Result<Vec<LoanGranted>, crate::error::Error> {
        let data = sqlx::query_as(
            r#"
            SELECT
                CASE
                    WHEN pc.position_type = 'Short' THEN CONCAT(pc.label, ' (Short)')
                    ELSE lo."LS_asset_symbol"
                END AS asset,
                SUM(lo."LS_loan_amnt_stable" / pc.lpn_decimals::numeric) AS loan
            FROM "LS_Opening" lo
            INNER JOIN pool_config pc ON lo."LS_loan_pool_id" = pc.pool_id
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
                        WHEN pc.position_type = 'Short' THEN pc.label
                        ELSE lso."LS_asset_symbol"
                    END AS "Leased Asset",
                    lso."LS_timestamp" AS "Opening Date",
                    COALESCE(pc.position_type, 'Long') AS "Type",
                    lso."LS_cltr_amnt_stable" / POWER(10, cr_cltr.decimal_digits) AS "Down Payment Amount",
                    lso."LS_cltr_symbol" AS "Down Payment Asset",
                    lso."LS_loan_amnt_stable" / pc.lpn_decimals::numeric AS "Loan",
                    lso."LS_lpn_loan_amnt" / lso."LS_lpn_decimals"::numeric AS "Total Position Amount (LPN)"
                FROM "LS_Opening" lso
                INNER JOIN pool_config pc ON lso."LS_loan_pool_id" = pc.pool_id
                INNER JOIN currency_registry cr_cltr ON cr_cltr.ticker = lso."LS_cltr_symbol"
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
                CASE 
                    WHEN COALESCE(o."LS_position_type", pc.position_type) = 'Short' THEN COALESCE(pc."label", o."LS_asset_symbol")
                    ELSE o."LS_asset_symbol"
                END AS leased_asset,
                o."LS_timestamp" AS opening_date,
                -- Use pre-computed position_type or fallback to pool_config
                COALESCE(o."LS_position_type", pc.position_type, 'Long') AS position_type,
                -- Normalized down payment amount using currency_registry
                o."LS_cltr_amnt_stable" / POWER(10, cr_cltr.decimal_digits) AS down_payment_amount,
                o."LS_cltr_symbol" AS down_payment_asset,
                -- Normalized loan amount using pool_config
                o."LS_loan_amnt_stable" / pc.lpn_decimals::numeric AS loan,
                -- Total position amount in LPN
                COALESCE(o."LS_lpn_loan_amnt" / o."LS_lpn_decimals"::numeric, 0) AS total_position_amount_lpn,
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
                        WHEN COALESCE(o."LS_position_type", pc.position_type, 'Long') = 'Long' THEN
                            (o."LS_loan_amnt_stable" / pc.lpn_decimals::numeric / 0.9) / 
                            NULLIF((o."LS_cltr_amnt_stable" + o."LS_loan_amnt_stable") / pc.lpn_decimals::numeric, 0) *
                            COALESCE(o."LS_opening_price", (
                                SELECT m."MP_price_in_stable"
                                FROM "MP_Asset" m
                                WHERE m."MP_asset_symbol" = o."LS_asset_symbol"
                                  AND m."MP_asset_timestamp" <= o."LS_timestamp"
                                ORDER BY m."MP_asset_timestamp" DESC
                                LIMIT 1
                            ))
                        WHEN COALESCE(o."LS_position_type", pc.position_type, 'Long') = 'Short' THEN
                            ((o."LS_cltr_amnt_stable" + o."LS_loan_amnt_stable") / pc.lpn_decimals::numeric) /
                            NULLIF(o."LS_lpn_loan_amnt" / o."LS_lpn_decimals"::numeric / 0.9, 0)
                    END
                ) AS liquidation_price
            FROM "LS_Opening" o
            INNER JOIN "pool_config" pc ON o."LS_loan_pool_id" = pc."pool_id"
            INNER JOIN currency_registry cr_cltr ON cr_cltr.ticker = o."LS_cltr_symbol"
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
    /// OPTIMIZED: Uses pre-computed LS_opening_price and LS_liquidation_price_at_open
    /// instead of expensive LATERAL JOINs to MP_Asset
    pub async fn get_all_historically_opened(
        &self,
    ) -> Result<Vec<HistoricallyOpened>, crate::error::Error> {
        let data = sqlx::query_as(
            r#"
            SELECT
                o."LS_contract_id" AS contract_id,
                o."LS_address_id" AS user,
                CASE
                    WHEN COALESCE(o."LS_position_type", pc.position_type) = 'Short' THEN COALESCE(pc."label", o."LS_asset_symbol")
                    ELSE o."LS_asset_symbol"
                END AS leased_asset,
                o."LS_timestamp" AS opening_date,
                COALESCE(o."LS_position_type", pc.position_type, 'Long') AS position_type,
                o."LS_cltr_amnt_stable" / POWER(10, cr_cltr.decimal_digits) AS down_payment_amount,
                o."LS_cltr_symbol" AS down_payment_asset,
                o."LS_loan_amnt_stable" / pc.lpn_decimals::numeric AS loan,
                COALESCE(o."LS_lpn_loan_amnt" / o."LS_lpn_decimals"::numeric, 0) AS total_position_amount_lpn,
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
                EXISTS (
                    SELECT 1
                    FROM "LS_State" s
                    WHERE s."LS_contract_id" = o."LS_contract_id"
                      AND s."LS_timestamp" >= NOW() - interval '1 hour'
                ) AS open,
                COALESCE(
                    o."LS_liquidation_price_at_open",
                    CASE
                        WHEN COALESCE(o."LS_position_type", pc.position_type, 'Long') = 'Long' THEN
                            (o."LS_loan_amnt_stable" / pc.lpn_decimals::numeric / 0.9) / 
                            NULLIF((o."LS_cltr_amnt_stable" + o."LS_loan_amnt_stable") / pc.lpn_decimals::numeric, 0) *
                            COALESCE(o."LS_opening_price", (
                                SELECT m."MP_price_in_stable"
                                FROM "MP_Asset" m
                                WHERE m."MP_asset_symbol" = o."LS_asset_symbol"
                                  AND m."MP_asset_timestamp" <= o."LS_timestamp"
                                ORDER BY m."MP_asset_timestamp" DESC
                                LIMIT 1
                            ))
                        WHEN COALESCE(o."LS_position_type", pc.position_type, 'Long') = 'Short' THEN
                            ((o."LS_cltr_amnt_stable" + o."LS_loan_amnt_stable") / pc.lpn_decimals::numeric) /
                            NULLIF(o."LS_lpn_loan_amnt" / o."LS_lpn_decimals"::numeric / 0.9, 0)
                    END
                ) AS liquidation_price
            FROM "LS_Opening" o
            INNER JOIN "pool_config" pc ON o."LS_loan_pool_id" = pc."pool_id"
            INNER JOIN currency_registry cr_cltr ON cr_cltr.ticker = o."LS_cltr_symbol"
            ORDER BY o."LS_timestamp" DESC
            "#,
        )
        .persistent(true)
        .fetch_all(&self.pool)
        .await?;

        Ok(data)
    }

    /// Get realized PnL by wallet - OPTIMIZED
    /// Uses pre-computed _stable values from LS_Close_Position and LS_Loan_Closing
    /// instead of expensive LATERAL JOINs to MP_Asset for price lookups
    pub async fn get_realized_pnl_by_wallet(
        &self,
        skip: i64,
        limit: i64,
    ) -> Result<Vec<RealizedPnlWallet>, crate::error::Error> {
        let data = sqlx::query_as(
            r#"
            WITH openings AS (
                SELECT
                    o."LS_contract_id" AS "Contract ID",
                    o."LS_address_id" AS "User",
                    o."LS_timestamp" AS "Opening Date",
                    o."LS_asset_symbol" AS "Leased Asset",
                    o."LS_cltr_symbol" AS "Down Payment Asset",
                    o."LS_cltr_amnt_stable" / pc.stable_currency_decimals::numeric AS "Down Payment (Stable)",
                    COALESCE(pc.lpn_symbol, 'USDC_NOBLE') AS "LPN_Symbol",
                    pc.stable_currency_decimals::numeric AS "stable_decimals"
                FROM "LS_Opening" o
                INNER JOIN pool_config pc ON o."LS_loan_pool_id" = pc.pool_id
                WHERE o."LS_timestamp" >= NOW() - INTERVAL '1 year'
            ),
            loan_close AS (
                SELECT
                    lc."LS_contract_id" AS "Contract ID",
                    lc."LS_timestamp" AS "Close Timestamp",
                    lc."LS_amnt_stable" / o."stable_decimals" AS "Returned Amount (Stable)"
                FROM "LS_Loan_Closing" lc
                JOIN openings o ON o."Contract ID" = lc."LS_contract_id"
            ),
            close_position_agg AS (
                SELECT
                    c."LS_contract_id" AS "Contract ID",
                    MAX(c."LS_timestamp") AS "Close Timestamp",
                    c."LS_payment_symbol" AS "Returned LPN",
                    SUM(c."LS_change") / POWER(10, cr.decimal_digits) AS "Returned Amount (LPN Units)",
                    SUM(c."LS_payment_amnt_stable") / o."stable_decimals" AS "Returned Amount (Stable)"
                FROM "LS_Close_Position" c
                JOIN openings o ON o."Contract ID" = c."LS_contract_id"
                INNER JOIN currency_registry cr ON cr.ticker = c."LS_payment_symbol"
                GROUP BY c."LS_contract_id", c."LS_payment_symbol", o."stable_decimals", cr.decimal_digits
            ),
            inflow AS (
                SELECT
                    COALESCE(lc."Contract ID", cp."Contract ID") AS "Contract ID",
                    COALESCE(lc."Close Timestamp", cp."Close Timestamp") AS "Close Timestamp",
                    cp."Returned LPN",
                    cp."Returned Amount (LPN Units)",
                    COALESCE(lc."Returned Amount (Stable)", cp."Returned Amount (Stable)") AS "Returned Amount (Stable)"
                FROM loan_close lc
                FULL OUTER JOIN close_position_agg cp ON lc."Contract ID" = cp."Contract ID"
            ),
            repays AS (
                SELECT 
                    r."LS_contract_id" AS "Contract ID",
                    SUM(r."LS_payment_amnt_stable") / o."stable_decimals" AS "Manual Repayments (Stable)"
                FROM "LS_Repayment" r 
                JOIN openings o ON o."Contract ID" = r."LS_contract_id"
                GROUP BY r."LS_contract_id", o."stable_decimals"
            ),
            liqs AS (
                SELECT 
                    l."LS_contract_id" AS "Contract ID",
                    SUM(l."LS_payment_amnt_stable") / o."stable_decimals" AS "Liquidations (Stable)",
                    COUNT(*) AS "Liquidation Events"
                FROM "LS_Liquidation" l 
                JOIN openings o ON o."Contract ID" = l."LS_contract_id"
                GROUP BY l."LS_contract_id", o."stable_decimals"
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

    /// Get realized PnL by wallet with time window filtering - OPTIMIZED
    /// Uses pre-computed _stable values instead of LATERAL JOINs to MP_Asset
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
            WITH openings AS (
                SELECT
                    o."LS_contract_id" AS "Contract ID",
                    o."LS_address_id" AS "User",
                    o."LS_timestamp" AS "Opening Date",
                    o."LS_asset_symbol" AS "Leased Asset",
                    o."LS_cltr_symbol" AS "Down Payment Asset",
                    o."LS_cltr_amnt_stable" / pc.stable_currency_decimals::numeric AS "Down Payment (Stable)",
                    COALESCE(pc.lpn_symbol, 'USDC_NOBLE') AS "LPN_Symbol",
                    pc.stable_currency_decimals::numeric AS "stable_decimals"
                FROM "LS_Opening" o
                INNER JOIN pool_config pc ON o."LS_loan_pool_id" = pc.pool_id
                {}
            ),
            loan_close AS (
                SELECT
                    lc."LS_contract_id" AS "Contract ID",
                    lc."LS_timestamp" AS "Close Timestamp",
                    lc."LS_amnt_stable" / o."stable_decimals" AS "Returned Amount (Stable)"
                FROM "LS_Loan_Closing" lc
                JOIN openings o ON o."Contract ID" = lc."LS_contract_id"
            ),
            close_position_agg AS (
                SELECT
                    c."LS_contract_id" AS "Contract ID",
                    MAX(c."LS_timestamp") AS "Close Timestamp",
                    c."LS_payment_symbol" AS "Returned LPN",
                    SUM(c."LS_change") / POWER(10, cr.decimal_digits) AS "Returned Amount (LPN Units)",
                    SUM(c."LS_payment_amnt_stable") / o."stable_decimals" AS "Returned Amount (Stable)"
                FROM "LS_Close_Position" c
                JOIN openings o ON o."Contract ID" = c."LS_contract_id"
                INNER JOIN currency_registry cr ON cr.ticker = c."LS_payment_symbol"
                GROUP BY c."LS_contract_id", c."LS_payment_symbol", o."stable_decimals", cr.decimal_digits
            ),
            inflow AS (
                SELECT
                    COALESCE(lc."Contract ID", cp."Contract ID") AS "Contract ID",
                    COALESCE(lc."Close Timestamp", cp."Close Timestamp") AS "Close Timestamp",
                    cp."Returned LPN",
                    cp."Returned Amount (LPN Units)",
                    COALESCE(lc."Returned Amount (Stable)", cp."Returned Amount (Stable)") AS "Returned Amount (Stable)"
                FROM loan_close lc
                FULL OUTER JOIN close_position_agg cp ON lc."Contract ID" = cp."Contract ID"
            ),
            repays AS (
                SELECT 
                    r."LS_contract_id" AS "Contract ID",
                    SUM(r."LS_payment_amnt_stable") / o."stable_decimals" AS "Manual Repayments (Stable)"
                FROM "LS_Repayment" r 
                JOIN openings o ON o."Contract ID" = r."LS_contract_id"
                GROUP BY r."LS_contract_id", o."stable_decimals"
            ),
            liqs AS (
                SELECT 
                    l."LS_contract_id" AS "Contract ID",
                    SUM(l."LS_payment_amnt_stable") / o."stable_decimals" AS "Liquidations (Stable)",
                    COUNT(*) AS "Liquidation Events"
                FROM "LS_Liquidation" l 
                JOIN openings o ON o."Contract ID" = l."LS_contract_id"
                GROUP BY l."LS_contract_id", o."stable_decimals"
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
