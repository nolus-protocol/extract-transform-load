use std::str::FromStr as _;

use chrono::{DateTime, Utc};
use sqlx::{types::BigDecimal, Error, QueryBuilder, Transaction};

use crate::{
    model::{
        Borrow_APR, LS_Amount, LS_History, LS_Opening, Leased_Asset,
        Leases_Monthly, Table,
    },
    types::LS_Max_Interest,
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
                "LS_lpn_loan_amnt"
            )
            VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
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
        .persistent(false)
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
                "LS_lpn_loan_amnt"
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
                .push_bind(&ls.LS_lpn_loan_amnt);
        });

        let query = query_builder.build().persistent(false);
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
        .persistent(false)
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
        .persistent(false)
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
        .persistent(false)
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
        .persistent(false)
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
        .persistent(false)
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
        .persistent(false)
        .fetch_all(&self.pool)
        .await?;
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
        .persistent(false)
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
                "LS_timestamp" > (now() - INTERVAL '1 hour')
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
        .persistent(false)
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
                    "LS_timestamp" >= NOW() - INTERVAL '1 hour'
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
            .persistent(false)
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
                    "LS_timestamp" >= NOW() - INTERVAL '1 hour'
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
        .persistent(false)
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
        .persistent(false)
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
        .persistent(false)
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
        .persistent(false)
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

        let data = query.persistent(false).fetch_all(&self.pool).await?;
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
          .persistent(false)
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

    pub async fn get_max_ls_interest_7d(
        &self,
        lpp_address: String,
    ) -> Result<Vec<LS_Max_Interest>, Error> {
        let data = sqlx::query_as(
            r#"
                SELECT
                    DATE("LS_timestamp") AS "date",
                    MAX("LS_interest") AS "max_interest"
                FROM
                    "LS_Opening"
                WHERE
                    "LS_timestamp" >= CURRENT_DATE - INTERVAL '7 days'
                    AND "LS_loan_pool_id" = $1
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
                    a."LS_lpn_loan_amnt"
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
        .persistent(false)
        .fetch_all(&self.pool)
        .await?;
        Ok(data)
    }

    //TODO: delete
    pub async fn get_leases_data(
        &self,
        leases: Vec<String>,
    ) -> Result<Vec<LS_Opening>, Error> {
        let mut s = String::from("");

        for (index, lease) in leases.iter().enumerate() {
            s += &format!("'{}'", lease);
            if index < leases.len() - 1 {
                s += ","
            }
        }

        let parsed_string = format!(
            r#"SELECT * FROM "LS_Opening" WHERE "LS_contract_id" IN ({})"#,
            s
        );

        let data = sqlx::query_as(&parsed_string)
            .persistent(false)
            .fetch_all(&self.pool)
            .await?;
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
        .persistent(false)
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
        .persistent(false)
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
        .persistent(false)
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
        .persistent(false)
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
        .persistent(false)
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

                -- Long positions handled by asset symbol
                WHEN o."LS_asset_symbol" IN ('WBTC', 'CRO', 'ALL_BTC') THEN 100000000
                WHEN o."LS_asset_symbol" IN ('ALL_SOL') THEN 1000000000
                WHEN o."LS_asset_symbol" = 'PICA' THEN 1000000000000
                WHEN o."LS_asset_symbol" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS', 'ALL_ETH') THEN 1000000000000000000

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
        .persistent(false)
        .fetch_all(&self.pool)
        .await?;
        Ok(data)
    }
}
