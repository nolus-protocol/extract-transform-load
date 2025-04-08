use bigdecimal::Zero as _;
use chrono::{DateTime, Utc};
use sqlx::{types::BigDecimal, Error, QueryBuilder, Transaction};

use crate::{
    model::{
        Borrow_APR, LS_History, LS_Opening, Leased_Asset, Leases_Monthly, Table,
    },
    types::LS_Max_Interest,
};

use super::DataBase;

impl Table<LS_Opening> {
    pub async fn isExists(
        &self,
        ls_opening: &LS_Opening,
    ) -> Result<bool, Error> {
        const SQL: &'static str = r#"
        SELECT EXISTS(
            SELECT
            FROM "LS_Opening"
            WHERE "LS_contract_id" = $1
        )
        "#;

        sqlx::query_as(SQL)
            .bind(&ls_opening.LS_contract_id)
            .fetch_one(&self.pool)
            .await
            .map(|(result,)| result)
    }

    pub async fn insert(
        &self,
        data: LS_Opening,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<(), Error> {
        const SQL: &str = r#"
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
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
        "#;

        sqlx::query(SQL)
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
            .execute(transaction.as_mut())
            .await
            .map(drop)
    }

    pub async fn insert_many(
        &self,
        data: &Vec<LS_Opening>,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<(), Error> {
        const SQL: &str = r#"
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
        "#;

        if data.is_empty() {
            return Ok(());
        }

        QueryBuilder::new(SQL)
            .push_values(data, |mut b, ls| {
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
            })
            .build()
            .persistent(false)
            .execute(transaction.as_mut())
            .await
            .map(drop)
    }

    pub async fn count(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<i64, Error> {
        const SQL: &str = r#"
        SELECT
            COUNT(*)
        FROM "LS_Opening"
        WHERE
            "LS_timestamp" > $1 AND
            "LS_timestamp" <= $2
        "#;

        sqlx::query_as(SQL)
            .bind(from)
            .bind(to)
            .fetch_one(&self.pool)
            .await
            .map(|(result,)| result)
    }

    pub async fn get_cltr_amnt_opened_stable_sum(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<BigDecimal, Error> {
        const SQL: &str = r#"
        SELECT
            SUM("LS_cltr_amnt_stable")
        FROM "LS_Opening"
        WHERE
            "LS_timestamp" > $1 AND
            "LS_timestamp" <= $2
        "#;

        sqlx::query_as(SQL)
            .bind(from)
            .bind(to)
            .fetch_optional(&self.pool)
            .await
            .map(|maybe_row| {
                maybe_row.map_or_else(BigDecimal::zero, |(result,)| result)
            })
    }

    pub async fn get_loan_amnt_stable_sum(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<BigDecimal, Error> {
        const SQL: &str = r#"
        SELECT
            SUM("LS_loan_amnt_stable")
        FROM "LS_Opening"
        WHERE
            "LS_timestamp" > $1 AND
            "LS_timestamp" <= $2
        "#;

        sqlx::query_as(SQL)
            .bind(from)
            .bind(to)
            .fetch_optional(&self.pool)
            .await
            .map(|maybe_row| {
                maybe_row.map_or_else(BigDecimal::zero, |(result,)| result)
            })
    }

    pub async fn get_ls_cltr_amnt_stable_sum(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<BigDecimal, Error> {
        const SQL: &str = r#"
        SELECT
            SUM("LS_cltr_amnt_stable")
        FROM "LS_Opening"
        LEFT JOIN "LS_Closing" ON "LS_Opening"."LS_contract_id" = "LS_Closing"."LS_contract_id"
        WHERE
            "LS_Closing"."LS_timestamp" > $1 AND
            "LS_Closing"."LS_timestamp" <= $2
        "#;

        sqlx::query_as(SQL)
            .bind(from)
            .bind(to)
            .fetch_optional(&self.pool)
            .await
            .map(|maybe_row| {
                maybe_row.map_or_else(BigDecimal::zero, |(result,)| result)
            })
    }

    pub async fn get_ls_amnt_stable_sum(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<BigDecimal, Error> {
        const SQL: &str = r#"
        SELECT
            SUM("LS_loan_amnt_stable" + "LS_cltr_amnt_stable")
        FROM "LS_Opening"
        LEFT JOIN "LS_Closing" ON "LS_Opening"."LS_contract_id" = "LS_Closing"."LS_contract_id"
        WHERE
            "LS_Closing"."LS_timestamp" > $1 AND
            "LS_Closing"."LS_timestamp" <= $2
        "#;

        sqlx::query_as(SQL)
            .bind(from)
            .bind(to)
            .fetch_optional(&self.pool)
            .await
            .map(|maybe_row| {
                maybe_row.map_or_else(BigDecimal::zero, |(result,)| result)
            })
    }

    pub async fn get_borrow_apr(
        &self,
        protocol: String,
        skip: i64,
        limit: i64,
    ) -> Result<Vec<Borrow_APR>, Error> {
        const SQL: &str = r#"
        SELECT
            (
                "LS_interest" / 10.0
            ) AS "APR"
        FROM "LS_Opening"
        WHERE "LS_loan_pool_id" = $1
        ORDER BY "LS_timestamp" DESC
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

    pub async fn get_leased_assets(
        &self,
        protocol: String,
    ) -> Result<Vec<Leased_Asset>, Error> {
        const SQL: &str = r#"
        SELECT
            "LS_asset_symbol" AS "Asset",
            SUM(
                "LS_loan_amnt_asset" / 1000000
            ) AS "Loan"
        FROM "LS_Opening"
        WHERE "LS_loan_pool_id" = $1
        GROUP BY "Asset"
        "#;

        sqlx::query_as(SQL)
            .bind(protocol)
            .fetch_all(&self.pool)
            .await
    }

    pub async fn get_leased_assets_total(
        &self,
    ) -> Result<Vec<Leased_Asset>, Error> {
        const SQL: &str = r#"
        WITH "LatestTimestamps" AS (
            SELECT
                "LS_contract_id",
                MAX("LS_timestamp") AS "MaxTimestamp"
            FROM "LS_State"
            WHERE "LS_timestamp" > (NOW() - INTERVAL '1 hour')
            GROUP BY "LS_contract_id"
        ),
        "Opened" AS (
            SELECT
                "s"."LS_contract_id",
                "s"."LS_amnt_stable",
                (
                    CASE
                        WHEN lo."LS_loan_pool_id" = 'nolus1jufcaqm6657xmfltdezzz85quz92rmtd88jk5x0hq9zqseem32ysjdm990' THEN
                            'ST_ATOM (Short)'
                        WHEN lo."LS_loan_pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN
                            'ALL_BTC (Short)'
                        WHEN lo."LS_loan_pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN
                            'ALL_SOL (Short)'
                        WHEN lo."LS_loan_pool_id" = 'nolus1lxr7f5xe02jq6cce4puk6540mtu9sg36at2dms5sk69wdtzdrg9qq0t67z' THEN
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
                "op"."Asset Type" AS "Asset",
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
            FROM "Opened" AS "op"
        )
        SELECT 
            "Asset", 
            SUM("Lease Value") AS "Loan"
        FROM "Lease_Value_Table" 
        GROUP BY "Asset"
        ORDER BY "Loan" DESC
        "#;

        sqlx::query_as(SQL).fetch_all(&self.pool).await
    }

    pub async fn get_earn_apr_interest(
        &self,
        protocol: String,
        max_interest: f32,
    ) -> Result<BigDecimal, Error> {
        const SQL: &str = r#"
        WITH "Last_Hour_States" AS (
            SELECT *
            FROM "LS_State"
            WHERE "LS_timestamp" >= (NOW() - INTERVAL '1 hour')
        ),
        "Last_Hour_Pool_State" AS (
            SELECT
                (
                    "LP_Pool_total_borrowed_stable" / NULLIF(
                        "LP_Pool_total_value_locked_stable",
                        0
                    )
                ) AS "utilization_rate"
            FROM "LP_Pool_State"
            WHERE "LP_Pool_id" = $1
            ORDER BY "LP_Pool_timestamp" DESC
            LIMIT 1
        ),
        "APRCalc" AS (
            SELECT
                (
                    (
                        (
                            AVG("o"."LS_interest") / 10.0
                        ) - $2
                    ) * (
                        SELECT
                            "utilization_rate"
                        FROM "Last_Hour_Pool_State"
                    )
                ) AS "apr"
            FROM "Last_Hour_States" AS "s"
            JOIN "LS_Opening" "o" ON "s"."LS_contract_id" = "o"."LS_contract_id"
            WHERE "o"."LS_loan_pool_id" = $1
        )
        SELECT
            (
                (
                    POWER(
                        1 + (
                            "apr" / 36500
                        ),
                        365
                    ) - 1
                ) * 100
            ) AS "PERCENT"
        FROM "APRCalc"
        "#;

        sqlx::query_as(&SQL)
            .bind(protocol)
            .bind(max_interest)
            .fetch_optional(&self.pool)
            .await
            .map(|maybe_row| {
                maybe_row.map_or_else(BigDecimal::zero, |(result,)| result)
            })
    }

    pub async fn get_earn_apr(
        &self,
        protocol: String,
    ) -> Result<BigDecimal, Error> {
        const SQL: &str = r#"
        WITH "Last_Hour_States" AS (
            SELECT *
            FROM "LS_State"
            WHERE "LS_timestamp" >= NOW() - INTERVAL '1 hour'
        ),
        "Last_Hour_Pool_State" AS (
            SELECT
                (
                    "LP_Pool_total_borrowed_stable" / NULLIF(
                        "LP_Pool_total_value_locked_stable",
                        0
                    )
                ) AS "utilization_rate"
            FROM "LP_Pool_State"
            WHERE "LP_Pool_id" = $1
            ORDER BY "LP_Pool_timestamp" DESC
            LIMIT 1
        ),
        "APRCalc" AS (
            SELECT
                (
                    (
                        (
                            AVG("o"."LS_interest") / 10.0
                        ) - 4
                    ) * (
                        SELECT
                            "utilization_rate"
                        FROM "Last_Hour_Pool_State"
                    )
                ) AS "apr"
            FROM "Last_Hour_States" AS "s"
            JOIN "LS_Opening" AS "o" ON "s"."LS_contract_id" = "o"."LS_contract_id"
            WHERE "o"."LS_loan_pool_id" = $1
        )
        SELECT
            (
                (
                    POWER(
                        (
                            1 + (
                                "apr" / 36500
                            )
                        ),
                        365
                    ) - 1
                ) * 100
            ) AS "PERCENT"
        FROM APRCalc       
        "#;

        sqlx::query_as(SQL)
            .bind(&protocol)
            .fetch_optional(&self.pool)
            .await
            .map(|maybe_row| {
                maybe_row.map_or_else(BigDecimal::zero, |(result,)| result)
            })
    }

    pub async fn get(
        &self,
        LS_contract_id: String,
    ) -> Result<Option<LS_Opening>, Error> {
        const SQL: &str = r#"
        SELECT *
        FROM "LS_Opening"
        WHERE "LS_contract_id" = $1
        "#;

        sqlx::query_as(SQL)
            .bind(LS_contract_id)
            .fetch_optional(&self.pool)
            .await
    }

    pub async fn get_borrowed(
        &self,
        protocol: String,
    ) -> Result<BigDecimal, Error> {
        const SQL: &str = r#"
        SELECT
            SUM(
                "LS_loan_amnt_asset" / 1000000
            ) AS "Loan"
        FROM "LS_Opening"
        WHERE "LS_loan_pool_id" = $1
        "#;

        sqlx::query_as(SQL)
            .bind(protocol)
            .fetch_optional(&self.pool)
            .await
            .map(|maybe_row| {
                maybe_row.map_or_else(BigDecimal::zero, |(result,)| result)
            })
    }

    pub async fn get_borrowed_total(&self) -> Result<BigDecimal, Error> {
        const SQL: &str = r#"
        SELECT
            SUM(
                "LS_loan_amnt_asset" / 1000000
            ) AS "Loan"
        FROM "LS_Opening"
        "#;

        sqlx::query_as(SQL)
            .fetch_optional(&self.pool)
            .await
            .map(|maybe_row| {
                maybe_row.map_or_else(BigDecimal::zero, |(result,)| result)
            })
    }

    pub async fn get_leases(
        &self,
        leases: Vec<&str>,
    ) -> Result<Vec<LS_Opening>, Error> {
        const SQL: &str = r#"
        SELECT *
        FROM "LS_Opening"
        WHERE "LS_contract_id" IN ( 
        "#;

        leases
            .iter()
            .fold(&mut QueryBuilder::new(SQL), QueryBuilder::push_bind)
            .push(")")
            .build_query_as()
            .persistent(false)
            .fetch_all(&self.pool)
            .await
    }

    pub async fn get_total_tx_value(&self) -> Result<BigDecimal, Error> {
        const SQL: &str = r#"
        WITH "Opened_Leases" AS (
            SELECT
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
                ) AS "Down Payment Amount",
                (
                    "LS_loan_amnt_asset" / (
                        CASE
                            WHEN "LS_loan_pool_id" = 'nolus1jufcaqm6657xmfltdezzz85quz92rmtd88jk5x0hq9zqseem32ysjdm990' THEN
                                1000000
                            WHEN "LS_loan_pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN
                                100000000
                            WHEN "LS_loan_pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN
                                1000000000
                            WHEN "LS_loan_pool_id" = 'nolus1lxr7f5xe02jq6cce4puk6540mtu9sg36at2dms5sk69wdtzdrg9qq0t67z' THEN
                                1000000
                            ELSE
                                1000000
                        END
                    )
                ) AS "Loan"
            FROM "LS_Opening"
        ),
        "LP_Deposits" AS (
            SELECT
                (
                    "LP_amnt_stable" / (
                        CASE
                            WHEN "LP_address_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN
                                100000000    -- Example for ALL_BTC or similar
                            WHEN "LP_address_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN
                                1000000000   -- Example for ALL_SOL
                            ELSE
                                1000000    -- Default divisor
                        END
                    )
                ) AS "Volume"
            FROM "LP_Deposit"
        ),
        "LP_Withdrawals" AS (
            SELECT
                (
                    "LP_amnt_stable" / (
                        CASE
                            WHEN "LP_address_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN
                                100000000    -- Example for ALL_BTC or similar
                            WHEN "LP_address_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN
                                1000000000   -- Example for ALL_SOL
                            ELSE
                                1000000    -- Default divisor
                        END
                    )
                ) AS "Volume"
            FROM "LP_Withdraw"
        ),
        "LS_Close" AS (
            SELECT
                (
                    "LS_payment_amnt_stable" / (
                        CASE
                            WHEN "LS_payment_symbol" IN ('ALL_BTC', 'WBTC', 'CRO') THEN
                                100000000
                            WHEN "LS_payment_symbol" IN ('ALL_SOL') THEN
                                1000000000
                            WHEN "LS_payment_symbol" IN ('PICA') THEN
                                1000000000000 
                            WHEN "LS_payment_symbol" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS') THEN
                                1000000000000000000
                            ELSE
                                1000000
                        END
                    )
                ) AS "Volume"
            FROM "LS_Close_Position"
        ),
        "LS_Repayment" AS (
            SELECT
                (
                    "LS_payment_amnt_stable" / (
                        CASE
                            WHEN "LS_payment_symbol" IN ('ALL_BTC', 'WBTC', 'CRO') THEN
                                100000000
                            WHEN "LS_payment_symbol" IN ('ALL_SOL') THEN
                                1000000000
                            WHEN "LS_payment_symbol" IN ('PICA') THEN
                                1000000000000 
                            WHEN "LS_payment_symbol" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS') THEN
                                1000000000000000000
                            ELSE
                                1000000
                        END
                    )
                ) AS "Volume"
            FROM "LS_Repayment"
        )
        SELECT
            SUM("Volume") AS "Tx Value"
        FROM (
            SELECT
                (
                    "Down Payment Amount" + "Loan"
                ) AS "Volume"
            FROM "Opened_Leases"
            UNION ALL
                SELECT
                    "Volume"
                FROM "LP_Deposits"
            UNION ALL 
                SELECT
                    "Volume"
                FROM "LP_Withdrawals"
            UNION ALL
                SELECT
                    "Volume"
                FROM "LS_Close"
            UNION ALL
                SELECT
                    "Volume"
                FROM "LS_Repayment"
        )
        "#;

        sqlx::query_as(SQL)
            .fetch_optional(&self.pool)
            .await
            .map(|maybe_row| {
                maybe_row.map_or_else(BigDecimal::zero, |(result,)| result)
            })
    }

    pub async fn get_max_ls_interest_7d(
        &self,
        lpp_address: String,
    ) -> Result<Vec<LS_Max_Interest>, Error> {
        const SQL: &str = r#"
        SELECT
            DATE("LS_timestamp") AS "date",
            MAX("LS_interest") AS "max_interest"
        FROM "LS_Opening"
        WHERE
            "LS_timestamp" >= (CURRENT_DATE - INTERVAL '7 days') AND
            "LS_loan_pool_id" = $1
        GROUP BY "date"
        ORDER BY "date" DESC
        "#;

        sqlx::query_as(SQL)
            .bind(lpp_address)
            .fetch_all(&self.pool)
            .await
    }

    pub async fn get_leases_by_address(
        &self,
        address: String,
        skip: i64,
        limit: i64,
    ) -> Result<Vec<LS_Opening>, Error> {
        const SQL: &str = r#"
        SELECT
            "a"."LS_contract_id",
            "a"."LS_address_id",
            "a"."LS_asset_symbol",
            "a"."LS_interest",
            "a"."LS_timestamp",
            "a"."LS_loan_pool_id",
            "a"."LS_loan_amnt_stable",
            "a"."LS_loan_amnt_asset",
            "a"."LS_cltr_symbol",
            "a"."LS_cltr_amnt_stable",
            "a"."LS_cltr_amnt_asset",
            "a"."LS_native_amnt_stable",
            "a"."LS_native_amnt_nolus",
            "a"."Tx_Hash",
            "a"."LS_loan_amnt",
            "a"."LS_lpn_loan_amnt"
        FROM "LS_Opening" AS "a"
        LEFT JOIN "LS_Closing" AS "b" ON "a"."LS_contract_id" = "b"."LS_contract_id" 
        WHERE "a"."LS_address_id" = $1
        ORDER BY "LS_timestamp" DESC
        OFFSET $2
        LIMIT $3
        "#;

        sqlx::query_as(SQL)
            .bind(address)
            .bind(skip)
            .bind(limit)
            .fetch_all(&self.pool)
            .await
    }

    //TODO: delete
    pub async fn get_leases_data(
        &self,
        leases: Vec<String>,
    ) -> Result<Vec<LS_Opening>, Error> {
        const SQL: &str = r#"
        SELECT *
        FROM "LS_Opening"
        WHERE "LS_contract_id" IN (
        "#;

        leases
            .iter()
            .fold(&mut QueryBuilder::new(SQL), QueryBuilder::push_bind)
            .push(")")
            .build_query_as()
            .fetch_all(&self.pool)
            .await
    }

    pub async fn update_ls_loan_amnt(
        &self,
        ls_opening: &LS_Opening,
    ) -> Result<(), Error> {
        const SQL: &str = r#"
        UPDATE "LS_Opening" 
        SET
            "LS_loan_amnt" = $1
        WHERE "LS_contract_id" = $2
        "#;

        sqlx::query(SQL)
            .bind(&ls_opening.LS_loan_amnt)
            .bind(&ls_opening.LS_contract_id)
            .execute(&self.pool)
            .await
            .map(drop)
    }

    pub async fn update_ls_lpn_loan_amnt(
        &self,
        ls_opening: &LS_Opening,
    ) -> Result<(), Error> {
        const SQL: &str = r#"
        UPDATE "LS_Opening" 
        SET
            "LS_lpn_loan_amnt" = $1
        WHERE "LS_contract_id" = $2
        "#;

        sqlx::query(SQL)
            .bind(&ls_opening.LS_lpn_loan_amnt)
            .bind(&ls_opening.LS_contract_id)
            .execute(&self.pool)
            .await
            .map(drop)
    }

    pub async fn get_lease_history(
        &self,
        contract_id: String,
    ) -> Result<Vec<LS_History>, Error> {
        const SQL: &str = r#"
        SELECT *
        FROM (
            SELECT
                "LS_payment_symbol" as "symbol",
                "LS_payment_amnt" as "amount",
                "LS_timestamp" as "time",
                'repay' as "type"
            FROM "LS_Repayment"
            WHERE "LS_contract_id" = $1
            UNION ALL
                SELECT
                    "LS_payment_symbol" as "symbol",
                    "LS_payment_amnt" as "amount",
                    "LS_timestamp" as "time",
                    'market-close' as "type"
                FROM "LS_Close_Position"
                WHERE "LS_contract_id" = $1
            UNION ALL
                SELECT
                    "LS_payment_symbol" as "symbol",
                    "LS_payment_amnt" as "amount",
                    "LS_timestamp" as "time",
                    'liquidation' as "type"
                FROM "LS_Liquidation"
                WHERE "LS_contract_id" = $1
        ) AS "combined_data"
        ORDER BY "time" ASC
        "#;

        sqlx::query_as(SQL)
            .bind(contract_id)
            .fetch_all(&self.pool)
            .await
    }

    pub async fn get_leases_monthly(
        &self,
    ) -> Result<Vec<Leases_Monthly>, Error> {
        const SQL: &str = r#"
        WITH "Historically_Opened_Base" AS (
            SELECT
                DISTINCT ON ("lso"."LS_contract_id")
                "lso"."LS_contract_id" AS "Contract ID",
                "lso"."LS_address_id" AS "User",
                (
                    CASE
                        WHEN "LS_loan_pool_id" = 'nolus1jufcaqm6657xmfltdezzz85quz92rmtd88jk5x0hq9zqseem32ysjdm990' THEN
                            'ST_ATOM'
                        WHEN "LS_loan_pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN
                            'ALL_BTC'
                        WHEN "LS_loan_pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN
                            'ALL_SOL'
                        WHEN "LS_loan_pool_id" = 'nolus1lxr7f5xe02jq6cce4puk6540mtu9sg36at2dms5sk69wdtzdrg9qq0t67z' THEN
                            'AKT'
                        ELSE
                            "LS_asset_symbol"
                    END
                ) AS "Leased Asset",
                DATE_TRUNC('month', "LS_timestamp") AS "Date",
                (
                    CASE
                        WHEN "LS_cltr_symbol" IN ('ALL_BTC', 'WBTC', 'CRO') THEN
                            "LS_cltr_amnt_stable" / 100000000
                        WHEN "LS_cltr_symbol" IN ('ALL_SOL') THEN
                            "LS_cltr_amnt_stable" / 1000000000
                        WHEN "LS_cltr_symbol" IN ('PICA') THEN
                            "LS_cltr_amnt_stable" / 1000000000000
                        WHEN "LS_cltr_symbol" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS') THEN
                            "LS_cltr_amnt_stable" / 1000000000000000000
                        ELSE
                            "LS_cltr_amnt_stable" / 1000000
                    END
                ) AS "Down Payment Amount",
                (
                    CASE
                        WHEN "LS_loan_pool_id" = 'nolus1jufcaqm6657xmfltdezzz85quz92rmtd88jk5x0hq9zqseem32ysjdm990' THEN
                            "LS_loan_amnt_stable" / 1000000
                        WHEN "LS_loan_pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN
                            "LS_loan_amnt_stable" / 100000000
                        WHEN "LS_loan_pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN
                            "LS_loan_amnt_stable" / 1000000000
                        WHEN "LS_loan_pool_id" = 'nolus1lxr7f5xe02jq6cce4puk6540mtu9sg36at2dms5sk69wdtzdrg9qq0t67z' THEN
                            "LS_loan_amnt_stable" / 1000000
                        ELSE
                            "LS_loan_amnt_asset" / 1000000
                    END
                ) AS "Loan Amount"
            FROM "LS_Opening" AS "lso"
        )
        SELECT
            "Date",
            (
                SUM("Down Payment Amount") + SUM("Loan Amount")
            ) AS "Amount"
        FROM "Historically_Opened_Base"
        GROUP BY "Date"
        ORDER BY "Date" DESC
        "#;

        sqlx::query_as(SQL).fetch_all(&self.pool).await
    }
}
