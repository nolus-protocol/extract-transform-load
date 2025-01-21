use std::iter;

use chrono::{DateTime, Utc};
use sqlx::{error::Error, types::BigDecimal, QueryBuilder, Transaction};

use crate::{
    custom_uint::{UInt31, UInt63},
    model::{Borrow_APR, LS_History, LS_Opening, Leased_Asset, Table},
    types::LS_Max_Interest,
};

use super::DataBase;

impl Table<LS_Opening> {
    pub async fn isExists(
        &self,
        LS_Opening { LS_contract_id, .. }: &LS_Opening,
    ) -> Result<bool, Error> {
        const SQL: &str = r#"
        SELECT COUNT(1)
        FROM "LS_Opening"
        WHERE "LS_contract_id" = $1
        "#;

        sqlx::query_as(SQL)
            .bind(LS_contract_id)
            .fetch_one(&self.pool)
            .await
            .map(|(result,)| result)
    }

    pub async fn insert(
        &self,
        LS_Opening {
            LS_contract_id,
            LS_address_id,
            LS_asset_symbol,
            LS_interest,
            LS_timestamp,
            LS_loan_pool_id,
            LS_loan_amnt,
            LS_loan_amnt_stable,
            LS_loan_amnt_asset,
            LS_cltr_symbol,
            LS_cltr_amnt_stable,
            LS_cltr_amnt_asset,
            LS_native_amnt_stable,
            LS_native_amnt_nolus,
            LS_lpn_loan_amnt,
            Tx_Hash,
        }: &LS_Opening,
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
            "LS_loan_amnt",
            "LS_loan_amnt_stable",
            "LS_loan_amnt_asset",
            "LS_cltr_symbol",
            "LS_cltr_amnt_stable",
            "LS_cltr_amnt_asset",
            "LS_native_amnt_stable",
            "LS_native_amnt_nolus",
            "LS_lpn_loan_amnt",
            "Tx_Hash"
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
        "#;

        sqlx::query(SQL)
            .bind(LS_contract_id)
            .bind(LS_address_id)
            .bind(LS_asset_symbol)
            .bind(LS_interest)
            .bind(LS_timestamp)
            .bind(LS_loan_pool_id)
            .bind(LS_loan_amnt)
            .bind(LS_loan_amnt_stable)
            .bind(LS_loan_amnt_asset)
            .bind(LS_cltr_symbol)
            .bind(LS_cltr_amnt_stable)
            .bind(LS_cltr_amnt_asset)
            .bind(LS_native_amnt_stable)
            .bind(LS_native_amnt_nolus)
            .bind(LS_lpn_loan_amnt)
            .bind(Tx_Hash)
            .execute(&mut **transaction)
            .await
            .map(drop)
    }

    pub async fn insert_many<'r, T>(
        &self,
        data: T,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<(), Error>
    where
        T: IntoIterator<Item = &'r LS_Opening>,
    {
        const SQL: &str = r#"
        INSERT INTO "LS_Opening" (
            "LS_contract_id",
            "LS_address_id",
            "LS_asset_symbol",
            "LS_interest",
            "LS_timestamp",
            "LS_loan_pool_id",
            "LS_loan_amnt",
            "LS_loan_amnt_stable",
            "LS_loan_amnt_asset",
            "LS_cltr_symbol",
            "LS_cltr_amnt_stable",
            "LS_cltr_amnt_asset",
            "LS_native_amnt_stable",
            "LS_native_amnt_nolus",
            "LS_lpn_loan_amnt",
            "Tx_Hash"
        )
        "#;

        let mut iter = data.into_iter();

        let Some(first) = iter.next() else {
            return Ok(());
        };

        QueryBuilder::new(SQL)
            .push_values(
                iter::once(first).chain(iter),
                |mut b,
                 &LS_Opening {
                     ref LS_contract_id,
                     ref LS_address_id,
                     ref LS_asset_symbol,
                     LS_interest,
                     LS_timestamp,
                     ref LS_loan_pool_id,
                     ref LS_loan_amnt,
                     ref LS_loan_amnt_stable,
                     ref LS_loan_amnt_asset,
                     ref LS_cltr_symbol,
                     ref LS_cltr_amnt_stable,
                     ref LS_cltr_amnt_asset,
                     ref LS_native_amnt_stable,
                     ref LS_native_amnt_nolus,
                     ref LS_lpn_loan_amnt,
                     ref Tx_Hash,
                 }| {
                    b.push_bind(LS_contract_id)
                        .push_bind(LS_address_id)
                        .push_bind(LS_asset_symbol)
                        .push_bind(LS_interest)
                        .push_bind(LS_timestamp)
                        .push_bind(LS_loan_pool_id)
                        .push_bind(LS_loan_amnt)
                        .push_bind(LS_loan_amnt_stable)
                        .push_bind(LS_loan_amnt_asset)
                        .push_bind(LS_cltr_symbol)
                        .push_bind(LS_cltr_amnt_stable)
                        .push_bind(LS_cltr_amnt_asset)
                        .push_bind(LS_native_amnt_stable)
                        .push_bind(LS_native_amnt_nolus)
                        .push_bind(LS_lpn_loan_amnt)
                        .push_bind(Tx_Hash);
                },
            )
            .build()
            .execute(&mut **transaction)
            .await
            .map(drop)
    }

    pub async fn count(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<UInt63, Error> {
        const SQL: &str = r#"
        SELECT COUNT(1)
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
        SELECT COALESCE(SUM("LS_cltr_amnt_stable"), 0)
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
            .map(|(amnt,)| amnt)
    }

    pub async fn get_loan_amnt_stable_sum(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<BigDecimal, Error> {
        const SQL: &str = r#"
        SELECT COALESCE(SUM("LS_loan_amnt_stable"), 0)
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
            .map(|(amnt,)| amnt)
    }

    pub async fn get_ls_cltr_amnt_stable_sum(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<BigDecimal, Error> {
        const SQL: &str = r#"
        SELECT COALESCE(SUM("LS_cltr_amnt_stable"), 0)
        FROM "LS_Opening"
        LEFT JOIN "LS_Closing" ON "LS_Opening"."LS_contract_id" = "LS_Closing"."LS_contract_id"
        WHERE
            "LS_Closing"."LS_timestamp" > $1 AND
            "LS_Closing"."LS_timestamp" <= $2
        "#;

        sqlx::query_as(SQL)
            .bind(from)
            .bind(to)
            .fetch_one(&self.pool)
            .await
            .map(|(amnt,)| amnt)
    }

    pub async fn get_ls_amnt_stable_sum(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<BigDecimal, Error> {
        const SQL: &str = r#"
        SELECT COALESCE(SUM("LS_loan_amnt_stable" + "LS_cltr_amnt_stable"), 0)
        FROM "LS_Opening"
        LEFT JOIN "LS_Closing" ON "LS_Opening"."LS_contract_id" = "LS_Closing"."LS_contract_id"
        WHERE
            "LS_Closing"."LS_timestamp" > $1 AND
            "LS_Closing"."LS_timestamp" <= $2
        "#;

        sqlx::query_as(SQL)
            .bind(from)
            .bind(to)
            .fetch_one(&self.pool)
            .await
            .map(|(amnt,)| amnt)
    }

    pub async fn get_borrow_apr(
        &self,
        protocol: &str,
        skip: UInt63,
        limit: UInt63,
    ) -> Result<Vec<Borrow_APR>, Error> {
        const SQL: &str = r#"
        SELECT "LS_interest" / 10.0 AS "APR"
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
        protocol: &str,
    ) -> Result<Vec<Leased_Asset>, Error> {
        const SQL: &str = r#"
        SELECT
            "LS_asset_symbol" AS "Asset",
            SUM("LS_loan_amnt_asset" / 1000000) AS "Loan"
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
        SELECT
            "LS_asset_symbol" AS "Asset",
            SUM("LS_loan_amnt_asset" / 1000000) AS "Loan"
        FROM "LS_Opening"
        GROUP BY "Asset"
        "#;

        sqlx::query_as(SQL).fetch_all(&self.pool).await
    }

    pub async fn get_earn_apr(
        &self,
        protocol: &str,
    ) -> Result<BigDecimal, Error> {
        const SQL: &str = r#"
        WITH "DateRange" AS (
            SELECT
                generate_series(
                    CURRENT_DATE - INTERVAL '7 days',
                    CURRENT_DATE,
                    '1 day'
                )::date AS "date"
        ),
        "Pool_State_Interest" AS (
            SELECT
                "LP_Pool_timestamp",
                CASE
                    WHEN "LP_Pool_total_borrowed_stable"/"LP_Pool_total_value_locked_stable" < 0.7 THEN (12 + (("LP_Pool_total_borrowed_stable"/"LP_Pool_total_value_locked_stable") / (1 - ("LP_Pool_total_borrowed_stable"/"LP_Pool_total_value_locked_stable")) / 0.7)*2) * 10
                    ELSE 186
                END AS "interest"
            FROM "LP_Pool_State"
            WHERE
                "LP_Pool_timestamp" >= CURRENT_DATE - INTERVAL '7 days' AND
                "LP_Pool_id" = $1
            ORDER BY "LP_Pool_timestamp" DESC
        ),
        "DailyInterest" AS (
            SELECT
                DATE("LP_Pool_timestamp") as "date",
                MAX("interest") AS "max_interest"
            FROM "Pool_State_Interest"
            GROUP BY "LP_Pool_timestamp"
        ),
        "MaxLSInterest" AS (
            SELECT
                "dr"."date",
                COALESCE(
                    "di"."max_interest",
                    FIRST_VALUE("di"."max_interest") OVER (
                        ORDER BY "dr"."date"
                        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                    )
                ) AS max_interest
            FROM "DateRange" AS "dr"
            LEFT JOIN "DailyInterest" AS "di" ON "dr"."date" = "di"."date"
        ),
        "MaxLPRatio" AS (
            SELECT
                DATE("LP_Pool_timestamp") AS "date",
                ("LP_Pool_total_borrowed_stable" / "LP_Pool_total_value_locked_stable") AS "ratio"
            FROM (
                SELECT
                    *,
                    RANK() OVER (
                        PARTITION BY DATE("LP_Pool_timestamp")
                        ORDER BY ("LP_Pool_total_borrowed_stable" / "LP_Pool_total_value_locked_stable") DESC
                    ) AS "rank"
                FROM "LP_Pool_State"
                WHERE
                    "LP_Pool_timestamp" >= CURRENT_DATE - INTERVAL '7 days' AND
                    "LP_Pool_id" = $1
            ) AS "ranked"
            WHERE "ranked"."rank" = 1
        ),
        APRCalc AS (
            SELECT AVG(("mli"."max_interest" - 40) * "mlr"."ratio") / 10 AS "Earn APR"
            FROM "MaxLSInterest" AS "mli"
            JOIN "MaxLPRatio" AS "mlr" ON "mli"."date" = "mlr"."date"
        )
        SELECT COALESCE(
            (
                SELECT POWER((1 + ("Earn APR" / 100 / 365)), 365) - 1) * 100
                FROM "APRCalc"
            ),
            0
        ) AS "Earn APY"
        "#;

        sqlx::query_as(SQL)
            .bind(&protocol)
            .fetch_one(&self.pool)
            .await
            .map(|(amnt,)| amnt)
    }

    pub async fn get_earn_apr_interest(
        &self,
        protocol: &str,
        max_interest: UInt31,
    ) -> Result<BigDecimal, Error> {
        const SQL: &str = r#"
        WITH "DateRange" AS (
            SELECT
                generate_series(
                    CURRENT_DATE - INTERVAL '7 days',
                    CURRENT_DATE,
                    '1 day'
                )::date AS "date"
        ),
        "DailyInterest" AS (
            SELECT
                DATE("LS_timestamp") AS "date",
                MAX("LS_interest") AS "max_interest"
            FROM "LS_Opening"
            WHERE
                "LS_timestamp" >= CURRENT_DATE - INTERVAL '7 days' AND
                "LS_loan_pool_id" = $1
            GROUP BY DATE("LS_timestamp")
        ),
        MaxLSInterest AS (
            SELECT
                "dr"."date",
                COALESCE(
                    "di"."max_interest",
                    FIRST_VALUE("di"."max_interest") OVER (
                        ORDER BY "dr"."date"
                        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                    )
                ) AS "max_interest"
            FROM "DateRange" AS "dr"
            LEFT JOIN "DailyInterest" AS "di" ON "dr"."date" = "di"."date"
        ),
        MaxLPRatio AS (
            SELECT
                DATE("LP_Pool_timestamp") AS "date",
                ("LP_Pool_total_borrowed_stable" / "LP_Pool_total_value_locked_stable") AS "ratio"
            FROM (
                SELECT
                    *,
                    RANK() OVER (
                        PARTITION BY DATE("LP_Pool_timestamp")
                        ORDER BY ("LP_Pool_total_borrowed_stable" / "LP_Pool_total_value_locked_stable") DESC
                    ) AS "rank"
                FROM "LP_Pool_State"
                WHERE
                    "LP_Pool_timestamp" >= CURRENT_DATE - INTERVAL '7 days' AND
                    "LP_Pool_id" = $1
            ) AS "ranked"
            WHERE "ranked"."rank" = 1
        ),
        "APRCalc" AS (
            SELECT AVG(("mli"."max_interest" - $2) * mlr.ratio) / 10 AS "allBTC APR"
            FROM "MaxLSInterest" AS "mli"
            JOIN "MaxLPRatio" AS "mlr" ON "mli"."date" = "mlr"."date"
        )
        SELECT COALESCE(
            (
                SELECT (POWER((1 + ("allBTC APR" / 100 / 365)), 365) - 1) * 100 AS "ALL_BTC_OSMOSIS"
                FROM "APRCalc"
            ),
            0
        )
        "#;

        sqlx::query_as(SQL)
            .bind(&protocol)
            .bind(&max_interest)
            .fetch_one(&self.pool)
            .await
            .map(|(amnt,)| amnt)
    }

    pub async fn get(
        &self,
        LS_contract_id: &str,
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
        protocol: &str,
    ) -> Result<BigDecimal, Error> {
        const SQL: &str = r#"
        SELECT COALESCE(SUM("LS_loan_amnt_asset" / 1000000), 0) AS "Loan"
        FROM "LS_Opening"
        WHERE "LS_loan_pool_id" = $1
        "#;

        sqlx::query_as(SQL)
            .bind(protocol)
            .fetch_one(&self.pool)
            .await
            .map(|(amnt,)| amnt)
    }

    pub async fn get_borrowed_total(&self) -> Result<BigDecimal, Error> {
        const SQL: &str = r#"
        SELECT COALESCE(SUM("LS_loan_amnt_asset" / 1000000), 0) AS "Loan"
        FROM "LS_Opening"
        "#;

        sqlx::query_as(SQL)
            .fetch_one(&self.pool)
            .await
            .map(|(amnt,)| amnt)
    }

    pub async fn get_leases<'r, I, T>(
        &self,
        leases: I,
    ) -> Result<Vec<LS_Opening>, Error>
    where
        I: IntoIterator<Item = &'r T>,
        T: AsRef<str> + ?Sized + 'r,
    {
        const SQL: &str = r#"
        SELECT *
        FROM "LS_Opening"
        WHERE "LS_contract_id" IN (
        "#;

        let mut iter = leases.into_iter();

        let Some(first) = iter.next() else {
            return Ok(vec![]);
        };

        iter::once(first)
            .chain(iter)
            .into_iter()
            .map(AsRef::as_ref)
            .fold(&mut QueryBuilder::new(SQL), QueryBuilder::push_bind)
            .push(")")
            .build_query_as()
            .fetch_all(&self.pool)
            .await
    }

    pub async fn get_total_tx_value(&self) -> Result<BigDecimal, Error> {
        const SQL: &str = r#"
        WITH "Opened_Leases" AS (
            SELECT
                (
                    CASE
                        WHEN "LS_cltr_symbol" IN ('ALL_BTC', 'WBTC', 'CRO') THEN "LS_cltr_amnt_stable" / 100000000
                        WHEN "LS_cltr_symbol" IN ('ALL_SOL') THEN "LS_cltr_amnt_stable" / 1000000000
                        WHEN "LS_cltr_symbol" IN ('PICA') THEN "LS_cltr_amnt_stable" / 1000000000000
                        WHEN "LS_cltr_symbol" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS') THEN "LS_cltr_amnt_stable" / 1000000000000000000
                        ELSE "LS_cltr_amnt_stable" / 1000000
                    END
                ) AS "Down Payment Amount",
                (
                    CASE
                        WHEN "LS_loan_pool_id" = 'nolus1jufcaqm6657xmfltdezzz85quz92rmtd88jk5x0hq9zqseem32ysjdm990' THEN "LS_loan_amnt_stable" / 1000000
                        WHEN "LS_loan_pool_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN "LS_loan_amnt_stable" / 100000000
                        WHEN "LS_loan_pool_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN "LS_loan_amnt_stable" / 1000000000
                        WHEN "LS_loan_pool_id" = 'nolus1lxr7f5xe02jq6cce4puk6540mtu9sg36at2dms5sk69wdtzdrg9qq0t67z' THEN "LS_loan_amnt_stable" / 1000000
                        ELSE "LS_loan_amnt_asset" / 1000000
                    END
                ) AS "Loan"
            FROM "LS_Opening"
        ),
        "LP_Deposits" AS (
            SELECT
                (
                    CASE
                        WHEN "LP_address_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN "LP_amnt_stable" / 100000000
                        WHEN "LP_address_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN "LP_amnt_stable" / 1000000000
                        ELSE "LP_amnt_stable" / 1000000
                    END
                ) AS "Volume"
            FROM "LP_Deposit"
        ),
        LP_Withdrawals AS (
            SELECT
                (
                    CASE
                        WHEN "LP_address_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN "LP_amnt_stable" / 100000000
                        WHEN "LP_address_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN "LP_amnt_stable" / 1000000000
                        ELSE "LP_amnt_stable" / 1000000
                    END
                ) AS "Volume"
            FROM "LP_Withdraw"
        ),
        "LS_Close" AS (
            SELECT
                (
                    CASE
                        WHEN "LS_payment_symbol" IN ('ALL_BTC', 'WBTC', 'CRO') THEN "LS_payment_amnt_stable" / 100000000
                        WHEN "LS_payment_symbol" IN ('ALL_SOL') THEN "LS_payment_amnt_stable" / 1000000000
                        WHEN "LS_payment_symbol" IN ('PICA') THEN "LS_payment_amnt_stable" / 1000000000000
                        WHEN "LS_payment_symbol" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS') THEN "LS_payment_amnt_stable" / 1000000000000000000
                        ELSE "LS_payment_amnt_stable" / 1000000
                    END
                ) AS "Volume"
            FROM "LS_Close_Position"
        ),
        "LS_Repayment" AS (
            SELECT
                (
                    CASE
                        WHEN "LS_payment_symbol" IN ('ALL_BTC', 'WBTC', 'CRO') THEN "LS_payment_amnt_stable" / 100000000
                        WHEN "LS_payment_symbol" IN ('ALL_SOL') THEN "LS_payment_amnt_stable" / 1000000000
                        WHEN "LS_payment_symbol" IN ('PICA') THEN "LS_payment_amnt_stable" / 1000000000000
                        WHEN "LS_payment_symbol" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS') THEN "LS_payment_amnt_stable" / 1000000000000000000
                        ELSE "LS_payment_amnt_stable" / 1000000
                    END
                ) AS "Volume"
            FROM "LS_Repayment"
        )
        SELECT COALESCE(SUM("Volume"), 0) AS "Tx Value"
        FROM (
            SELECT ("Down Payment Amount" + "Loan") AS "Volume"
            FROM "Opened_Leases"
            UNION ALL
                SELECT "Volume"
                FROM "LP_Deposits"
            UNION ALL
                SELECT "Volume"
                FROM "LP_Withdrawals"
            UNION ALL
                SELECT "Volume"
                FROM "LS_Close"
            UNION ALL
                SELECT "Volume"
                FROM "LS_Repayment"
        ) AS "combined_data"
        "#;

        sqlx::query_as(SQL)
            .fetch_one(&self.pool)
            .await
            .map(|(amnt,)| amnt)
    }

    pub async fn get_max_ls_interest_7d(
        &self,
        lpp_address: &str,
    ) -> Result<Vec<LS_Max_Interest>, Error> {
        const SQL: &str = r#"
        SELECT
            DATE("LS_timestamp") AS "date",
            MAX("LS_interest") AS "max_interest"
        FROM "LS_Opening"
        WHERE
            "LS_timestamp" >= CURRENT_DATE - INTERVAL '7 days' AND
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
        address: &str,
        skip: UInt63,
        limit: UInt63,
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
        LEFT JOIN "LS_Closing" AS "b" ON a."LS_contract_id" = b."LS_contract_id"
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
    pub async fn get_leases_data<'r, I, T>(
        &self,
        leases: I,
    ) -> Result<Vec<LS_Opening>, Error>
    where
        I: IntoIterator<Item = &'r T>,
        T: AsRef<str>,
    {
        const SQL: &str = r#"
        SELECT *
        FROM "LS_Opening"
        WHERE "LS_contract_id" IN (
        "#;

        let mut iter = leases.into_iter();

        let Some(first) = iter.next() else {
            return Ok(vec![]);
        };

        iter::once(first)
            .chain(iter)
            .into_iter()
            .map(AsRef::as_ref)
            .fold(&mut QueryBuilder::new(SQL), QueryBuilder::push_bind)
            .push(")")
            .build_query_as()
            .fetch_all(&self.pool)
            .await
    }

    pub async fn update_ls_loan_amount(
        &self,
        contract_id: &str,
        loan_amount: &BigDecimal,
    ) -> Result<(), Error> {
        const SQL: &str = r#"
        UPDATE "LS_Opening"
        WHERE "LS_contract_id" = $1
        SET "LS_loan_amnt" = $2
        "#;

        sqlx::query(SQL)
            .bind(contract_id)
            .bind(loan_amount)
            .execute(&self.pool)
            .await
            .map(drop)
    }

    pub async fn update_ls_lpn_loan_amount(
        &self,
        contract_id: &str,
        lpn_loan_amount: &BigDecimal,
    ) -> Result<(), Error> {
        const SQL: &str = r#"
        UPDATE "LS_Opening"
        WHERE "LS_contract_id" = $1
        SET "LS_lpn_loan_amnt" = $2
        "#;

        sqlx::query(SQL)
            .bind(contract_id)
            .bind(lpn_loan_amount)
            .execute(&self.pool)
            .await
            .map(drop)
    }

    pub async fn get_lease_history(
        &self,
        contract_id: &str,
    ) -> Result<Vec<LS_History>, Error> {
        const SQL: &str = r#"
        SELECT *
        FROM (
            SELECT
                "LS_payment_symbol" AS "symbol",
                "LS_payment_amnt" AS "amount",
                "LS_timestamp" AS "time",
                'repay' AS "type"
            FROM "LS_Repayment"
            WHERE "LS_contract_id" = $1
            UNION ALL
                SELECT
                    "LS_payment_symbol" AS "symbol",
                    "LS_payment_amnt" AS "amount",
                    "LS_timestamp" AS "time",
                    'market-close' AS "type"
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
}
