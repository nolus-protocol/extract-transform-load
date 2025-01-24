use bigdecimal::Zero as _;
use chrono::{DateTime, Utc};
use sqlx::{types::BigDecimal, Error, QueryBuilder, Transaction};

use crate::{
    model::{Borrow_APR, LS_History, LS_Opening, Leased_Asset, Table},
    types::LS_Max_Interest,
};

use super::DataBase;

impl Table<LS_Opening> {
    pub async fn isExists(
        &self,
        ls_opening: &LS_Opening,
    ) -> Result<bool, Error> {
        const SQL: &str = r#"
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
            .execute(&mut **transaction)
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
            .execute(&mut **transaction)
            .await
            .map(drop)
    }

    pub async fn count(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<i64, Error> {
        const SQL: &str = r#"
        SELECT COUNT(*)
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
        SELECT SUM("LS_cltr_amnt_stable")
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
            .map(|result| {
                result.map_or_else(BigDecimal::zero, |(result,)| result)
            })
    }

    pub async fn get_loan_amnt_stable_sum(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<BigDecimal, Error> {
        const SQL: &str = r#"
        SELECT SUM("LS_loan_amnt_stable")
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
            .map(|result| {
                result.map_or_else(BigDecimal::zero, |(result,)| result)
            })
    }

    pub async fn get_ls_cltr_amnt_stable_sum(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<BigDecimal, Error> {
        const SQL: &str = r#"
        SELECT SUM("LS_cltr_amnt_stable")
        FROM "LS_Opening" AS "opening"
        LEFT JOIN "LS_Closing" AS "closing" ON "opening"."LS_contract_id" = "closing"."LS_contract_id"
        WHERE "closing"."LS_timestamp" > $1 AND "closing"."LS_timestamp" <= $2
        "#;

        sqlx::query_as(SQL)
            .bind(from)
            .bind(to)
            .fetch_optional(&self.pool)
            .await
            .map(|result| {
                result.map_or_else(BigDecimal::zero, |(result,)| result)
            })
    }

    pub async fn get_ls_amnt_stable_sum(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<BigDecimal, Error> {
        const SQL: &str = r#"
        SELECT SUM("LS_loan_amnt_stable" + "LS_cltr_amnt_stable")
        FROM "LS_Opening" AS "opening"
        LEFT JOIN "LS_Closing" AS "closing" ON "opening"."LS_contract_id" = "closing"."LS_contract_id"
        WHERE
            "closing"."LS_timestamp" > $1 AND
            "closing"."LS_timestamp" <= $2
        "#;

        sqlx::query_as(SQL)
            .bind(from)
            .bind(to)
            .fetch_optional(&self.pool)
            .await
            .map(|result| {
                result.map_or_else(BigDecimal::zero, |(result,)| result)
            })
    }

    pub async fn get_borrow_apr(
        &self,
        protocol: String,
        skip: i64,
        limit: i64,
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
        protocol: String,
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
        protocol: String,
    ) -> Result<BigDecimal, Error> {
        const SQL: &str = r#"
        WITH "DateRange" AS (
            SELECT generate_series(
                CURRENT_DATE - INTERVAL('7 days'),
                CURRENT_DATE,
                '1 day'
            )::date AS "date"
        ),
        "Pool_State_Interest" AS (
            SELECT
                "LP_Pool_timestamp",
                (
                    CASE
                        WHEN "LP_Pool_total_borrowed_stable" / "LP_Pool_total_value_locked_stable" < 0.7 THEN (12 + (("LP_Pool_total_borrowed_stable" / "LP_Pool_total_value_locked_stable") / (1 - ("LP_Pool_total_borrowed_stable" / "LP_Pool_total_value_locked_stable")) / 0.7) * 2) * 10
                        ELSE 186
                    END
                ) AS "interest"
            FROM "LP_Pool_State"
            WHERE
                "LP_Pool_timestamp" >= CURRENT_DATE - INTERVAL('7 days') AND
                "LP_Pool_id" = $1
            ORDER BY "LP_Pool_timestamp" DESC
        ),
        "DailyInterest" AS (
            SELECT
                DATE("LP_Pool_timestamp") AS "date",
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
                        ORDER BY
                            "dr"."date" ROWS BETWEEN UNBOUNDED PRECEDING
                            AND 1 PRECEDING
                    )
                ) AS "max_interest"
            FROM "DateRange" AS "dr"
            LEFT JOIN "DailyInterest" AS "di" ON "dr"."date" = "di"."date"
        ),
        "MaxLPRatio" AS (
            SELECT
                DATE("LP_Pool_timestamp") AS "date",
                "LP_Pool_total_borrowed_stable" / "LP_Pool_total_value_locked_stable" AS ratio
            FROM
                (
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
                ) "ranked"
            WHERE "ranked"."rank" = 1
        ),
        "APRCalc" AS (
            SELECT AVG(("mli"."max_interest" - 40) * "mlr"."ratio") / 10 AS "Earn APR"
            FROM "MaxLSInterest" AS "mli"
            INNER JOIN "MaxLPRatio" AS "mlr" ON "mli"."date" = "mlr"."date"
        )
        SELECT COALESCE(
            (
                POWER(
                    (1 + ("Earn APR" / 36500)),
                    365
                ) - 1
            ) * 100,
            0
        ) AS "Earn APY"
        FROM "APRCalc"
        "#;

        sqlx::query_as(SQL)
            .bind(&protocol)
            .fetch_optional(&self.pool)
            .await
            .map(|result| {
                result.map_or_else(BigDecimal::zero, |(result,)| result)
            })
    }

    pub async fn get_earn_apr_interest(
        &self,
        protocol: String,
        max_interest: i32,
    ) -> Result<BigDecimal, Error> {
        const SQL: &str = r#"
        WITH "DateRange" AS (
            SELECT generate_series(
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
        "MaxLSInterest" AS (
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
        "MaxLPRatio" AS (
            SELECT
                DATE("LP_Pool_timestamp") AS date,
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
            SELECT AVG(("mli"."max_interest" - $2) * "mlr"."ratio") / 10 AS "allBTC APR"
            FROM "MaxLSInterest" AS "mli"
            JOIN "MaxLPRatio" AS "mlr" ON "mli"."date" = "mlr"."date"
        )
        SELECT (POWER((1 + ("allBTC APR" / 36500)), 365) - 1) * 100 AS "ALL_BTC_OSMOSIS"
        FROM "APRCalc"
        "#;

        sqlx::query_as(SQL)
            .bind(&protocol)
            .bind(max_interest)
            .fetch_optional(&self.pool)
            .await
            .map(|result| {
                result.map_or_else(BigDecimal::zero, |(result,)| result)
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
        SELECT SUM("LS_loan_amnt_asset" / 1000000) AS "Loan"
        FROM "LS_Opening"
        WHERE "LS_loan_pool_id" = $1
        "#;

        sqlx::query_as(SQL)
            .bind(protocol)
            .fetch_optional(&self.pool)
            .await
            .map(|result| {
                result.map_or_else(BigDecimal::zero, |(result,)| result)
            })
    }

    pub async fn get_borrowed_total(&self) -> Result<BigDecimal, Error> {
        const SQL: &str = r#"
        SELECT SUM("LS_loan_amnt_asset" / 1000000) AS "Loan"
        FROM "LS_Opening"
        "#;

        sqlx::query_as(SQL)
            .fetch_optional(&self.pool)
            .await
            .map(|result| {
                result.map_or_else(BigDecimal::zero, |(result,)| result)
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
            .into_iter()
            .fold(&mut QueryBuilder::new(SQL), QueryBuilder::push_bind)
            .push(")")
            .build_query_as()
            .fetch_all(&self.pool)
            .await
    }

    pub async fn get_total_tx_value(&self) -> Result<BigDecimal, Error> {
        const SQL: &str = r#"
        WITH Opened_Leases AS (
            SELECT
                "LS_cltr_amnt_stable" / (
                    CASE
                        WHEN "LS_cltr_symbol" IN ('ALL_BTC', 'WBTC', 'CRO') THEN 100000000
                        WHEN "LS_cltr_symbol" IN ('ALL_SOL') THEN 1000000000
                        WHEN "LS_cltr_symbol" IN ('PICA') THEN 1000000000000
                        WHEN "LS_cltr_symbol" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS') THEN 1000000000000000000
                        ELSE 1000000
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
        LP_Deposits AS (
            SELECT "LP_amnt_stable" / (
                CASE
                    WHEN "LP_address_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN 100000000
                    WHEN "LP_address_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN 1000000000
                    ELSE 1000000
                END
            ) AS "Volume"
            FROM "LP_Deposit"
        ),
        LP_Withdrawals AS (
            SELECT "LP_amnt_stable" / (
                CASE
                    WHEN "LP_address_id" = 'nolus1w2yz345pqheuk85f0rj687q6ny79vlj9sd6kxwwex696act6qgkqfz7jy3' THEN 100000000
                    WHEN "LP_address_id" = 'nolus1qufnnuwj0dcerhkhuxefda6h5m24e64v2hfp9pac5lglwclxz9dsva77wm' THEN 1000000000
                    ELSE 1000000
                END
            ) AS "Volume"
            FROM "LP_Withdraw"
        ),
        LS_Close AS (
            SELECT "LS_payment_amnt_stable" / (
                CASE
                    WHEN "LS_payment_symbol" IN ('ALL_BTC', 'WBTC', 'CRO') THEN 100000000
                    WHEN "LS_payment_symbol" IN ('ALL_SOL') THEN 1000000000
                    WHEN "LS_payment_symbol" IN ('PICA') THEN 1000000000000
                    WHEN "LS_payment_symbol" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS') THEN 1000000000000000000
                    ELSE 1000000
                END
            ) AS "Volume"
            FROM "LS_Close_Position"
        ),
        LS_Repayment AS (
            SELECT "LS_payment_amnt_stable" / (
                CASE
                    WHEN "LS_payment_symbol" IN ('ALL_BTC', 'WBTC', 'CRO') THEN 100000000
                    WHEN "LS_payment_symbol" IN ('ALL_SOL') THEN 1000000000
                    WHEN "LS_payment_symbol" IN ('PICA') THEN 1000000000000
                    WHEN "LS_payment_symbol" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS') THEN 1000000000000000000
                    ELSE 1000000
                END
            ) AS "Volume"
            FROM "LS_Repayment"
        )
        SELECT SUM("Volume") AS "Tx Value"
        FROM (
            SELECT ("Down Payment Amount" + "Loan") AS "Volume"
            FROM Opened_Leases
            UNION ALL
            SELECT "Volume"
            FROM LP_Deposits
            UNION ALL
            SELECT "Volume"
            FROM LP_Withdrawals
            UNION ALL
            SELECT "Volume"
            FROM LS_Close
            UNION ALL
            SELECT "Volume"
            FROM LS_Repayment
        )
        "#;

        sqlx::query_as(SQL)
            .fetch_optional(&self.pool)
            .await
            .map(|result| {
                result.map_or_else(BigDecimal::zero, |(result,)| result)
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
            "LS_timestamp" >= CURRENT_DATE - INTERVAL('7 days') AND
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
        SELECT "a".*
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
        const PRE_BINDS_SQL: &str = r#"
        SELECT *
        FROM "LS_Opening"
        WHERE "LS_contract_id" IN (
        "#;

        leases
            .into_iter()
            .fold(
                &mut QueryBuilder::new(PRE_BINDS_SQL),
                QueryBuilder::push_bind,
            )
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
        SET "LS_loan_amnt" = $1
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
        SET "LS_lpn_loan_amnt" = $1
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
        )
        ORDER BY "time" ASC
        "#;

        sqlx::query_as(SQL)
            .bind(contract_id)
            .fetch_all(&self.pool)
            .await
    }
}
