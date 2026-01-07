use std::str::FromStr as _;

use chrono::{DateTime, Utc};
use sqlx::{types::BigDecimal, Error, FromRow, QueryBuilder, Transaction};

use crate::model::{LS_Repayment, Table};

use super::{DataBase, QueryResult};

type OptionDecimal = Option<BigDecimal>;

#[derive(Debug, Clone, FromRow)]
pub struct HistoricallyRepaid {
    pub contract_id: String,
    pub symbol: String,
    pub loan: BigDecimal,
    pub total_repaid: BigDecimal,
    pub close_timestamp: Option<DateTime<Utc>>,
    pub loan_closed: String,
}

impl Table<LS_Repayment> {
    pub async fn isExists(
        &self,
        ls_repayment: &LS_Repayment,
    ) -> Result<bool, crate::error::Error> {
        let (value,): (i64,) = sqlx::query_as(
            r#"
            SELECT
                COUNT(*)
            FROM "LS_Repayment"
            WHERE
                "LS_repayment_height" = $1 AND
                "LS_contract_id" = $2 AND
                "LS_timestamp" = $3
            "#,
        )
        .bind(ls_repayment.LS_repayment_height)
        .bind(&ls_repayment.LS_contract_id)
        .bind(ls_repayment.LS_timestamp)
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
        data: LS_Repayment,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<QueryResult, Error> {
        sqlx::query(
            r#"
            INSERT INTO "LS_Repayment" (
                "LS_repayment_height",
                "LS_contract_id",
                "LS_payment_symbol",
                "LS_payment_amnt",
                "LS_payment_amnt_stable",
                "LS_timestamp",
                "LS_loan_close",
                "LS_prev_margin_stable",
                "LS_prev_interest_stable",
                "LS_current_margin_stable",
                "LS_current_interest_stable",
                "LS_principal_stable",
                "Tx_Hash"
            )
            VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
        "#,
        )
        .bind(data.LS_repayment_height)
        .bind(&data.LS_contract_id)
        .bind(&data.LS_payment_symbol)
        .bind(&data.LS_payment_amnt)
        .bind(&data.LS_payment_amnt_stable)
        .bind(data.LS_timestamp)
        .bind(data.LS_loan_close)
        .bind(&data.LS_prev_margin_stable)
        .bind(&data.LS_prev_interest_stable)
        .bind(&data.LS_current_margin_stable)
        .bind(&data.LS_current_interest_stable)
        .bind(&data.LS_principal_stable)
        .bind(&data.Tx_Hash)
        .persistent(true)
        .execute(&mut **transaction)
        .await
    }

    pub async fn insert_many(
        &self,
        data: &Vec<LS_Repayment>,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<(), Error> {
        if data.is_empty() {
            return Ok(());
        }

        let mut query_builder: QueryBuilder<DataBase> = QueryBuilder::new(
            r#"
            INSERT INTO "LS_Repayment" (
                "LS_repayment_height",
                "LS_contract_id",
                "LS_payment_symbol",
                "LS_payment_amnt",
                "LS_payment_amnt_stable",
                "LS_timestamp",
                "LS_loan_close",
                "LS_prev_margin_stable",
                "LS_prev_interest_stable",
                "LS_current_margin_stable",
                "LS_current_interest_stable",
                "LS_principal_stable",
                "Tx_Hash"
            )"#,
        );

        query_builder.push_values(data, |mut b, ls| {
            b.push_bind(ls.LS_repayment_height)
                .push_bind(&ls.LS_contract_id)
                .push_bind(&ls.LS_payment_symbol)
                .push_bind(&ls.LS_payment_amnt)
                .push_bind(&ls.LS_payment_amnt_stable)
                .push_bind(ls.LS_timestamp)
                .push_bind(ls.LS_loan_close)
                .push_bind(&ls.LS_prev_margin_stable)
                .push_bind(&ls.LS_prev_interest_stable)
                .push_bind(&ls.LS_current_margin_stable)
                .push_bind(&ls.LS_current_interest_stable)
                .push_bind(&ls.LS_principal_stable)
                .push_bind(&ls.Tx_Hash);
        });

        let query = query_builder.build().persistent(true);
        query.execute(&mut **transaction).await?;
        Ok(())
    }

    pub async fn get_sum(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<
        (BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal),
        crate::error::Error,
    > {
        let value: (
            OptionDecimal,
            OptionDecimal,
            OptionDecimal,
            OptionDecimal,
            OptionDecimal,
        ) = sqlx::query_as(
            r#"
            SELECT
                SUM("LS_prev_margin_stable"),
                SUM("LS_prev_interest_stable"),
                SUM("LS_current_margin_stable"),
                SUM("LS_current_interest_stable"),
                SUM("LS_principal_stable")
            FROM "LS_Repayment" WHERE "LS_timestamp" > $1 AND "LS_timestamp" < $2
            "#,
        )
        .bind(from)
        .bind(to)
        .persistent(true)
        .fetch_one(&self.pool)
        .await?;
        let (
            prev_margin_stable,
            prev_interest_stable,
            current_margin_stable,
            current_interest_stable,
            prinicap_stable,
        ) = value;

        let prev_margin_stable =
            prev_margin_stable.unwrap_or(BigDecimal::from_str("0")?);
        let prev_interest_stable =
            prev_interest_stable.unwrap_or(BigDecimal::from_str("0")?);
        let current_margin_stable =
            current_margin_stable.unwrap_or(BigDecimal::from_str("0")?);
        let current_interest_stable =
            current_interest_stable.unwrap_or(BigDecimal::from_str("0")?);
        let prinicap_stable =
            prinicap_stable.unwrap_or(BigDecimal::from_str("0")?);

        Ok((
            prev_margin_stable,
            prev_interest_stable,
            current_margin_stable,
            current_interest_stable,
            prinicap_stable,
        ))
    }

    pub async fn get_by_contract(
        &self,
        contract: String,
    ) -> Result<Vec<LS_Repayment>, Error> {
        let data = sqlx::query_as(
            r#"
                SELECT
                    "LS_repayment_height",
                    "LS_repayment_idx",
                    "LS_contract_id",
                    "LS_payment_symbol",
                    "LS_payment_amnt",
                    "LS_payment_amnt_stable",
                    "LS_timestamp",
                    "LS_loan_close",
                    "LS_prev_margin_stable",
                    "LS_prev_interest_stable",
                    "LS_current_margin_stable",
                    "LS_current_interest_stable",
                    "LS_principal_stable",
                    "Tx_Hash"
                FROM
                    "LS_Repayment" as a
                WHERE
                    a."LS_contract_id" = $1
            "#,
        )
        .bind(contract)
        .persistent(true)
        .fetch_all(&self.pool)
        .await?;
        Ok(data)
    }

    pub async fn get_historically_repaid(
        &self,
    ) -> Result<Vec<HistoricallyRepaid>, crate::error::Error> {
        let data = sqlx::query_as(
            r#"
            WITH Closed_Loans AS (
                SELECT
                    "LS_contract_id",
                    "LS_timestamp",
                    "LS_amnt_stable",
                    "LS_loan_close",
                    "LS_amnt_symbol" AS "Amount Symbol"
                FROM
                    "LS_Close_Position"
                UNION ALL
                SELECT
                    "LS_contract_id",
                    "LS_timestamp",
                    "LS_payment_amnt_stable" AS "LS_amnt_stable",
                    "LS_loan_close",
                    "LS_payment_symbol" AS "Amount Symbol"
                FROM
                    "LS_Repayment"
            ),
            RepaidLeases AS (
                SELECT
                    lso."LS_contract_id" AS "Contract ID",
                    lso."LS_asset_symbol" AS "Symbol",
                    lso."LS_loan_amnt_asset" / 1000000 AS "Loan",
                    COALESCE(
                        SUM(
                            CASE
                                WHEN cl."Amount Symbol" IN ('ALL_BTC', 'WBTC', 'CRO') THEN cl."LS_amnt_stable" / 100000000
                                WHEN cl."Amount Symbol" IN ('ALL_SOL') THEN cl."LS_amnt_stable" / 1000000000
                                WHEN cl."Amount Symbol" IN ('PICA') THEN cl."LS_amnt_stable" / 1000000000000
                                WHEN cl."Amount Symbol" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS', 'ALL_ETH') THEN cl."LS_amnt_stable" / 1000000000000000000
                                ELSE cl."LS_amnt_stable" / 1000000
                            END
                        ),
                        0
                    ) AS "Total Repaid",
                    MAX(
                        CASE
                            WHEN cl."LS_loan_close" = true THEN cl."LS_timestamp"
                        END
                    ) AS "Close Timestamp",
                    CASE
                        WHEN SUM(
                            CASE
                                WHEN cl."LS_loan_close" = true THEN 1
                                ELSE 0
                            END
                        ) > 0 THEN 'yes'
                        ELSE 'no'
                    END AS "Loan Closed"
                FROM
                    "LS_Opening" lso
                    LEFT JOIN Closed_Loans cl ON lso."LS_contract_id" = cl."LS_contract_id"
                GROUP BY
                    lso."LS_contract_id",
                    lso."LS_asset_symbol",
                    lso."LS_loan_amnt_asset"
            )
            SELECT
                rl."Contract ID" AS contract_id,
                rl."Symbol" AS symbol,
                rl."Loan" AS loan,
                rl."Total Repaid" AS total_repaid,
                rl."Close Timestamp" AS close_timestamp,
                rl."Loan Closed" AS loan_closed
            FROM
                RepaidLeases rl
            "#,
        )
        .persistent(true)
        .fetch_all(&self.pool)
        .await?;

        Ok(data)
    }
}
