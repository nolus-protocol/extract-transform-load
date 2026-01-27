use std::str::FromStr;

use sqlx::{Error, QueryBuilder, Transaction};

use crate::{
    model::{CosmosTypes, Raw_Message, Table},
    types::Bucket_Type,
};

use super::{DataBase, QueryResult};

impl Table<Raw_Message> {
    pub async fn insert_if_not_exists(
        &self,
        data: Raw_Message,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<QueryResult, Error> {
        sqlx::query(
            r#"
            INSERT INTO "raw_message" ("index", "from", "to", "tx_hash", "type", "value", "block", "fee_amount", "fee_denom", "memo", "timestamp", "rewards", "code")
            VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            ON CONFLICT ("index", "tx_hash") DO NOTHING
            "#,
        )
        .bind(data.index)
        .bind(&data.from)
        .bind(&data.to)
        .bind(&data.tx_hash)
        .bind(&data.r#type)
        .bind(&data.value)
        .bind(data.block)
        .bind(&data.fee_amount)
        .bind(&data.fee_denom)
        .bind(&data.memo)
        .bind(data.timestamp)
        .bind(&data.rewards)
		.bind(data.code)
        .persistent(true)
        .execute(&mut **transaction)
        .await
    }

    pub async fn get(
        &self,
        address: String,
        skip: i64,
        limit: i64,
        filter: Vec<String>,
        to: Vec<String>,
        combine: bool,
    ) -> Result<Vec<Raw_Message>, Error> {
        let mut filters: Vec<String> = Vec::new();

        for f in filter {
            match CosmosTypes::from_str(&f)? {
                CosmosTypes::MsgExecuteContract => {},
                _ => filters.push(f),
            }
        }

        let mut qb = QueryBuilder::new(
            r#"
        SELECT *
        FROM "raw_message"
        WHERE ("from" = "#,
        );

        qb.push_bind(&address)
            .push(r#" OR "to" = "#)
            .push_bind(&address)
            .push(")");

        let has_filters = !filters.is_empty();
        let has_to = !to.is_empty();
        let earn_type = CosmosTypes::MsgExecuteContract.to_string();

        if has_filters && has_to && combine {
            qb.push(" AND (");

            qb.push(" \"type\" = ANY(")
                .push_bind(filters.as_slice())
                .push(")");

            qb.push(" OR (");

            qb.push(" \"type\" = ").push_bind(&earn_type);

            qb.push(" AND \"to\" = ANY(")
                .push_bind(to.as_slice())
                .push(")");

            qb.push(")");
            qb.push(")");
        } else {
            if has_filters {
                qb.push(" AND \"type\" = ANY(")
                    .push_bind(filters.as_slice())
                    .push(")");
            }

            if has_to {
                qb.push(" AND \"to\" = ANY(")
                    .push_bind(to.as_slice())
                    .push(")");
            }
        }

        qb.push(r#" ORDER BY "timestamp" DESC OFFSET "#)
            .push_bind(skip)
            .push(" LIMIT ")
            .push_bind(limit);

        let query = qb.build_query_as::<Raw_Message>();
        let rows = query.persistent(true).fetch_all(&self.pool).await?;

        Ok(rows)
    }

    pub async fn get_tx_volume(
        &self,
        address: String,
    ) -> Result<f64, crate::error::Error> {
        let value: (Option<f64>,) = sqlx::query_as(
            r#"
                WITH
                openings AS (
                SELECT
                    o."LS_contract_id",
                    o."LS_address_id",
                    (o."LS_cltr_amnt_stable" / POWER(10, cr_cltr.decimal_digits)::NUMERIC)::double precision AS down_payment_usdc,
                    (o."LS_loan_amnt_stable" / pc.lpn_decimals::numeric)::double precision AS loan_usdc
                FROM "LS_Opening" o
                INNER JOIN pool_config pc ON o."LS_loan_pool_id" = pc.pool_id
                INNER JOIN currency_registry cr_cltr ON cr_cltr.ticker = o."LS_cltr_symbol"
                WHERE o."LS_address_id" = $1
                ),
                repayments AS (
                SELECT
                    r."LS_contract_id",
                    (r."LS_payment_amnt_stable" / POWER(10, cr_pay.decimal_digits)::NUMERIC)::double precision AS repayment_usdc
                FROM "LS_Repayment" r
                INNER JOIN currency_registry cr_pay ON cr_pay.ticker = r."LS_payment_symbol"
                INNER JOIN openings o USING ("LS_contract_id")
                ),
                closes AS (
                SELECT
                    c."LS_contract_id",
                    (c."LS_amnt_stable" / POWER(10, cr_close.decimal_digits)::NUMERIC)::double precision AS close_usdc
                FROM "LS_Close_Position" c
                INNER JOIN currency_registry cr_close ON cr_close.ticker = c."LS_amnt_symbol"
                INNER JOIN openings o USING ("LS_contract_id")
                )
                SELECT
                SUM(vol) AS "Tx Volume"
                FROM (
                SELECT 'OPEN' AS src, (o.down_payment_usdc + o.loan_usdc) AS vol FROM openings o
                UNION ALL
                SELECT 'REPAYMENT', r.repayment_usdc FROM repayments r
                UNION ALL
                SELECT 'CLOSE', c.close_usdc FROM closes c
                ) x
            "#,
        )
        .persistent(true)
        .bind(address)
        .fetch_one(&self.pool)
        .await?;
        let (amnt,) = value;
        let amnt = amnt.unwrap_or(0.0);

        Ok(amnt)
    }

    pub async fn get_win_rate(
        &self,
        address: String,
    ) -> Result<f64, crate::error::Error> {
        let value: (Option<f64>,) = sqlx::query_as(
            r#"
                WITH
                openings AS (
                SELECT
                    o."LS_contract_id",
                    o."LS_cltr_symbol",
                    o."LS_cltr_amnt_stable",
                    o."LS_loan_pool_id"
                FROM "LS_Opening" o
                WHERE o."LS_address_id" = $1
                ),

                repayments AS (
                SELECT
                    r."LS_contract_id",
                    (SUM(r."LS_payment_amnt_stable") / pc.stable_currency_decimals::numeric)::numeric(38,8) AS total_repaid_usdc
                FROM "LS_Repayment" r
                INNER JOIN openings o ON o."LS_contract_id" = r."LS_contract_id"
                INNER JOIN pool_config pc ON pc.pool_id = o."LS_loan_pool_id"
                GROUP BY r."LS_contract_id", pc.stable_currency_decimals
                ),

                collects AS (
                SELECT
                    lc."LS_contract_id",
                    SUM(lc."LS_amount_stable" / POWER(10, cr_col.decimal_digits)::NUMERIC)::numeric(38,8) AS total_collected_usdc
                FROM "LS_Loan_Collect" lc
                INNER JOIN openings o ON o."LS_contract_id" = lc."LS_contract_id"
                INNER JOIN currency_registry cr_col ON cr_col.ticker = lc."LS_symbol"
                GROUP BY lc."LS_contract_id"
                ),

                finalized AS (
                SELECT
                    COALESCE(c.total_collected_usdc, 0::numeric(38,8))
                    - (
                        (o."LS_cltr_amnt_stable" / POWER(10, cr_cltr.decimal_digits)::NUMERIC)::numeric(38,8)
                        + COALESCE(r.total_repaid_usdc, 0::numeric(38,8))
                    ) AS pnl
                FROM openings o
                INNER JOIN currency_registry cr_cltr ON cr_cltr.ticker = o."LS_cltr_symbol"
                LEFT JOIN repayments r ON r."LS_contract_id" = o."LS_contract_id"
                LEFT JOIN collects c ON c."LS_contract_id" = o."LS_contract_id"
                INNER JOIN "LS_Loan_Closing" ct ON ct."LS_contract_id" = o."LS_contract_id"
                )

                SELECT
                (COUNT(CASE WHEN pnl > 0 THEN 1 END)::float
                / COUNT(*)::float) * 100 AS "Winrate (%)"
                FROM finalized
            "#,
        )
        .persistent(true)
        .bind(address)
        .fetch_one(&self.pool)
        .await?;
        let (amnt,) = value;
        let amnt = amnt.unwrap_or(0.0);

        Ok(amnt)
    }

    pub async fn get_buckets(
        &self,
        address: String,
    ) -> Result<Vec<Bucket_Type>, crate::error::Error> {
        let data = sqlx::query_as(
            r#"
            WITH
            buckets AS (
            SELECT 1 AS ord, '<0'    AS bucket UNION ALL
            SELECT 2,        '0-50'               UNION ALL
            SELECT 3,        '51–100'             UNION ALL
            SELECT 4,        '101–300'            UNION ALL
            SELECT 5,        '301+'
            ),
            openings AS (
            SELECT
              o."LS_contract_id",
              o."LS_cltr_symbol",
              o."LS_cltr_amnt_stable",
              o."LS_loan_pool_id"
            FROM "LS_Opening" o
            WHERE o."LS_address_id" = $1
            ),
            repayments AS (
            SELECT
              r."LS_contract_id",
              (SUM(r."LS_payment_amnt_stable") / pc.stable_currency_decimals::numeric)::numeric(38,8) AS total_repaid_usdc
            FROM "LS_Repayment" r
            INNER JOIN openings o ON o."LS_contract_id" = r."LS_contract_id"
            INNER JOIN pool_config pc ON pc.pool_id = o."LS_loan_pool_id"
            GROUP BY r."LS_contract_id", pc.stable_currency_decimals
            ),
            collects AS (
            SELECT
              lc."LS_contract_id",
              SUM(lc."LS_amount_stable" / POWER(10, cr_col.decimal_digits)::NUMERIC)::numeric(38,8) AS total_collected_usdc
            FROM "LS_Loan_Collect" lc
            INNER JOIN openings o ON o."LS_contract_id" = lc."LS_contract_id"
            INNER JOIN currency_registry cr_col ON cr_col.ticker = lc."LS_symbol"
            GROUP BY lc."LS_contract_id"
            ),
            finalized AS (
            SELECT
              o."LS_contract_id" AS position_id,
              (
                (o."LS_cltr_amnt_stable" / POWER(10, cr_cltr.decimal_digits)::NUMERIC)::numeric(38,8)
                + COALESCE(r.total_repaid_usdc, 0::numeric(38,8))
              )::double precision AS sent_usdc,
              (
                COALESCE(c.total_collected_usdc, 0::numeric(38,8))
                - (
                    (o."LS_cltr_amnt_stable" / POWER(10, cr_cltr.decimal_digits)::NUMERIC)::numeric(38,8)
                    + COALESCE(r.total_repaid_usdc, 0::numeric(38,8))
                  )
              )::double precision AS realized_pnl_usdc
            FROM openings o
            INNER JOIN currency_registry cr_cltr ON cr_cltr.ticker = o."LS_cltr_symbol"
            LEFT JOIN repayments r ON r."LS_contract_id" = o."LS_contract_id"
            LEFT JOIN collects c ON c."LS_contract_id" = o."LS_contract_id"
            INNER JOIN "LS_Loan_Closing" ct ON ct."LS_contract_id" = o."LS_contract_id"
            ),
            with_pct AS (
            SELECT
              position_id,
              sent_usdc,
              realized_pnl_usdc,
              CASE
                WHEN sent_usdc = 0 THEN NULL
                ELSE (realized_pnl_usdc / sent_usdc) * 100.0
              END AS pnl_pct
            FROM finalized
            ),
            counts AS (
            SELECT
              CASE
                WHEN pnl_pct < 0 THEN '<0'
                WHEN pnl_pct >= 0   AND pnl_pct < 50  THEN '0-50'
                WHEN pnl_pct >= 50  AND pnl_pct < 100 THEN '51–100'
                WHEN pnl_pct >= 100 AND pnl_pct <= 300 THEN '101–300'
                WHEN pnl_pct > 300 THEN '301+'
              END AS bucket,
              COUNT(*) AS cnt
            FROM with_pct
            WHERE pnl_pct IS NOT NULL
            GROUP BY 1
            ),
            tot AS (
            SELECT COALESCE(SUM(cnt),0) AS total FROM counts
            )
            SELECT
            b.bucket,
            COALESCE(c.cnt, 0) AS positions,
            CASE WHEN t.total > 0
                  THEN ROUND(100.0 * COALESCE(c.cnt,0) / t.total, 2)
                  ELSE 0
            END AS share_percent
            FROM buckets b
            LEFT JOIN counts c USING (bucket)
            CROSS JOIN tot t
            ORDER BY b.ord;
            "#,
        )
        .bind(&address)
        .persistent(true)
        .fetch_all(&self.pool)
        .await?;
        Ok(data)
    }

    pub async fn get_all(&self) -> Result<Vec<Raw_Message>, Error> {
        sqlx::query_as(r#"SELECT * FROM "raw_message" where code is null"#)
            .persistent(true)
            .fetch_all(&self.pool)
            .await
    }

    //TODO: delete
    pub async fn update(
        &self,
        data: Raw_Message,
    ) -> Result<QueryResult, Error> {
        sqlx::query(
            r#"
            UPDATE
                "raw_message"
            SET
                "code" = $1
            WHERE
				"index" = $2
			AND
				"tx_hash" = $3

        "#,
        )
        .bind(data.code)
        .bind(data.index)
        .bind(&data.tx_hash)
        .persistent(true)
        .execute(&self.pool)
        .await
    }
}
