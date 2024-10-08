use super::{DataBase, QueryResult};
use crate::{
    model::{Borrow_APR, LS_Opening, Leased_Asset, Table},
    types::LS_Max_Interest,
};
use chrono::{DateTime, Utc};
use sqlx::{error::Error, types::BigDecimal, QueryBuilder, Transaction};
use std::str::FromStr;

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
        .fetch_one(&self.pool)
        .await?;

        if value > 0 {
            sqlx::query(
                r#"
                    UPDATE 
                        "LS_Opening" 
                    SET 
                        "Tx_Hash" = $1
                    WHERE 
                        "LS_contract_id" = $2
                "#,
            )
            .bind(&ls_opening.Tx_Hash)
            .bind(&ls_opening.LS_contract_id)
            .execute(&self.pool)
            .await?;

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
                "Tx_Hash"
            )
            VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
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
                "Tx_Hash"
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
                .push_bind(&ls.Tx_Hash);
        });

        let query = query_builder.build();
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
        .fetch_all(&self.pool)
        .await?;
        Ok(data)
    }

    pub async fn get_leased_assets_total(
        &self,
    ) -> Result<Vec<Leased_Asset>, Error> {
        let data = sqlx::query_as(
            r#"
            SELECT "LS_asset_symbol" AS "Asset", SUM("LS_loan_amnt_asset" / 1000000) AS "Loan" FROM "LS_Opening" GROUP BY "Asset"
            "#,
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(data)
    }

    pub async fn get_earn_apr(
        &self,
        protocol: String,
    ) -> Result<BigDecimal, crate::error::Error> {
        let value: Option<(BigDecimal,)> = sqlx::query_as(
            r#"
            WITH DateRange AS (
                SELECT
                    generate_series(
                        CURRENT_DATE - INTERVAL '7 days',
                        CURRENT_DATE,
                        '1 day'
                    ) :: date AS date
            ),
            Pool_State_Interest AS (
            SELECT
                "LP_Pool_timestamp",
                CASE
                    WHEN "LP_Pool_total_borrowed_stable"/"LP_Pool_total_value_locked_stable" < 0.7 THEN (12 + (("LP_Pool_total_borrowed_stable"/"LP_Pool_total_value_locked_stable") / (1 - ("LP_Pool_total_borrowed_stable"/"LP_Pool_total_value_locked_stable")) / 0.7)*2) * 10
                    ELSE 186 
                END AS "interest"
            FROM
                "LP_Pool_State"
            WHERE
                "LP_Pool_timestamp" >= CURRENT_DATE - INTERVAL '7 days'
                AND "LP_Pool_id" = $1
            ORDER BY
                "LP_Pool_timestamp" DESC
            ),
            DailyInterest AS (
                SELECT
                DATE("LP_Pool_timestamp") as date,
                MAX("interest") AS max_interest
                FROM
                Pool_State_Interest
                GROUP BY
                "LP_Pool_timestamp"
            ),
            MaxLSInterest AS (
                SELECT
                    dr.date,
                    COALESCE(
                        di.max_interest,
                        FIRST_VALUE(di.max_interest) OVER (
                            ORDER BY
                                dr.date ROWS BETWEEN UNBOUNDED PRECEDING
                                AND 1 PRECEDING
                        )
                    ) AS max_interest
                FROM
                    DateRange dr
                    LEFT JOIN DailyInterest di ON dr.date = di.date
            ),
            MaxLPRatio AS (
                SELECT
                    DATE("LP_Pool_timestamp") AS date,
                    (
                        "LP_Pool_total_borrowed_stable" / "LP_Pool_total_value_locked_stable"
                    ) AS ratio
                FROM
                    (
                        SELECT
                            *,
                            RANK() OVER (
                                PARTITION BY DATE("LP_Pool_timestamp")
                                ORDER BY
                                    (
                                        "LP_Pool_total_borrowed_stable" / "LP_Pool_total_value_locked_stable"
                                    ) DESC
                            ) AS rank
                        FROM
                            "LP_Pool_State"
                        WHERE
                            "LP_Pool_timestamp" >= CURRENT_DATE - INTERVAL '7 days'
                            AND "LP_Pool_id" = $2
                    ) ranked
                WHERE
                    ranked.rank = 1
            ),
            APRCalc AS (
                SELECT
                    AVG((mli.max_interest - 40) * mlr.ratio) / 10 AS "Earn APR"
                FROM
                    MaxLSInterest mli
                    JOIN MaxLPRatio mlr ON mli.date = mlr.date
            )
            SELECT
                COALESCE((POWER((1 + ("Earn APR" / 100 / 365)), 365) - 1) * 100, 0) AS "Earn APY"
            FROM APRCalc
            "#,
        )
        .bind(&protocol)
        .bind(&protocol)
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

        let data = query.fetch_all(&self.pool).await?;
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
                    WHEN "LS_cltr_symbol" IN ('WBTC', 'ALL_BTC', 'CRO') THEN "LS_cltr_amnt_stable" / 100000000 
                    WHEN "LS_cltr_symbol" IN ('ALL_SOL') THEN "LS_cltr_amnt_stable" / 1000000000 
                    WHEN "LS_cltr_symbol" IN ('PICA') THEN "LS_cltr_amnt_stable" / 1000000000000 
                    WHEN "LS_cltr_symbol" IN ('WETH', 'EVMOS', 'INJ', 'DYDX', 'DYM', 'CUDOS') THEN "LS_cltr_amnt_stable" / 1000000000000000000
                    ELSE "LS_cltr_amnt_stable" / 1000000
                    END AS "Down Payment Amount",
                    "LS_loan_amnt_asset" / 1000000 AS "Loan"
                    FROM "LS_Opening"
                )
                
                SELECT
                    SUM ("Volume") AS "Tx Value"
                FROM (
                    SELECT ("Down Payment Amount" + "Loan") AS "Volume" FROM Opened_Leases
                    UNION ALL
                    SELECT SUM("LP_amnt_asset" / 1000000) AS "Volume" FROM "LP_Deposit"
                    UNION ALL 
                    SELECT SUM("LP_amnt_asset" / 1000000) AS "Volume" FROM "LP_Withdraw"
                    UNION ALL
                    SELECT SUM("LS_amnt_stable" / 1000000) AS "Volume" FROM "LS_Close_Position"
                    UNION ALL
                    SELECT SUM("LS_amnt_stable" / 1000000) AS "Volume" FROM "LS_Repayment"
                ) AS combined_data;
          
              "#,
          )
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
                    a."Tx_Hash"
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
        .fetch_all(&self.pool)
        .await?;
        Ok(data)
    }
}
