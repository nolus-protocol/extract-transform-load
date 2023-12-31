use super::{DataBase, QueryResult};
use crate::model::{LS_Opening, Table, Borrow_APR, Leased_Asset};
use chrono::{DateTime, Utc};
use sqlx::{error::Error, types::BigDecimal, QueryBuilder, Transaction};
use std::str::FromStr;

impl Table<LS_Opening> {
    pub async fn insert(
        &self,
        data: LS_Opening,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<QueryResult, Error> {
        sqlx::query(
            r#"
            INSERT INTO `LS_Opening` (
                `LS_contract_id`,
                `LS_address_id`,
                `LS_asset_symbol`,
                `LS_interest`,
                `LS_timestamp`,
                `LS_loan_pool_id`,
                `LS_loan_amnt_stable`,
                `LS_loan_amnt_asset`,
                `LS_cltr_symbol`,
                `LS_cltr_amnt_stable`,
                `LS_cltr_amnt_asset`,
                `LS_native_amnt_stable`,
                `LS_native_amnt_nolus`
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            INSERT INTO `LS_Opening` (
                `LS_contract_id`,
                `LS_address_id`,
                `LS_asset_symbol`,
                `LS_interest`,
                `LS_timestamp`,
                `LS_loan_pool_id`,
                `LS_loan_amnt_stable`,
                `LS_loan_amnt_asset`,
                `LS_cltr_symbol`,
                `LS_cltr_amnt_stable`,
                `LS_cltr_amnt_asset`,
                `LS_native_amnt_stable`,
                `LS_native_amnt_nolus`
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
                .push_bind(&ls.LS_native_amnt_nolus);
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
            FROM `LS_Opening` WHERE `LS_timestamp` > ? AND `LS_timestamp` <= ?
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
                SUM(`LS_cltr_amnt_stable`)
            FROM `LS_Opening` WHERE `LS_timestamp` > ? AND `LS_timestamp` <= ?
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
                SUM(`LS_loan_amnt_stable`)
            FROM `LS_Opening` WHERE `LS_timestamp` > ? AND `LS_timestamp` <= ?
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
                SUM(`LS_cltr_amnt_stable`)
            FROM `LS_Opening`
            LEFT JOIN 
                `LS_Closing`
            ON
                `LS_Opening`.`LS_contract_id` = `LS_Closing`.`LS_contract_id`
            WHERE `LS_Closing`.`LS_timestamp` > ? AND `LS_Closing`.`LS_timestamp` <= ?
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
                SUM(`LS_loan_amnt_stable` + `LS_cltr_amnt_stable`)
            FROM `LS_Opening`
            LEFT JOIN 
                `LS_Closing`
            ON
                `LS_Opening`.`LS_contract_id` = `LS_Closing`.`LS_contract_id`
            WHERE `LS_Closing`.`LS_timestamp` > ? AND `LS_Closing`.`LS_timestamp` <= ?
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

    pub async fn get_borrow_apr(&self, skip: i64, limit: i64) -> Result<Vec<Borrow_APR>, Error> {
        let data = sqlx::query_as(
            r#"
            SELECT `LS_interest` / 10.0 AS `APR` FROM `LS_Opening` ORDER BY `LS_timestamp` DESC LIMIT ? OFFSET ?
            "#,
        )
        .bind(limit)
        .bind(skip)
        .fetch_all(&self.pool)
        .await?;
        Ok(data)
    }

    pub async fn get_leased_assets(&self) -> Result<Vec<Leased_Asset>, Error> {
        let data = sqlx::query_as(
            r#"
            SELECT `LS_asset_symbol` AS `Asset`, SUM(`LS_loan_amnt_asset` / 1000000) AS `Loan` FROM `LS_Opening` GROUP BY `Asset`
            "#,
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(data)
    }
}
