use super::{DataBase, QueryResult};
use crate::model::{LS_Repayment, Table};
use chrono::{DateTime, Utc};
use sqlx::{error::Error, types::BigDecimal, QueryBuilder, Transaction};
use std::str::FromStr;

impl Table<LS_Repayment> {
    pub async fn insert(
        &self,
        data: LS_Repayment,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<QueryResult, Error> {
        sqlx::query(
            r#"
            INSERT INTO `LS_Repayment` (
                `LS_repayment_height`,
                `LS_contract_id`,
                `LS_symbol`,
                `LS_amnt_stable`,
                `LS_timestamp`,
                `LS_loan_close`,
                `LS_prev_margin_stable`,
                `LS_prev_interest_stable`,
                `LS_current_margin_stable`,
                `LS_current_interest_stable`,
                `LS_principal_stable`
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#,
        )
        .bind(&data.LS_repayment_height)
        .bind(&data.LS_contract_id)
        .bind(&data.LS_symbol)
        .bind(&data.LS_amnt_stable)
        .bind(&data.LS_timestamp)
        .bind(&data.LS_loan_close)
        .bind(&data.LS_prev_margin_stable)
        .bind(&data.LS_prev_interest_stable)
        .bind(&data.LS_current_margin_stable)
        .bind(&data.LS_current_interest_stable)
        .bind(&data.LS_principal_stable)
        .execute(transaction)
        .await
    }

    pub async fn insert_many(
        &self,
        data: &Vec<LS_Repayment>,
        transaction: &mut Transaction<'_, DataBase>,
    ) -> Result<(), Error> {
        if data.len() == 0 {
            return Ok(());
        }

        let mut query_builder: QueryBuilder<DataBase> = QueryBuilder::new(
            r#"
            INSERT INTO `LS_Repayment` (
                `LS_repayment_height`,
                `LS_contract_id`,
                `LS_symbol`,
                `LS_amnt_stable`,
                `LS_timestamp`,
                `LS_loan_close`,
                `LS_prev_margin_stable`,
                `LS_prev_interest_stable`,
                `LS_current_margin_stable`,
                `LS_current_interest_stable`,
                `LS_principal_stable`
            )"#,
        );

        query_builder.push_values(data, |mut b, ls| {
            b.push_bind(&ls.LS_repayment_height)
                .push_bind(&ls.LS_contract_id)
                .push_bind(&ls.LS_symbol)
                .push_bind(&ls.LS_amnt_stable)
                .push_bind(&ls.LS_timestamp)
                .push_bind(&ls.LS_loan_close)
                .push_bind(&ls.LS_prev_margin_stable)
                .push_bind(&ls.LS_prev_interest_stable)
                .push_bind(&ls.LS_current_margin_stable)
                .push_bind(&ls.LS_current_interest_stable)
                .push_bind(&ls.LS_principal_stable);
        });

        let query = query_builder.build();
        query.execute(transaction).await?;
        Ok(())
    }

    pub async fn get_sum(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<(BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal), crate::error::Error>
    {
        let value: (
            Option<BigDecimal>,
            Option<BigDecimal>,
            Option<BigDecimal>,
            Option<BigDecimal>,
            Option<BigDecimal>,
        ) = sqlx::query_as(
            r#"
            SELECT 
                SUM(`LS_prev_margin_stable`),
                SUM(`LS_prev_interest_stable`),
                SUM(`LS_current_margin_stable`),
                SUM(`LS_current_interest_stable`),
                SUM(`LS_principal_stable`)
            FROM `LS_Repayment` WHERE `LS_timestamp` > ? AND `LS_timestamp` < ?
            "#,
        )
        .bind(from)
        .bind(to)
        .fetch_one(&self.pool)
        .await?;
        let (
            prev_margin_stable,
            prev_interest_stable,
            current_margin_stable,
            current_interest_stable,
            prinicap_stable,
        ) = value;

        let prev_margin_stable = prev_margin_stable.unwrap_or(BigDecimal::from_str("0")?);
        let prev_interest_stable = prev_interest_stable.unwrap_or(BigDecimal::from_str("0")?);
        let current_margin_stable = current_margin_stable.unwrap_or(BigDecimal::from_str("0")?);
        let current_interest_stable = current_interest_stable.unwrap_or(BigDecimal::from_str("0")?);
        let prinicap_stable = prinicap_stable.unwrap_or(BigDecimal::from_str("0")?);

        Ok((
            prev_margin_stable,
            prev_interest_stable,
            current_margin_stable,
            current_interest_stable,
            prinicap_stable,
        ))
    }
}
