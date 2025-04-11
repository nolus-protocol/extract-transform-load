use sqlx::Error;

use crate::model::{PL_State, Table};

use super::QueryResult;

impl Table<PL_State> {
    pub async fn insert(&self, data: PL_State) -> Result<QueryResult, Error> {
        sqlx::query(
            r#"
            INSERT INTO "PL_State" (
                "PL_timestamp",
                "PL_pools_TVL_stable",
                "PL_pools_borrowed_stable",
                "PL_pools_yield_stable",
                "PL_LS_count_open",
                "PL_LS_count_closed",
                "PL_LS_count_opened",
                "PL_IN_LS_cltr_amnt_opened_stable",
                "PL_LP_count_open",
                "PL_LP_count_closed",
                "PL_LP_count_opened",
                "PL_OUT_LS_loan_amnt_stable",
                "PL_IN_LS_rep_amnt_stable",
                "PL_IN_LS_rep_prev_margin_stable",
                "PL_IN_LS_rep_prev_interest_stable",
                "PL_IN_LS_rep_current_margin_stable",
                "PL_IN_LS_rep_current_interest_stable",
                "PL_IN_LS_rep_principal_stable",
                "PL_OUT_LS_cltr_amnt_stable",
                "PL_OUT_LS_amnt_stable",
                "PL_native_amnt_stable",
                "PL_native_amnt_nolus",
                "PL_IN_LP_amnt_stable",
                "PL_OUT_LP_amnt_stable",
                "PL_TR_profit_amnt_stable",
                "PL_TR_profit_amnt_nls",
                "PL_TR_tax_amnt_stable",
                "PL_TR_tax_amnt_nls",
                "PL_OUT_TR_rewards_amnt_stable",
                "PL_OUT_TR_rewards_amnt_nls"
            )
            VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30)
        "#,
        )
        .bind(data.PL_timestamp)
        .bind(&data.PL_pools_TVL_stable)
        .bind(&data.PL_pools_borrowed_stable)
        .bind(&data.PL_pools_yield_stable)
        .bind(data.PL_LS_count_open)
        .bind(data.PL_LS_count_closed)
        .bind(data.PL_LS_count_opened)
        .bind(&data.PL_IN_LS_cltr_amnt_opened_stable)
        .bind(data.PL_LP_count_open)
        .bind(data.PL_LP_count_closed)
        .bind(data.PL_LP_count_opened)
        .bind(&data.PL_OUT_LS_loan_amnt_stable)
        .bind(&data.PL_IN_LS_rep_amnt_stable)
        .bind(&data.PL_IN_LS_rep_prev_margin_stable)
        .bind(&data.PL_IN_LS_rep_prev_interest_stable)
        .bind(&data.PL_IN_LS_rep_current_margin_stable)
        .bind(&data.PL_IN_LS_rep_current_interest_stable)
        .bind(&data.PL_IN_LS_rep_principal_stable)
        .bind(&data.PL_OUT_LS_cltr_amnt_stable)
        .bind(&data.PL_OUT_LS_amnt_stable)
        .bind(&data.PL_native_amnt_stable)
        .bind(&data.PL_native_amnt_nolus)
        .bind(&data.PL_IN_LP_amnt_stable)
        .bind(&data.PL_OUT_LP_amnt_stable)
        .bind(&data.PL_TR_profit_amnt_stable)
        .bind(&data.PL_TR_profit_amnt_nls)
        .bind(&data.PL_TR_tax_amnt_stable)
        .bind(&data.PL_TR_tax_amnt_nls)
        .bind(&data.PL_OUT_TR_rewards_amnt_stable)
        .bind(&data.PL_OUT_TR_rewards_amnt_nls)
        .persistent(false)
        .execute(&self.pool)
        .await
    }
}
