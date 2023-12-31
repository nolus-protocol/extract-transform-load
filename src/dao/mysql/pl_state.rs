use super::QueryResult;
use crate::model::{PL_State, Table};
use sqlx::error::Error;

impl Table<PL_State> {
    pub async fn insert(&self, data: PL_State) -> Result<QueryResult, Error> {
        sqlx::query(
            r#"
            INSERT INTO `PL_State` (
                `PL_timestamp`,
                `PL_pools_TVL_stable`,
                `PL_pools_borrowed_stable`,
                `PL_pools_yield_stable`,
                `PL_LS_count_open`,
                `PL_LS_count_closed`,
                `PL_LS_count_opened`,
                `PL_IN_LS_cltr_amnt_opened_stable`,
                `PL_LP_count_open`,
                `PL_LP_count_closed`,
                `PL_LP_count_opened`,
                `PL_OUT_LS_loan_amnt_stable`,
                `PL_IN_LS_rep_amnt_stable`,
                `PL_IN_LS_rep_prev_margin_stable`,
                `PL_IN_LS_rep_prev_interest_stable`,
                `PL_IN_LS_rep_current_margin_stable`,
                `PL_IN_LS_rep_current_interest_stable`,
                `PL_IN_LS_rep_principal_stable`,
                `PL_OUT_LS_cltr_amnt_stable`,
                `PL_OUT_LS_amnt_stable`,
                `PL_native_amnt_stable`,
                `PL_native_amnt_nolus`,
                `PL_IN_LP_amnt_stable`,
                `PL_OUT_LP_amnt_stable`,
                `PL_TR_profit_amnt_stable`,
                `PL_TR_profit_amnt_nls`,
                `PL_TR_tax_amnt_stable`,
                `PL_TR_tax_amnt_nls`,
                `PL_OUT_TR_rewards_amnt_stable`,
                `PL_OUT_TR_rewards_amnt_nls`
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        .execute(&self.pool)
        .await
    }
}
