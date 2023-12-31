use chrono::{DateTime, Utc};
use sqlx::{types::BigDecimal, FromRow};

#[derive(Debug, FromRow)]
pub struct PL_State {
    pub PL_timestamp: DateTime<Utc>,
    pub PL_pools_TVL_stable: BigDecimal,
    pub PL_pools_borrowed_stable: BigDecimal,
    pub PL_pools_yield_stable: BigDecimal,
    pub PL_LS_count_open: i64,
    pub PL_LS_count_closed: i64,
    pub PL_LS_count_opened: i64,
    pub PL_IN_LS_cltr_amnt_opened_stable: BigDecimal,
    pub PL_LP_count_open: i64,
    pub PL_LP_count_closed: i64,
    pub PL_LP_count_opened: i64,
    pub PL_OUT_LS_loan_amnt_stable: BigDecimal,
    pub PL_IN_LS_rep_amnt_stable: BigDecimal,
    pub PL_IN_LS_rep_prev_margin_stable: BigDecimal,
    pub PL_IN_LS_rep_prev_interest_stable: BigDecimal,
    pub PL_IN_LS_rep_current_margin_stable: BigDecimal,
    pub PL_IN_LS_rep_current_interest_stable: BigDecimal,
    pub PL_IN_LS_rep_principal_stable: BigDecimal,
    pub PL_OUT_LS_cltr_amnt_stable: BigDecimal,
    pub PL_OUT_LS_amnt_stable: BigDecimal,
    pub PL_native_amnt_stable: BigDecimal,
    pub PL_native_amnt_nolus: BigDecimal,
    pub PL_IN_LP_amnt_stable: BigDecimal,
    pub PL_OUT_LP_amnt_stable: BigDecimal,
    pub PL_TR_profit_amnt_stable: BigDecimal,
    pub PL_TR_profit_amnt_nls: BigDecimal,
    pub PL_TR_tax_amnt_stable: BigDecimal,
    pub PL_TR_tax_amnt_nls: BigDecimal,
    pub PL_OUT_TR_rewards_amnt_stable: BigDecimal,
    pub PL_OUT_TR_rewards_amnt_nls: BigDecimal,
}
