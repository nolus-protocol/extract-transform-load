use crate::model::PL_State;
use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use tokio::task::JoinHandle;
use std::str::FromStr;

use crate::{
    configuration::{AppState, State},
    error::Error,
};

pub async fn parse_and_insert(
    app_state: AppState<State>,
    prev_action_timestamp: DateTime<Utc>,
    last_action_timestamp: DateTime<Utc>,
    current_timestsamp: DateTime<Utc>,
) -> Result<(), Error> {
    let (PL_pools_TVL_stable, PL_pools_borrowed_stable, PL_pools_yield_stable) = app_state
        .database
        .lp_pool_state
        .get_total_value_locked_stable(current_timestsamp)
        .await?;

    let PL_LS_count_open = app_state
        .database
        .ls_state
        .count(current_timestsamp)
        .await?;

    let PL_LS_count_closed = app_state
        .database
        .ls_closing
        .count(last_action_timestamp, current_timestsamp)
        .await?;

    let PL_LS_count_opened = app_state
        .database
        .ls_opening
        .count(last_action_timestamp, current_timestsamp)
        .await?;

    let PL_IN_LS_cltr_amnt_opened_stable = app_state
        .database
        .ls_opening
        .get_cltr_amnt_opened_stable_sum(last_action_timestamp, current_timestsamp)
        .await?;

    let PL_LP_count_open = app_state
        .database
        .lp_lender_state
        .count(current_timestsamp)
        .await?;

    let PL_LP_count_closed = app_state
        .database
        .lp_withdraw
        .count_closed(last_action_timestamp, current_timestsamp)
        .await?;

    let PL_LP_count_opened = app_state
        .database
        .lp_deposit
        .count(last_action_timestamp, current_timestsamp)
        .await?;

    let PL_OUT_LS_loan_amnt_stable = app_state
        .database
        .ls_opening
        .get_loan_amnt_stable_sum(last_action_timestamp, current_timestsamp)
        .await?;

    let (
        PL_IN_LS_rep_prev_margin_stable,
        PL_IN_LS_rep_prev_interest_stable,
        PL_IN_LS_rep_current_margin_stable,
        PL_IN_LS_rep_current_interest_stable,
        PL_IN_LS_rep_principal_stable,
    ) = app_state
        .database
        .ls_repayment
        .get_sum(last_action_timestamp, current_timestsamp)
        .await?;

    let PL_IN_LS_rep_amnt_stable = &PL_IN_LS_rep_prev_margin_stable
        + &PL_IN_LS_rep_prev_interest_stable
        + &PL_IN_LS_rep_current_margin_stable
        + &PL_IN_LS_rep_current_interest_stable;

    let PL_OUT_LS_cltr_amnt_stable = app_state
        .database
        .ls_opening
        .get_ls_cltr_amnt_stable_sum(last_action_timestamp, current_timestsamp)
        .await?;

    let PL_OUT_LS_amnt_stable = app_state
        .database
        .ls_opening
        .get_ls_amnt_stable_sum(last_action_timestamp, current_timestsamp)
        .await?;

    let PL_IN_LP_amnt_stable = app_state
        .database
        .lp_deposit
        .get_amnt_stable(last_action_timestamp, current_timestsamp)
        .await?;

    let PL_OUT_LP_amnt_stable = app_state
        .database
        .lp_withdraw
        .get_amnt_stable(last_action_timestamp, current_timestsamp)
        .await?;

    let (PL_TR_profit_amnt_stable, PL_TR_profit_amnt_nls) = app_state
        .database
        .tr_profit
        .get_amnt_stable(last_action_timestamp, current_timestsamp)
        .await?;

    let TR_amnt_stable = app_state
        .database
        .tr_state
        .get_amnt_stable(last_action_timestamp, current_timestsamp)
        .await?;

    let TR_amnt_stable_prev = app_state
        .database
        .tr_state
        .get_amnt_stable(prev_action_timestamp, last_action_timestamp)
        .await?;

    let TR_amnt_nls = app_state
        .database
        .tr_state
        .get_amnt_nls(last_action_timestamp, current_timestsamp)
        .await?;

    let TR_amnt_nls_prev = app_state
        .database
        .tr_state
        .get_amnt_nls(prev_action_timestamp, last_action_timestamp)
        .await?;

    let PL_OUT_TR_rewards_amnt_stable = app_state
        .database
        .tr_rewards_distribution
        .get_amnt_stable(last_action_timestamp, current_timestsamp)
        .await?;

    let PL_OUT_TR_rewards_amnt_nls = app_state
        .database
        .tr_rewards_distribution
        .get_amnt_nls(last_action_timestamp, current_timestsamp)
        .await?;

    let PL_TR_tax_amnt_stable = TR_amnt_stable + &PL_OUT_TR_rewards_amnt_stable
        - &PL_TR_profit_amnt_stable
        - TR_amnt_stable_prev;

    let PL_TR_tax_amnt_nls =
        TR_amnt_nls + &PL_OUT_TR_rewards_amnt_nls - &PL_TR_profit_amnt_nls - TR_amnt_nls_prev;

    let pl_state = PL_State {
        PL_timestamp: current_timestsamp,
        PL_pools_TVL_stable,
        PL_pools_borrowed_stable,
        PL_pools_yield_stable,
        PL_LS_count_open,
        PL_LS_count_closed,
        PL_LS_count_opened,
        PL_IN_LS_cltr_amnt_opened_stable,
        PL_LP_count_open,
        PL_LP_count_closed,
        PL_LP_count_opened,
        PL_OUT_LS_loan_amnt_stable,
        PL_IN_LS_rep_amnt_stable,
        PL_IN_LS_rep_prev_margin_stable,
        PL_IN_LS_rep_prev_interest_stable,
        PL_IN_LS_rep_current_margin_stable,
        PL_IN_LS_rep_current_interest_stable,
        PL_IN_LS_rep_principal_stable,
        PL_OUT_LS_cltr_amnt_stable,
        PL_OUT_LS_amnt_stable,
        PL_native_amnt_stable: BigDecimal::from_str("0")?,
        PL_native_amnt_nolus: BigDecimal::from_str("0")?,
        PL_IN_LP_amnt_stable,
        PL_OUT_LP_amnt_stable,
        PL_TR_profit_amnt_stable,
        PL_TR_profit_amnt_nls,
        PL_TR_tax_amnt_stable,
        PL_TR_tax_amnt_nls,
        PL_OUT_TR_rewards_amnt_stable,
        PL_OUT_TR_rewards_amnt_nls,
    };

    app_state.database.pl_state.insert(pl_state).await?;

    Ok(())
}

pub fn start_task(
    app_state: AppState<State>,
    prev_action_timestamp: DateTime<Utc>,
    last_action_timestamp: DateTime<Utc>,
    current_timestsamp: DateTime<Utc>,
) -> JoinHandle<Result<(), Error>> {
    tokio::spawn(async move {
        parse_and_insert(
            app_state,
            prev_action_timestamp,
            last_action_timestamp,
            current_timestsamp,
        )
        .await
    })
}
