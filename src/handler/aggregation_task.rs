use std::sync::Arc;

use chrono::{DateTime, Utc};
use tokio::try_join;

use crate::{
    configuration::State,
    error::Error,
    model::{Action_History, Actions, Table},
};

use super::{
    cache_state, lp_lender_state, lp_pool_state, ls_state, pl_state, tr_state,
};

pub async fn aggregation_task(app_state: Arc<State>) -> Result<(), Error> {
    let timestamp = Utc::now();

    let last_action_timestamp = app_state
        .database
        .action_history
        .get_last_by_type(Actions::AggregationAction)
        .await?
        .map_or_else(Utc::now, |item| item.created_at);

    let prev_action_timestamp = app_state
        .database
        .action_history
        .get_last_by_type_before(
            Actions::AggregationAction,
            last_action_timestamp,
        )
        .await?
        .map_or_else(Utc::now, |action_history| action_history.created_at);

    insert_action(&app_state.database.action_history, timestamp).await?;

    let ((), (), (), ()) = try_join!(
        ls_state::parse_and_insert(app_state.clone(), timestamp),
        lp_lender_state::parse_and_insert(app_state.clone(), timestamp),
        lp_pool_state::parse_and_insert(app_state.clone(), timestamp),
        tr_state::parse_and_insert(app_state.clone(), timestamp),
    )?;

    pl_state::parse_and_insert(
        app_state.clone(),
        prev_action_timestamp,
        last_action_timestamp,
        timestamp,
    )
    .await?;

    cache_state::set_total_value_locked(app_state).await?;

    Ok(())
}

async fn insert_action(
    action_model: &Table<Action_History>,
    created_at: DateTime<Utc>,
) -> Result<(), Error> {
    action_model
        .insert(Action_History {
            action_type: Actions::AggregationAction,
            created_at,
        })
        .await
        .map_err(From::from)
}
