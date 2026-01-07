use chrono::{DateTime, Utc};
use tokio::task::JoinHandle;

use crate::{
    configuration::{AppState, State},
    error::Error,
    model::{Action_History, Actions, Table},
};

use super::{
    cache_refresher, lp_lender_state, lp_pool_state, ls_state, pl_state, tr_state,
};

pub fn aggregation_task(
    app_state: AppState<State>,
) -> JoinHandle<Result<(), Error>> {
    tokio::spawn(async move {
        let timestsamp = Utc::now();
        let action = app_state
            .database
            .action_history
            .get_last_by_type(Actions::AggregationAction.to_string())
            .await;

        let last_action_timestamp = match action {
            Ok(action) => match action {
                Some(item) => item.created_at,
                None => Utc::now(),
            },
            Err(_) => Utc::now(),
        };

        let prev_action = app_state
            .database
            .action_history
            .get_last_by_type_before(
                Actions::AggregationAction.to_string(),
                last_action_timestamp,
            )
            .await;

        let prev_action_timestamp = match prev_action {
            Ok(action) => match action {
                Some(item) => item.created_at,
                None => Utc::now(),
            },
            Err(_) => Utc::now(),
        };

        insert_action(&app_state.database.action_history, timestsamp).await?;

        let joins = vec![
            ls_state::start_task(app_state.clone(), timestsamp),
            lp_lender_state::start_task(app_state.clone(), timestsamp),
            lp_pool_state::start_task(app_state.clone(), timestsamp),
            tr_state::start_task(app_state.clone(), timestsamp),
        ];

        for j in joins {
            j.await??
        }

        if let Err(error) = pl_state::start_task(
            app_state.clone(),
            prev_action_timestamp,
            last_action_timestamp,
            timestsamp,
        )
        .await?
        {
            return Err(Error::ServerError(error.to_string()));
        };

        cache_refresher::refresh_tvl_cache(&app_state).await?;

        Ok(())
    })
}

async fn insert_action(
    action_model: &Table<Action_History>,
    timestamp: DateTime<Utc>,
) -> Result<(), Error> {
    let action = Action_History {
        action_type: Actions::AggregationAction.to_string(),
        created_at: timestamp,
    };
    action_model.insert(action).await?;
    Ok(())
}
