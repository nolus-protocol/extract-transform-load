use chrono::{DateTime, NaiveDateTime, Utc};
use std::vec;
use tokio::task::JoinHandle;

use crate::{
    configuration::{AppState, State},
    error::Error,
    model::Action_History,
    model::{Actions, Table},
};

use super::mp_assets_state;
use super::{lp_lender_state, lp_pool_state, ls_state, pl_state, tr_state};

pub fn aggregation_task(app_state: AppState<State>) -> JoinHandle<Result<(), Error>> {
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
                None => DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(0, 0), Utc),
            },
            Err(_) => DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(0, 0), Utc),
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
                None => DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(0, 0), Utc),
            },
            Err(_) => DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(0, 0), Utc),
        };

        insert_action(&app_state.database.action_history, timestsamp.clone()).await?;

        let joins = vec![
            mp_assets_state::start_task(app_state.clone(), timestsamp),
            ls_state::start_task(app_state.clone(), timestsamp),
            lp_lender_state::start_task(app_state.clone(), timestsamp),
            lp_pool_state::start_task(app_state.clone(), timestsamp),
            tr_state::start_task(app_state.clone(), timestsamp),
        ];

        for j in joins {
            if let Err(error) = j.await? {
                return Err(error);
            }
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
