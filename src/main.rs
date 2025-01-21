use actix_web::web;
use chrono::{TimeDelta, Utc};
use std::{sync::Arc, time::Duration};
use tokio::time::{self, Instant};
use tracing::{error, Level};

use etl::{
    configuration::{get_configuration, set_configuration, Config, State},
    error::Error,
    handler::{aggregation_task, cache_state, mp_assets},
    model::Actions,
    provider::{DatabasePool, Event, Grpc},
    server,
};

#[tokio::main]
async fn main() -> Result<(), Error> {
    app_main().await.inspect_err(|error| error!("{error}"))
}

async fn app_main() -> Result<(), Error> {
    tracing::subscriber::set_global_default(
        tracing_subscriber::fmt()
            .compact()
            .with_level(true)
            .with_file(true)
            .with_line_number(true)
            .finish(),
    )?;

    let (config, database) = init()
        .await
        .map_err(|error| Error::ConfigurationError(error.to_string()))?;

    let app_state = web::Data::new(
        State::new(config, database, Grpc::new(config.clone()).await?).await?,
    );

    mp_assets::fetch_insert(&app_state, None).await?;

    let (_, _, _, _, _) = tokio::try_join!(
        Event::new(app_state.clone()).run(),
        mp_assets::mp_assets_task(&app_state),
        start_aggregation_tasks(&app_state),
        server::server_task(&app_state),
        cache_state::cache_state_tasks(&app_state),
    )?;

    Ok(())
}

async fn init() -> Result<(Config, DatabasePool), Error> {
    set_configuration()?;

    let config = get_configuration()?;

    DatabasePool::new(&config)
        .await
        .map(|database| (config, database))
}

async fn start_aggregation_tasks(app_state: &Arc<State>) -> Result<(), Error> {
    let model = &app_state.database.action_history;

    let duration = Duration::from_secs(
        u64::from(app_state.config.aggregation_interval) * 60 * 60,
    );

    let mut interval = time::interval_at(
        Instant::now()
            + (if let Some(item) = model
                .get_last_by_type(Actions::AggregationAction)
                .await
                .map_err(Error::SQL)?
            {
                let (delta, added) = {
                    let delta = Utc::now() - item.created_at;

                    if delta < TimeDelta::zero() {
                        (delta.abs(), Some(duration))
                    } else {
                        (delta, None)
                    }
                };

                let delta = delta
                    .to_std()
                    .expect("Time delta should always be positive!");

                if let Some(added) = added {
                    delta + added
                } else {
                    delta
                }
                .min(duration)
            } else {
                duration
            }),
        duration,
    );

    tokio::spawn(async move {
        loop {
            interval.tick().await;

            aggregation_task(app_state.clone()).await?;
        }
    })
    .await?
}

// async fn test2(app_state: AppState<State>) -> Result<(), Error> {
//     let mut interval = time::interval(Duration::from_secs(10));
//
//     tokio::spawn(async move {
//         loop {
//             interval.tick().await;
//             let app = app_state.clone();
//             let grpc = &app.grpc;
//
//             match grpc.parse_block(6406972).await {
//                 Ok(data) => {
//                     dbg!(data);
//                 }
//                 Err(err) => {
//                     dbg!(err);
//                 }
//             };
//         }
//     })
//     .await?
// }
