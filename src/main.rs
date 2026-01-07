use std::time::Duration;

use chrono::Utc;
use tokio::time;
use tracing::{error, Level};

use etl::{
    configuration::{
        get_configuration, set_configuration, AppState, Config, State,
    },
    error::Error,
    handler::{aggregation_task, cache_refresher, mp_assets},
    model::Actions,
    provider::{DatabasePool, Event, Grpc, HTTP},
    server,
};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let result = app_main().await;

    if let Err(err) = &result {
        error!("{}", err);
    }

    result
}

async fn app_main() -> Result<(), Error> {
    let subscriber = tracing_subscriber::fmt()
        .compact()
        .with_level(true)
        .with_max_level({
            #[cfg(debug_assertions)]
            {
                Level::INFO
            }

            #[cfg(not(debug_assertions))]
            {
                Level::INFO
            }
        })
        .with_file(true)
        .with_line_number(true)
        .finish();

    tracing::subscriber::set_global_default(subscriber)?;

    let (config, database) = match init().await {
        Ok((config, database)) => (config, database),
        Err(e) => return Err(Error::ConfigurationError(e.to_string())),
    };

    let db_pool = database;
    let grpc = Grpc::new(config.clone()).await?;
    let http = HTTP::new(config.clone())?;

    let state = State::new(config.clone(), db_pool, grpc, http).await?;
    let app_state = AppState::new(state);

    mp_assets::fetch_insert(app_state.clone(), None).await?;
    let mut event_manager = Event::new(app_state.clone());

    let (_, _, _, _, _) = tokio::try_join!(
        event_manager.run(),
        mp_assets::mp_assets_task(app_state.clone()),
        start_aggregation_tasks(app_state.clone()),
        server::server_task(&app_state),
        cache_refresher::cache_refresh_task(app_state.clone()),
    )?;

    Ok(())
}

async fn init() -> Result<(Config, DatabasePool), Error> {
    set_configuration()?;
    let config = get_configuration()?;
    let database = DatabasePool::new(&config).await?;
    Ok((config, database))
}

async fn start_aggregation_tasks(
    app_state: AppState<State>,
) -> Result<(), Error> {
    if !app_state.config.enable_sync {
        return Ok(());
    }

    let interval_value: u64 = app_state.config.aggregation_interval.into();
    let interval_value = interval_value * 60 * 60;
    let model = &app_state.database.action_history;
    let mut dt_ms = 0;

    let latest_action = model
        .get_last_by_type(Actions::AggregationAction.to_string())
        .await;

    match latest_action {
        Ok(data) => {
            if let Some(item) = data {
                dt_ms = item.created_at.timestamp_millis();
            }
        },
        Err(e) => return Err(Error::SQL(e)),
    }

    let now = Utc::now();
    let ms_now = now.timestamp_millis();

    let diff_in_sec = (ms_now - dt_ms) / 1000;
    let diff_value: i64 = diff_in_sec;

    let tmp_interval: u64 = if diff_value > 0_i64 {
        (interval_value as i64 - diff_value).try_into().unwrap_or(0)
    } else {
        interval_value
    };

    let mut init = false;

    let mut interval =
        time::interval(Duration::from_secs(if tmp_interval > 0 {
            tmp_interval
        } else {
            interval_value
        }));

    tokio::spawn(async move {
        if tmp_interval == 0 {
            let result = aggregation_task(app_state.clone()).await?;
            result?;
            init = true;
        }
        interval.tick().await;
        loop {
            interval.tick().await;

            let result = aggregation_task(app_state.clone()).await?;
            result?;

            if !init {
                init = true;
                interval = time::interval(Duration::from_secs(interval_value));
                interval.tick().await;
            }
        }
    })
    .await?
}
