use crate::{
    configuration::{AppState, State},
    error::Error,
};
use tokio::{time, time::Duration};

pub async fn set_total_value_locked(app_state: AppState<State>) -> Result<(), Error> {
    let data = app_state.database.ls_state.get_total_value_locked().await?;
    let cache = &app_state.clone().cache;
    let cache = cache.lock();

    if let Ok(mut c) = cache {
        c.total_value_locked = Some(data);
    }

    Ok(())
}

pub async fn set_total_value_locked_series(app_state: AppState<State>) -> Result<(), Error> {
    let data = app_state.database.ls_state.get_total_value_locked_series().await?;
    let cache = &app_state.clone().cache;
    let cache = cache.lock();

    if let Ok(mut c) = cache {
        c.total_value_locked_series = Some(data);
    }

    Ok(())
}

pub async fn set_yield(app_state: AppState<State>) -> Result<(), Error> {
    let data = app_state.database.lp_deposit.get_yield().await?;
    let cache = &app_state.clone().cache;
    let cache = cache.lock();

    if let Ok(mut c) = cache {
        c.r#yield = Some(data);
    }

    Ok(())
}

pub async fn cache_state_tasks(app_state: AppState<State>) -> Result<(), Error> {
    let interval: u64 = app_state.config.cache_state_interval.into();
    let interval: u64 = interval * 60;

    let mut interval = time::interval(Duration::from_secs(interval));
    tokio::spawn(async move {
        tokio::try_join!(
            set_total_value_locked(app_state.clone()),
            set_yield(app_state.clone()),
            set_total_value_locked_series(app_state.clone())
        )?;

        interval.tick().await;
        loop {
            interval.tick().await;
            tokio::try_join!(
                set_total_value_locked(app_state.clone()),
                set_yield(app_state.clone()),
                set_total_value_locked_series(app_state.clone())
            )?;
        }
    })
    .await?
}
