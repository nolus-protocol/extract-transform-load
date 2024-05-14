use crate::{
    configuration::{AppState, State},
    error::Error,
};
use tokio::{time, time::Duration};

pub async fn set_total_value_locked(app_state: AppState<State>) -> Result<(), Error> {
    let osmosis_usdc = if let Some((osmosis_usdc, _)) = app_state.config.lp_pools.first() {
        osmosis_usdc
    } else {
        return Err(Error::ProtocolError(String::from("osmosis_usdc")));
    };

    let neutron_usdc_axelar =
        if let Some((neutron_usdc_axelar, _)) = app_state.config.lp_pools.get(1) {
            neutron_usdc_axelar
        } else {
            return Err(Error::ProtocolError(String::from("neutron_usdc_axelar")));
        };

    let osmosis_usdc_noble = if let Some((osmosis_usdc_noble, _)) = app_state.config.lp_pools.get(2)
    {
        osmosis_usdc_noble
    } else {
        return Err(Error::ProtocolError(String::from("osmosis_usdc_noble")));
    };
    let data = app_state
        .database
        .ls_state
        .get_total_value_locked(
            osmosis_usdc.to_owned(),
            neutron_usdc_axelar.to_owned(),
            osmosis_usdc_noble.to_owned(),
        )
        .await?;
    let cache = &app_state.clone().cache;
    let cache = cache.lock();

    if let Ok(mut c) = cache {
        c.total_value_locked = Some(data);
    }

    Ok(())
}

pub async fn cache_state_tasks(app_state: AppState<State>) -> Result<(), Error> {
    let interval: u64 = app_state.config.cache_state_interval.into();
    let interval: u64 = interval * 60;

    let mut interval = time::interval(Duration::from_secs(interval));
    tokio::spawn(async move {
        tokio::try_join!(set_total_value_locked(app_state.clone()),)?;

        interval.tick().await;
        loop {
            interval.tick().await;
            tokio::try_join!(set_total_value_locked(app_state.clone()),)?;
        }
    })
    .await?
}
