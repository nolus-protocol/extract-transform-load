use tokio::time::{self, Duration};

use crate::{
    configuration::{AppState, State},
    error::Error,
};

#[cfg(feature = "mainnet")]
pub async fn set_total_value_locked(
    app_state: AppState<State>,
) -> Result<(), Error> {
    let neutron_usdc_noble = if let Some((neutron_usdc_noble, _, _, _)) =
        app_state.config.lp_pools.get(0)
    {
        neutron_usdc_noble
    } else {
        return Err(Error::ProtocolError(String::from("neutron_usdc_noble")));
    };

    let osmosis_usdc = if let Some((osmosis_usdc, _, _, _)) =
        app_state.config.lp_pools.get(1)
    {
        osmosis_usdc
    } else {
        return Err(Error::ProtocolError(String::from("osmosis_usdc")));
    };

    let neutron_usdc_axelar = if let Some((neutron_usdc_axelar, _, _, _)) =
        app_state.config.lp_pools.get(2)
    {
        neutron_usdc_axelar
    } else {
        return Err(Error::ProtocolError(String::from("neutron_usdc_axelar")));
    };

    let osmosis_usdc_noble = if let Some((osmosis_usdc_noble, _, _, _)) =
        app_state.config.lp_pools.get(3)
    {
        osmosis_usdc_noble
    } else {
        return Err(Error::ProtocolError(String::from("osmosis_usdc_noble")));
    };

    let osmosis_st_atom = if let Some((osmosis_st_atom, _, _, _)) =
        app_state.config.lp_pools.get(4)
    {
        osmosis_st_atom
    } else {
        return Err(Error::ProtocolError(String::from("osmosis_st_atom")));
    };

    let osmosis_all_btc = if let Some((osmosis_all_btc, _, _, _)) =
        app_state.config.lp_pools.get(5)
    {
        osmosis_all_btc
    } else {
        return Err(Error::ProtocolError(String::from("osmosis_all_btc")));
    };

    let osmosis_all_sol = if let Some((osmosis_all_sol, _, _, _)) =
        app_state.config.lp_pools.get(6)
    {
        osmosis_all_sol
    } else {
        return Err(Error::ProtocolError(String::from("osmosis_all_sol")));
    };

    let osmosis_akt = if let Some((osmosis_akt, _, _, _)) =
        app_state.config.lp_pools.get(7)
    {
        osmosis_akt
    } else {
        return Err(Error::ProtocolError(String::from("osmosis_akt")));
    };

    let data = app_state
        .database
        .ls_state
        .get_total_value_locked(
            osmosis_usdc.to_owned(),
            neutron_usdc_axelar.to_owned(),
            osmosis_usdc_noble.to_owned(),
            neutron_usdc_noble.to_owned(),
            osmosis_st_atom.to_owned(),
            osmosis_all_btc.to_owned(),
            osmosis_all_sol.to_owned(),
            osmosis_akt.to_owned(),
        )
        .await?;
    let cache = &app_state.clone().cache;
    let cache = cache.lock();

    if let Ok(mut c) = cache {
        c.total_value_locked = Some(data);
    }

    Ok(())
}

#[cfg(feature = "testnet")]
pub async fn set_total_value_locked(
    app_state: AppState<State>,
) -> Result<(), Error> {
    let neutron_usdc_axelar = if let Some((neutron_usdc_axelar, _, _)) =
        app_state.config.lp_pools.get(0)
    {
        neutron_usdc_axelar
    } else {
        return Err(Error::ProtocolError(String::from("neutron_usdc_axelar")));
    };

    let osmosis_axelar_usdc = if let Some((osmosis_axelar_usdc, _, _)) =
        app_state.config.lp_pools.get(1)
    {
        osmosis_axelar_usdc
    } else {
        return Err(Error::ProtocolError(String::from("osmosis_usdc_axelar")));
    };

    let data = app_state
        .database
        .ls_state
        .get_total_value_locked(
            osmosis_axelar_usdc.to_owned(),
            neutron_usdc_axelar.to_owned(),
        )
        .await?;
    let cache = &app_state.clone().cache;
    let cache = cache.lock();

    if let Ok(mut c) = cache {
        c.total_value_locked = Some(data);
    }

    Ok(())
}

#[cfg(feature = "devnet")]
pub async fn set_total_value_locked(
    app_state: AppState<State>,
) -> Result<(), Error> {
    let neutron_usdc_axelar = if let Some((neutron_usdc_axelar, _, _, _)) =
        app_state.config.lp_pools.get(0)
    {
        neutron_usdc_axelar
    } else {
        return Err(Error::ProtocolError(String::from("neutron_usdc_axelar")));
    };

    let osmosis_axelar_usdc = if let Some((osmosis_axelar_usdc, _, _, _)) =
        app_state.config.lp_pools.get(1)
    {
        osmosis_axelar_usdc
    } else {
        return Err(Error::ProtocolError(String::from("osmosis_axelar_usdc")));
    };

    let data = app_state
        .database
        .ls_state
        .get_total_value_locked(
            osmosis_axelar_usdc.to_owned(),
            neutron_usdc_axelar.to_owned(),
        )
        .await?;
    let cache = &app_state.clone().cache;
    let cache = cache.lock();

    if let Ok(mut c) = cache {
        c.total_value_locked = Some(data);
    }

    Ok(())
}

pub async fn cache_state_tasks(
    app_state: AppState<State>,
) -> Result<(), Error> {
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
