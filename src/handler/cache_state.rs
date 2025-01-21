use std::{borrow::Cow, sync::RwLock};

use bigdecimal::BigDecimal;
use tokio::{time, time::Duration};

use crate::{
    configuration::{Cache, State},
    error::Error,
};

#[cfg(feature = "mainnet")]
pub async fn set_total_value_locked(app_state: &State) -> Result<(), Error> {
    let neutron_usdc_noble = &app_state
        .config
        .lp_pools
        .get(0)
        .ok_or_else(|| {
            Error::ProtocolError(Cow::Borrowed("neutron_usdc_noble"))
        })?
        .0;

    let osmosis_usdc = &app_state
        .config
        .lp_pools
        .get(1)
        .ok_or_else(|| Error::ProtocolError(Cow::Borrowed("osmosis_usdc")))?
        .0;

    let neutron_usdc_axelar = &app_state
        .config
        .lp_pools
        .get(2)
        .ok_or_else(|| {
            Error::ProtocolError(Cow::Borrowed("neutron_usdc_axelar"))
        })?
        .0;

    let osmosis_usdc_noble = &app_state
        .config
        .lp_pools
        .get(3)
        .ok_or_else(|| {
            Error::ProtocolError(Cow::Borrowed("osmosis_usdc_noble"))
        })?
        .0;

    let osmosis_st_atom = &app_state
        .config
        .lp_pools
        .get(4)
        .ok_or_else(|| Error::ProtocolError(Cow::Borrowed("osmosis_st_atom")))?
        .0;

    let osmosis_all_btc = &app_state
        .config
        .lp_pools
        .get(5)
        .ok_or_else(|| Error::ProtocolError(Cow::Borrowed("osmosis_all_btc")))?
        .0;

    let osmosis_all_sol = &app_state
        .config
        .lp_pools
        .get(6)
        .ok_or_else(|| Error::ProtocolError(Cow::Borrowed("osmosis_all_sol")))?
        .0;

    let osmosis_akt = &app_state
        .config
        .lp_pools
        .get(7)
        .ok_or_else(|| Error::ProtocolError(Cow::Borrowed("osmosis_akt")))?
        .0;

    let data = app_state
        .database
        .ls_state
        .get_total_value_locked(
            osmosis_usdc,
            neutron_usdc_axelar,
            osmosis_usdc_noble,
            neutron_usdc_noble,
            osmosis_st_atom,
            osmosis_all_btc,
            osmosis_all_sol,
            osmosis_akt,
        )
        .await?;

    write_tvl_to_cache(&app_state.cache, data);

    Ok(())
}

#[cfg(feature = "testnet")]
pub async fn set_total_value_locked(app_state: &State) -> Result<(), Error> {
    let neutron_usdc_axelar = &app_state
        .config
        .lp_pools
        .get(0)
        .ok_or_else(|| {
            Error::ProtocolError(Cow::Borrowed("neutron_usdc_axelar"))
        })?
        .0;

    let osmosis_usdc = &app_state
        .config
        .lp_pools
        .get(1)
        .ok_or_else(|| Error::ProtocolError(Cow::Borrowed("osmosis_usdc")))?
        .0;

    let data = app_state
        .database
        .ls_state
        .get_total_value_locked(&osmosis_usdc, &neutron_usdc_axelar)
        .await?;

    write_tvl_to_cache(&app_state.cache, data);

    Ok(())
}

pub async fn cache_state_tasks(app_state: &State) -> Result<(), Error> {
    let mut interval = time::interval(Duration::from_secs(
        u64::from(app_state.config.cache_state_interval) * 60,
    ));

    tokio::spawn(async move {
        loop {
            set_total_value_locked(app_state.clone()).await?;

            interval.tick().await;
        }
    })
    .await?
}

fn write_tvl_to_cache(cache_lock: &RwLock<Cache>, data: BigDecimal) {
    match cache_lock.write() {
        Ok(mut guard) => {
            guard.total_value_locked = Some(data);
        },
        Err(poisoned) => {
            let mut guard = poisoned.into_inner();

            guard.total_value_locked = Some(data);

            cache_lock.clear_poison();

            drop(guard);
        },
    }
}
