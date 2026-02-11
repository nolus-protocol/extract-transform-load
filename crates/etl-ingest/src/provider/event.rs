use std::time::Duration;

use anyhow::Context as _;
use futures::StreamExt as _;
use tendermint_rpc::{
    client::WebSocketClient, query::EventType, SubscriptionClient,
};
use tokio::time::sleep;
use tracing::{error, info};

use etl_core::{
    configuration::{AppState, State},
    error::Error,
};

use crate::{
    event_dispatch::insert_txs, provider::synchronization::start_sync,
};

pub struct Event {
    app_state: AppState<State>,
}

impl Event {
    pub fn new(app_state: AppState<State>) -> Self {
        Self { app_state }
    }

    pub async fn run(&mut self) -> Result<(), Error> {
        if !self.app_state.config.enable_sync {
            return Ok(());
        }

        loop {
            let app = self.app_state.clone();

            if let Err(e) = tokio::try_join!(self.init(), start_sync(app)) {
                error!("{}", e);
            }

            sleep(Duration::from_secs(
                self.app_state.config.socket_reconnect_interval,
            ))
            .await;
        }
    }

    async fn init(&mut self) -> Result<(), Error> {
        info!("WS connect successfully");
        let req = self.app_state.config.websocket_host.as_str();

        let (client, driver) = WebSocketClient::new(req)
            .await
            .context("Unable to run webscoket")?;

        let driver_handle = tokio::spawn(async move { driver.run().await });

        let mut subs = client
            .subscribe(EventType::NewBlock.into())
            .await
            .context("Unable to subscrive websocket events")?;

        while let Some(res) = subs.next().await {
            let ev = res.context("unable to parse event")?;
            match ev.data {
                tendermint_rpc::event::EventData::NewBlock {
                    block,
                    block_id: _,
                    result_finalize_block: _,
                } => {
                    self.insert_tx(
                        block
                            .map(|item| item.header.height.value())
                            .context("unable to parse header")?,
                    )
                    .await?;
                },
                tendermint_rpc::event::EventData::LegacyNewBlock {
                    block,
                    result_begin_block: _,
                    result_end_block: _,
                } => {
                    self.insert_tx(
                        block
                            .map(|item| item.header.height.value())
                            .context("unable to parse header")?,
                    )
                    .await?;
                },
                tendermint_rpc::event::EventData::Tx { tx_result: _ } => {},
                tendermint_rpc::event::EventData::GenericJsonEvent(_) => {},
            };
        }

        client.close().context("unable to close websocket")?;
        driver_handle
            .await?
            .context("unable to handle spawn websocket")?;

        Ok(())
    }

    async fn insert_tx(&mut self, height: u64) -> Result<(), Error> {
        let height = height.try_into()?;
        let (txs, time_stamp) = self.app_state.grpc.get_block(height).await?;
        insert_txs(self.app_state.clone(), txs, height, time_stamp).await?;
        Ok(())
    }
}
