use std::time::Duration;

use anyhow::Context as _;
use futures::StreamExt as _;
use tendermint_rpc::{
    client::WebSocketClient, query::EventType, SubscriptionClient,
};
use tokio::{sync::mpsc, time::sleep};
use tracing::{error, info};

use etl_core::{
    configuration::{AppState, State},
    error::Error,
};

use crate::{
    event_dispatch::insert_txs, provider::synchronization::start_sync,
};

/// Wait before processing a new block to allow gRPC indexing to complete.
/// The WebSocket announces blocks before gRPC nodes finish indexing them.
const BLOCK_PROPAGATION_DELAY: Duration = Duration::from_secs(1);

/// Per-block retry attempts before skipping.
const BLOCK_MAX_RETRIES: u32 = 3;

/// If this many blocks fail consecutively, assume infrastructure is down
/// and break out to trigger a clean reconnect cycle.
const MAX_CONSECUTIVE_FAILURES: u32 = 10;

pub struct Event {
    app_state: AppState<State>,
}

impl Event {
    pub fn new(app_state: AppState<State>) -> Self {
        Self { app_state }
    }

    pub async fn run(&self) -> Result<(), Error> {
        if !self.app_state.config.enable_sync {
            return Ok(());
        }

        loop {
            // Spawn sync independently — errors logged, don't affect WS
            let sync_state = self.app_state.clone();
            tokio::spawn(async move {
                if let Err(e) = start_sync(sync_state).await {
                    error!("Sync error: {}", e);
                }
            });

            // Run WebSocket session with guaranteed cleanup
            if let Err(e) = self.run_session().await {
                error!("WebSocket session error: {}", e);
            }

            sleep(Duration::from_secs(
                self.app_state.config.socket_reconnect_interval,
            ))
            .await;
        }
    }

    /// Establishes a WebSocket connection, spawns a block consumer, and
    /// feeds block heights from the event stream into it. Connection
    /// cleanup is guaranteed regardless of how the session ends.
    async fn run_session(&self) -> Result<(), Error> {
        let url = self.app_state.config.websocket_host.as_str();

        let (client, driver) = WebSocketClient::new(url)
            .await
            .context("Unable to connect WebSocket")?;

        info!("WebSocket connected to {}", url);

        let driver_handle = tokio::spawn(async move { driver.run().await });

        let (height_tx, height_rx) =
            mpsc::channel::<(u64, AppState<State>)>(64);

        let consumer_handle = tokio::spawn(block_consumer(height_rx));

        // Run the producer inline — blocks until stream ends or error
        let result = self.produce_heights(&client, height_tx).await;

        // GUARANTEED CLEANUP — always runs regardless of how produce_heights exited
        // 1. height_tx is already dropped (moved into produce_heights and dropped on return)
        // 2. Close the WebSocket connection (sends Close frame via driver)
        if let Err(e) = client.close() {
            error!("Failed to close WebSocket: {}", e);
        }
        // 3. Wait for driver to finish the Close handshake
        if let Err(e) = driver_handle.await {
            error!("WebSocket driver error: {}", e);
        }
        // 4. Wait for consumer to drain remaining heights
        if let Err(e) = consumer_handle.await {
            error!("Block consumer task error: {}", e);
        }

        result
    }

    /// Thin event loop: subscribes to NewBlock events and sends block
    /// heights to the channel. Contains zero business logic.
    async fn produce_heights(
        &self,
        client: &WebSocketClient,
        height_tx: mpsc::Sender<(u64, AppState<State>)>,
    ) -> Result<(), Error> {
        let mut subs = client
            .subscribe(EventType::NewBlock.into())
            .await
            .context("Unable to subscribe to WebSocket events")?;

        while let Some(res) = subs.next().await {
            let ev = res.context("WebSocket event stream error")?;

            let height = match ev.data {
                tendermint_rpc::event::EventData::NewBlock {
                    block,
                    block_id: _,
                    result_finalize_block: _,
                } => block.map(|b| b.header.height.value()),
                tendermint_rpc::event::EventData::LegacyNewBlock {
                    block,
                    result_begin_block: _,
                    result_end_block: _,
                } => block.map(|b| b.header.height.value()),
                _ => continue,
            };

            let Some(height) = height else {
                error!("Block event missing block data");
                continue;
            };

            // If consumer dropped (broke out of loop), exit cleanly
            if height_tx
                .send((height, self.app_state.clone()))
                .await
                .is_err()
            {
                error!("Block consumer stopped, ending WebSocket session");
                break;
            }
        }

        Ok(())
    }
}

/// Processes blocks from the channel with propagation delay, per-block
/// retry, and a circuit breaker for consecutive failures.
async fn block_consumer(mut rx: mpsc::Receiver<(u64, AppState<State>)>) {
    let mut consecutive_failures: u32 = 0;

    while let Some((height, app_state)) = rx.recv().await {
        // Layer 1: Wait for gRPC node to index the block
        sleep(BLOCK_PROPAGATION_DELAY).await;

        let mut succeeded = false;

        for attempt in 1..=BLOCK_MAX_RETRIES {
            match process_block(height, &app_state).await {
                Ok(()) => {
                    succeeded = true;
                    break;
                },
                Err(e) => {
                    if attempt < BLOCK_MAX_RETRIES {
                        error!(
                            "Block {} processing failed (attempt {}/{}): {}",
                            height, attempt, BLOCK_MAX_RETRIES, e
                        );
                        // Layer 2: Increasing backoff doubles as additional
                        // propagation time for the gRPC node
                        sleep(Duration::from_secs(2 * attempt as u64)).await;
                    } else {
                        error!(
                            "Block {} failed after {} attempts, skipping: {}",
                            height, BLOCK_MAX_RETRIES, e
                        );
                    }
                },
            }
        }

        if succeeded {
            consecutive_failures = 0;
        } else {
            consecutive_failures += 1;
            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES {
                error!(
                    "Aborting block consumer after {} consecutive block failures",
                    consecutive_failures
                );
                // Drops rx → producer's send() returns Err → clean shutdown
                break;
            }
        }
    }
}

async fn process_block(
    height: u64,
    app_state: &AppState<State>,
) -> Result<(), Error> {
    let height: i64 = height.try_into()?;
    let (txs, time_stamp) = app_state.grpc.get_block(height).await?;
    insert_txs(app_state.clone(), txs, height, time_stamp).await?;
    Ok(())
}
