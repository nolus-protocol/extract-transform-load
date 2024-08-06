use crate::configuration::{AppState, State};
use crate::error::Error;
use crate::helpers::insert_txs;
use crate::provider::Grpc;

use anyhow::Context;
use futures::future::try_join_all;
use std::sync::atomic::{AtomicBool, Ordering};
use tracing::{error, info};

static RUNNING: AtomicBool = AtomicBool::new(false);

#[derive(Debug)]
pub struct Synchronization {}

impl Synchronization {
    pub fn is_running(&self) -> bool {
        let running = &RUNNING;
        running.load(Ordering::SeqCst)
    }

    pub fn set_running(&self, bool: bool) {
        let running = &RUNNING;
        running.store(bool, Ordering::SeqCst)
    }

    pub async fn get_params<'a>(
        &self,
        app_state: &AppState<State>,
    ) -> Result<(i16, Vec<(i64, i64)>), Error> {
        let block_model = &app_state.database.block;
        let first_block = block_model.get_first_block().await.ok();
        let last_block = block_model.get_last_block().await.ok();
        let block_height = app_state.grpc.get_latest_block().await?;
        let missing_values = block_model.get_missing_blocks().await?;
        let threads_count = app_state.config.sync_threads;

        let mut parts: Vec<(i64, i64)> = Vec::new();
        let start_block = 1;

        if first_block.is_none() {
            parts.push((start_block, block_height + 1));
        } else {
            if let Some((last,)) = last_block {
                parts.push((last + 1, block_height + 1));
            }

            for (start, end) in missing_values {
                parts.push((start + 1, end));
            }
        }

        Ok((threads_count, parts))
    }

    pub async fn run<'a>(
        &self,
        app_state: AppState<State>,
    ) -> Result<(), Error> {
        let (threads_count, parts) = self.get_params(&app_state).await?;

        if !self.is_running() {
            self.start_tasks(threads_count, parts, app_state.clone())
                .await?;
        }

        Ok(())
    }

    async fn start_tasks<'a>(
        &self,
        threads_count: i16,
        mut parts: Vec<(i64, i64)>,
        app_state: AppState<State>,
    ) -> Result<(), Error> {
        let mut thread_parts: Vec<Vec<(i64, i64)>> =
            vec![vec![]; (threads_count - 1) as usize];
        let mut hs = Vec::new();

        for range in &mut parts {
            let count = (range.1 - range.0) / threads_count as i64;

            for i in 0..threads_count - 1 {
                let start = range.0;
                let end = range.0 + count;
                let p = thread_parts.get_mut(i as usize);
                if let Some(part) = p {
                    part.push((start, end));
                    range.0 += count;
                }
            }
        }

        thread_parts.push(parts);

        for p in thread_parts {
            let config = app_state.clone();
            let mut child_total = 0;

            for (start, end) in &p {
                child_total = child_total + end - start;
            }

            if child_total > 0 {
                self.set_running(true);

                hs.push(tokio::spawn(async move {
                    let mut handler = Handler::new(config).await?;
                    handler.init(p).await
                }));
            }
        }

        for h in try_join_all(hs).await? {
            h?;
        }

        self.set_running(false);
        println!();

        Ok(())
    }
}

#[derive(Debug)]
struct Handler {
    pub app_state: AppState<State>,
    pub grpc: Grpc,
}

impl Handler {
    pub async fn new(app_state: AppState<State>) -> Result<Self, Error> {
        let config = app_state.config.clone();
        let grpc: Grpc =
            Grpc::new(config).await.context("unable to start grpc")?;
        Ok(Handler { app_state, grpc })
    }
    pub async fn init(&mut self, parts: Vec<(i64, i64)>) -> Result<(), Error> {
        for range in &parts {
            let (start, end) = range;
            let r = *start..*end;
            for height in r {
                self.insert_tx(height).await?;
            }
        }

        Ok(())
    }

    async fn insert_tx(&mut self, height: i64) -> Result<(), Error> {
        let (txs, time_stamp) = self.grpc.get_block(height).await?;
        insert_txs(self.app_state.clone(), txs, height, time_stamp).await?;
        Ok(())
    }
}

pub async fn start_sync(app_state: AppState<State>) -> Result<(), Error> {
    tokio::spawn(async {
        let sync_manager = Synchronization {};
        match sync_manager.run(app_state).await {
            Ok(()) => {
                sync_manager.set_running(false);
                info!("Synchronization completed");
            },
            Err(e) => {
                sync_manager.set_running(false);
                error!("Synchronization error {}", e);

                return Err(e);
            },
        };
        Ok(())
    })
    .await?
}
