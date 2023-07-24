use crate::configuration::{AppState, State};
use crate::error::Error;
use crate::helpers;
use crate::types::BlockBody;

use futures::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use tracing::{info, error};
use std::process::exit;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use tokio::net::TcpStream;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream,
    {
        connect_async,
        tungstenite::{error::Error as WS_ERROR, Message},
    },
};
use url::Url;

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
    ) -> Result<(i16, Vec<(i64, i64)>, i64), Error> {
        let block_model = &app_state.database.block;
        let first_block = block_model.get_first_block().await.ok();
        let last_block = block_model.get_last_block().await.ok();
        let block_height = app_state.http.get_latest_block().await?;
        let missing_values = block_model.get_missing_blocks().await?;
        let threads_count = app_state.config.sync_threads;

        let mut parts: Vec<(i64, i64)> = Vec::new();
        let start_block = 1;
        let mut total = 0;

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

        for (start, end) in &parts {
            total = total + end - start;
        }

        Ok((threads_count, parts, total))
    }

    pub async fn run<'a>(&self, app_state: AppState<State>) -> Result<(), Error> {
        let (threads_count, parts, total) = self.get_params(&app_state).await?;

        if !self.is_running() {
            self.start_tasks(threads_count, parts, total, app_state.clone())
                .await?;
        }

        Ok(())
    }

    async fn start_tasks<'a>(
        &self,
        threads_count: i16,
        mut parts: Vec<(i64, i64)>,
        total: i64,
        app_state: AppState<State>,
    ) -> Result<(), Error> {
        let mut thread_parts: Vec<Vec<(i64, i64)>> = vec![vec![]; (threads_count - 1) as usize];
        let mut hs = Vec::new();
        let counter = Arc::new(AtomicI64::new(0));

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
            let counter = counter.clone();
            let mut child_total = 0;

            for (start, end) in &p {
                child_total = child_total + end - start;
            }

            if child_total > 0 {
                self.set_running(true);

                hs.push(tokio::spawn(async move {
                    let mut handler = Handler::new(config);
                    handler.init(p, counter, total).await
                }));
            }
        }

        for h in hs {
            h.await??;
        }

        self.set_running(false);
        println!();

        Ok(())
    }
}

#[derive(Debug)]
struct Handler {
    id: u64,
    pub app_state: AppState<State>,
}

impl Handler {
    pub fn new(app_state: AppState<State>) -> Self {
        Handler { app_state, id: 1 }
    }
    async fn init(
        &mut self,
        mut parts: Vec<(i64, i64)>,
        counter: Arc<AtomicI64>,
        total: i64,
    ) -> Result<(), Error> {
        let url = Url::parse(self.app_state.config.websocket_host.as_str())?;
        let (socket, _response) = connect_async(url).await?;
        let (mut write, mut read) = socket.split();

        self.stream_handler(&mut parts, &mut write, &mut read, counter, total)
            .await?;

        Ok(())
    }

    async fn stream_handler(
        &mut self,
        parts: &mut Vec<(i64, i64)>,
        write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        read: &mut SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
        counter: Arc<AtomicI64>,
        total: i64,
    ) -> Result<(), Error> {
        self.set_tasks(parts, write, counter.clone(), total).await?;
        loop {
            match self.parse_message(read.next().await).await {
                Ok(proceed) => {
                    if proceed && !self.set_tasks(parts, write, counter.clone(), total).await? {
                        break;
                    }
                }
                Err(e) => return Err(Error::ParseMessage(e.to_string())),
            }
        }

        Ok(())
    }

    async fn set_tasks(
        &mut self,
        parts: &mut Vec<(i64, i64)>,
        write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        counter: Arc<AtomicI64>,
        _total: i64,
    ) -> Result<bool, Error> {

        for mut range in &mut *parts {
            let (start, end) = range;
            let mut r = *start..*end;
            if let Some(i) = r.next() {
                let id = self.get_id();
                let event = self.app_state.config.block_results_event(i, id);
                write.send(Message::Text(event)).await?;
                counter.fetch_add(1, Ordering::SeqCst);
      
                range.0 += 1;
                return Ok(true);
            }
        }

        Ok(false)
    }

    async fn parse_message(
        &self,
        message: Option<Result<Message, WS_ERROR>>,
    ) -> Result<bool, Error> {
        if let Some(message) = message {
            let message = message;
            match message {
                Ok(msg_obj) => {
                    match msg_obj {
                        Message::Text(msg_obj) => {
                            return self.to_json(msg_obj).await;
                        }
                        Message::Binary(_)
                        | Message::Ping(_)
                        | Message::Pong(_)
                        | Message::Close(_)
                        | Message::Frame(_) => {}
                    };
                }
                Err(e) => return Err(Error::WS(e)),
            }
        };
        Ok(false)
    }

    async fn to_json(&self, message: String) -> Result<bool, Error> {
        let item = serde_json::from_str::<BlockBody>(&message)?;
        helpers::insert_block(self.app_state.clone(), item).await
    }

    fn get_id(&mut self) -> u64 {
        let id = self.id;
        self.id += 1;
        id
    }
}

pub fn start_sync(app_state: AppState<State>) {
    tokio::spawn(async {
        let sync_manager = Synchronization {};
        let result = sync_manager.run(app_state).await;
        match result {
            Ok(_) => {
                info!("Synchronization completed")
            }
            Err(error) => {
                error!("Synchronization end with error: {}", error);
                exit(1);
            }
        }
    });
}
