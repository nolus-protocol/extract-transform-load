use std::time::Duration;

use super::synchronization::start_sync;
use crate::configuration::{AppState, State};
use crate::error::Error;
use crate::helpers;
use crate::types::BlockValue;
use futures::stream::SplitSink;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::time::sleep;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
// use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::{error::Error as WS_ERROR, Message};
use tokio_tungstenite::{connect_async_with_config, MaybeTlsStream, WebSocketStream};
use tracing::{error, info};
use url::Url;

#[derive(Debug)]
pub struct Event {
    id: u64,
    app_state: AppState<State>,
}

impl Event {
    pub fn new(app_state: AppState<State>) -> Self {
        let id = 1;
        Self { id, app_state }
    }

    pub async fn run(&mut self) -> Result<(), Error> {
        loop {
            let app = self.app_state.clone();

            let res = tokio::try_join!(start_sync(app), self.init());

            if let Err(e) = res {
                error!("WS disconnected with error {}, try to reconnecting...", e);
            }

            sleep(Duration::from_secs(
                self.app_state.config.socket_reconnect_interval,
            ))
            .await;
        }
    }

    async fn init(&mut self) -> Result<(), Error> {
        info!("WS connect successfully");

        let url = Url::parse(self.app_state.config.websocket_host.as_str())?;
        let (socket, _response) = connect_async_with_config(
            url,
            Some(WebSocketConfig {
                max_send_queue: None,
                write_buffer_size: 256 * 1024,
                max_write_buffer_size: usize::MAX,
                max_message_size: Some(256 << 20),
                max_frame_size: Some(64 << 20),
                accept_unmasked_frames: false,
            }),
            false,
        )
        .await?;
        let (mut write, mut read) = socket.split();

        let id = self.get_id();
        let new_block_event = self.app_state.config.new_block_event(id);

        write.send(Message::Text(new_block_event)).await?;

        loop {
            if (self.parse_message(read.next().await, &mut write).await).is_err() {
                return Err(Error::ServerError(String::from("WS disconnected")));
            };
        }
    }

    fn get_id(&mut self) -> u64 {
        let id = self.id;
        self.id += 1;
        id
    }

    async fn parse_message(
        &self,
        message: Option<Result<Message, WS_ERROR>>,
        write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
    ) -> Result<(), Error> {
        if let Some(message) = message {
            let message = message;
            match message {
                Ok(msg_obj) => {
                    match msg_obj {
                        Message::Text(msg_obj) => {
                            self.to_json(msg_obj, write).await?;
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
        Ok(())
    }

    async fn to_json(
        &self,
        message: String,
        write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
    ) -> Result<(), Error> {
        let item = serde_json::from_str::<BlockValue>(&message)?;

        match item {
            BlockValue::Block(block) => {
                helpers::insert_block(self.app_state.clone(), block).await?;
            }
            BlockValue::NewBlock(block) => {
                if let Some(item) = block.result.data {
                    let height = item.value.block.header.height;
                    let event = self
                        .app_state
                        .config
                        .block_results_event(height.parse()?, 0);
                    write.send(Message::Text(event)).await?;
                }
            }
        }

        Ok(())
    }
}
