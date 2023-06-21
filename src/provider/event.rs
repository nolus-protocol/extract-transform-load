use super::synchronization::start_sync;
use crate::error::Error;
use crate::helpers::parse_event;
use crate::model::Block;
use crate::types::{NewBlockBody, NewBlockData};
use crate::{
    configuration::{AppState, State},
    helpers::MessageType,
};
use futures::{SinkExt, StreamExt};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{error::Error as WS_ERROR, Message},
};
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

            start_sync(app);
            if let Err(e) = self.init().await {
                eprintln!("WS disconnected with error: {}, try to reconnecting...", e);
            }
        }
    }

    async fn init(&mut self) -> Result<(), Error> {
        println!("WS connect successfully");

        let url = Url::parse(self.app_state.config.websocket_host.as_str())?;
        let (socket, _response) = connect_async(url).await?;
        let (mut write, mut read) = socket.split();

        let id = self.get_id();
        let new_block_event = self.app_state.config.new_block_event(id);

        write.send(Message::Text(new_block_event)).await?;

        loop {
            self.parse_message(read.next().await).await?;
        }
    }

    fn get_id(&mut self) -> u64 {
        let id = self.id;
        self.id += 1;
        id
    }

    async fn parse_message(&self, message: Option<Result<Message, WS_ERROR>>) -> Result<(), Error> {
        if let Some(message) = message {
            let message = message;
            match message {
                Ok(msg_obj) => {
                    match msg_obj {
                        Message::Text(msg_obj) => {
                            self.to_json(msg_obj).await?;
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

    async fn to_json(&self, message: String) -> Result<(), Error> {
        let item = serde_json::from_str::<NewBlockBody>(&message)?;

        if let Some(block) = item.result.data {
            self.check_message(block).await?;
        }

        Ok(())
    }

    async fn check_message(&self, data: NewBlockData) -> Result<(), Error> {
        let msg_type = data.r#type.parse::<MessageType>()?;
        match msg_type {
            MessageType::NewEvent => self.insert_block(data).await?,
        }
        Ok(())
    }

    async fn insert_block(&self, data: NewBlockData) -> Result<(), Error> {
        let mut tx = self.app_state.database.pool.begin().await?;

        if let Some(events) = data.value.result_begin_block.events {
            for event in events {
                parse_event(self.app_state.clone(), event, &mut tx).await?;
            }
        }

        let height = data.value.block.header.height.parse::<i64>()?;
        self.app_state
            .database
            .block
            .insert(Block { id: height }, &mut tx)
            .await?;

        tx.commit().await?;

        Ok(())
    }
}
