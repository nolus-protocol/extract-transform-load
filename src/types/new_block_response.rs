use super::EventData;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct NewBlockBody {
    pub jsonrpc: String,
    pub id: i64,
    pub result: NewBlockResult,
}

#[derive(Debug, Deserialize)]
pub struct NewBlockResult {
    pub query: Option<String>,
    pub data: Option<NewBlockData>,
}

#[derive(Debug, Deserialize)]
pub struct NewBlockData {
    pub r#type: String,
    pub value: Block,
}

#[derive(Debug, Deserialize)]
pub struct Block {
    pub block: Header,
    pub result_begin_block: ResultBeginBlock,
}

#[derive(Debug, Deserialize)]
pub struct Header {
    pub header: NewBlockHeaderData,
}

#[derive(Debug, Deserialize)]
pub struct NewBlockHeaderData {
    pub height: String,
}

#[derive(Debug, Deserialize)]
pub struct ResultBeginBlock {
    pub events: Option<Vec<EventData>>,
}
