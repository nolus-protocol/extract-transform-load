use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct BlockQuery {
    pub jsonrpc: String,
    pub id: i64,
    pub result: BlockResult,
    pub error: Option<BodyError>,
}

#[derive(Debug, Deserialize)]
pub struct BlockResult {
    pub block: BlockHeader,
    pub data: BlockData,
}

#[derive(Debug, Deserialize)]
pub struct BlockHeader {
    pub height: String,
}

#[derive(Debug, Deserialize)]
pub struct BlockData {
    pub txs: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct BodyError {
    pub code: String,
    pub message: String,
    pub data: String,
}
