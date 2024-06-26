use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct BlockBody {
    pub jsonrpc: String,
    pub id: i64,
    pub result: BlockResult,
    pub error: Option<BodyError>,
}

#[derive(Debug, Deserialize)]
pub struct BlockResult {
    pub height: String,
    pub txs_results: Option<Vec<TXS_RESULTS>>,
}

#[derive(Debug, Deserialize)]
pub struct TXS_RESULTS {
    pub events: Option<Vec<EventData>>,
}

#[derive(Debug, Deserialize)]
pub struct EventData {
    pub r#type: String,
    pub attributes: Vec<Attributes>,
}

#[derive(Debug, Deserialize)]
pub struct Attributes {
    pub key: String,
    pub value: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct BodyError {
    pub code: String,
    pub message: String,
    pub data: String,
}
