use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct AbciBody {
    pub jsonrpc: String,
    pub id: i64,
    pub result: AbciDataResult,
}

#[derive(Debug, Deserialize)]
pub struct AbciDataResult {
    pub response: AbciDataResponse,
}

#[derive(Debug, Deserialize)]
pub struct AbciDataResponse {
    pub data: String,
    pub version: String,
    pub last_block_height: String,
    pub last_block_app_hash: String,
}
