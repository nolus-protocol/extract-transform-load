use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct QueryBody {
    pub jsonrpc: String,
    pub id: i64,
    pub result: Response,
}

#[derive(Debug, Deserialize)]
pub struct Response {
    pub response: Params,
}

#[derive(Debug, Deserialize)]
pub struct Params {
    pub height: String,
    pub value: Option<String>,
    pub log: Option<String>
}
