use cosmrs::proto::{tendermint::abci::Event, Timestamp};
use cosmrs::{tx::Fee, Any};

/// Parameters for constructing a Raw_Message from Any
pub struct RawMsgParams<'a> {
    pub index: i32,
    pub value: Any,
    pub tx_hash: String,
    pub block: i64,
    pub time_stamp: Timestamp,
    pub fee: Fee,
    pub memo: String,
    pub events: Vec<String>,
    pub tx_events: &'a [Event],
    pub code: u32,
}
