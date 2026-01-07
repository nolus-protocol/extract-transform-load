use cosmrs::proto::{tendermint::abci::Event, Timestamp};
use cosmrs::Any;

/// Parameters for parsing a raw transaction
pub struct RawTxParams<'a> {
    pub tx_hash: String,
    pub tx_data: Any,
    pub height: i64,
    pub code: u32,
    pub time_stamp: Timestamp,
    pub tx_events: &'a [Event],
}
