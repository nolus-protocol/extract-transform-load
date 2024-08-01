use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct MsgReceivePacket {
    pub amount: String,
    pub denom: String,
    pub receiver: String,
    pub sender: String,
}
