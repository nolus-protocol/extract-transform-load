use super::{BlockBody, NewBlockBody};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum BlockValue {
    Block(BlockBody),
    NewBlock(NewBlockBody),
}
