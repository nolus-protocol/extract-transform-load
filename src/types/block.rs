use serde::Deserialize;

use super::{BlockBody, NewBlockBody};

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum BlockValue {
    Block(BlockBody),
    NewBlock(NewBlockBody),
}
