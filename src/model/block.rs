use sqlx::FromRow;

use crate::custom_uint::UInt63;

#[derive(Debug, FromRow)]
pub struct Block {
    pub id: UInt63,
}
