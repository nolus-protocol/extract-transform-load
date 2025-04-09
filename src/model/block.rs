use sqlx::FromRow;

#[derive(Debug, FromRow)]
pub struct Block {
    // FIXME Use `UInt63` instead for block height.
    pub id: i64,
}
