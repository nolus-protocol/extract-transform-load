use sqlx::FromRow;

#[derive(Debug, FromRow)]
pub struct Block {
    pub id: i64,
}
