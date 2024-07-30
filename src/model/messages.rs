use sqlx::FromRow;

#[derive(Debug, FromRow)]
pub struct Message {
    pub sender: String,
    pub contract: String,
}
