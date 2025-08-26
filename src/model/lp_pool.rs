use sqlx::FromRow;
#[derive(Debug, FromRow)]
pub struct LP_Pool {
    pub LP_Pool_id: String,
    pub LP_symbol: String,
    pub LP_status: bool,
}
