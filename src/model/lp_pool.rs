use sqlx::FromRow;

#[derive(Debug, FromRow)]
pub struct LP_Pool<Str>
where
    Str: AsRef<str>,
{
    pub LP_Pool_id: Str,
    pub LP_symbol: Str,
}
