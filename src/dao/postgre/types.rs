use sqlx::{PgPool, postgres::{PgPoolOptions, PgRow, PgQueryResult}, Postgres};

pub type PoolType = PgPool;
pub type PoolOption = PgPoolOptions;
pub type DBRow = PgRow;
pub type QueryResult = PgQueryResult;
pub type DataBase = Postgres;