use sqlx::{
    postgres::{PgPoolOptions, PgQueryResult, PgRow},
    PgPool, Postgres,
};

pub type PoolType = PgPool;
pub type PoolOption = PgPoolOptions;
pub type DBRow = PgRow;
pub type QueryResult = PgQueryResult;
pub type DataBase = Postgres;
