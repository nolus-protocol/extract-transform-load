use sqlx::{
    mysql::{MySqlPool, MySqlPoolOptions, MySqlQueryResult, MySqlRow},
    MySql,
};

pub type PoolType = MySqlPool;
pub type PoolOption = MySqlPoolOptions;
pub type DBRow = MySqlRow;
pub type QueryResult = MySqlQueryResult;
pub type DataBase = MySql;
