#[cfg(feature = "mysql")]
mod mysql;

#[cfg(feature = "mysql")]
pub use mysql::{get_path, DBRow, DataBase, PoolOption, PoolType, QueryResult};

#[cfg(feature = "postgres")]
mod postgre;

#[cfg(feature = "postgres")]
pub use postgre::{get_path, DBRow, DataBase, PoolOption, PoolType, QueryResult};
