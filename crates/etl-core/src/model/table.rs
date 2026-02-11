use std::marker::{self, PhantomData};

use crate::dao::PoolType;

#[derive(Debug)]
pub struct Table<T> {
    pub pool: PoolType,
    _phantomdata: marker::PhantomData<T>,
}

impl<T> Table<T> {
    pub fn new(pool: PoolType) -> Self {
        Table {
            pool,
            _phantomdata: PhantomData,
        }
    }
}
