#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

use std::{future::Future, num::NonZeroUsize};

use tokio::task::{JoinError, JoinSet};

pub mod configuration;
pub mod controller;
pub mod dao;
pub mod error;
pub mod handler;
pub mod helpers;
pub mod custom_uint;
pub mod model;
pub mod provider;
pub mod server;
pub mod types;

pub async fn try_join<Iterable, Collection, Err, FutureOk, FutureErr>(
    iterable: Iterable,
) -> Result<Collection, Err>
where
    Iterable: IntoIterator,
    Iterable::Item:
        Future<Output = Result<FutureOk, FutureErr>> + Send + 'static,
    Collection: Default + Extend<FutureOk>,
    FutureOk: Send + 'static,
    FutureErr: Into<Err> + Send + 'static,
    JoinError: Into<Err>,
{
    let mut set: JoinSet<_> = iterable.into_iter().collect();

    let mut collection = Collection::default();

    while let Some(result) = set.join_next().await {
        collection.extend([result.map_err(Into::into)?.map_err(Into::into)?]);
    }

    Ok(collection)
}

pub async fn try_join_with_capacity<
    Iterable,
    Collection,
    Err,
    FutureOk,
    FutureErr,
>(
    iterable: Iterable,
    capacity: NonZeroUsize,
) -> Result<Collection, Err>
where
    Iterable: IntoIterator,
    Iterable::Item:
        Future<Output = Result<FutureOk, FutureErr>> + Send + 'static,
    Collection: Default + Extend<FutureOk>,
    FutureOk: Send + 'static,
    FutureErr: Into<Err> + Send + 'static,
    JoinError: Into<Err>,
{
    let mut iter = iterable.into_iter().fuse();

    let mut set: JoinSet<_> = (&mut iter).take(capacity.get()).collect();

    let mut collection = Collection::default();

    while let Some(result) = set.join_next().await {
        collection.extend([result.map_err(Into::into)?.map_err(Into::into)?]);

        if let Some(future) = iter.next() {
            set.spawn(future);
        }
    }

    Ok(collection)
}
