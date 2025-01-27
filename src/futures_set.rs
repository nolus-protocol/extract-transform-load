use std::{future::Future, num::NonZeroUsize};

use tokio::task::{JoinError, JoinSet};

#[inline]
pub fn try_join_all<Iterable, FutureErr, MapErr, Err>(
    iterable: Iterable,
) -> impl Future<Output = Result<(), Err>> + use<Iterable, FutureErr, MapErr, Err>
where
    Iterable: IntoIterator,
    Iterable::Item: Future<Output = Result<(), FutureErr>> + Send + 'static,
    FutureErr: Into<Err> + Send + 'static,
    JoinError: Into<Err>,
{
    try_join_all_mapping_err(iterable, Into::into)
}

#[inline]
pub fn try_join_all_mapping_err<Iterable, FutureErr, MapErr, Err>(
    iterable: Iterable,
    map_err: MapErr,
) -> impl Future<Output = Result<(), Err>> + use<Iterable, FutureErr, MapErr, Err>
where
    Iterable: IntoIterator,
    Iterable::Item: Future<Output = Result<(), FutureErr>> + Send + 'static,
    FutureErr: Send + 'static,
    MapErr: FnOnce(FutureErr) -> Err,
    JoinError: Into<Err>,
{
    try_join_all_folding_mapping_err(iterable, (), |(), ()| (), map_err)
}

pub fn try_join_all_folding<
    Iterable,
    FutureOk,
    FutureErr,
    Accumulator,
    FoldWith,
    Err,
>(
    iterable: Iterable,
    accumulator: Accumulator,
    fold_with: FoldWith,
) -> impl Future<Output = Result<Accumulator, Err>>
       + use<Iterable, FutureOk, FutureErr, Accumulator, FoldWith, Err>
where
    Iterable: IntoIterator,
    Iterable::Item:
        Future<Output = Result<FutureOk, FutureErr>> + Send + 'static,
    FutureOk: Send + 'static,
    FutureErr: Into<Err> + Send + 'static,
    FoldWith: FnMut(Accumulator, FutureOk) -> Accumulator,
    JoinError: Into<Err>,
{
    try_join_all_folding_mapping_err(
        iterable,
        accumulator,
        fold_with,
        Into::into,
    )
}

pub fn try_join_all_folding_mapping_err<
    Iterable,
    FutureOk,
    FutureErr,
    Accumulator,
    FoldWith,
    MapErr,
    Err,
>(
    iterable: Iterable,
    accumulator: Accumulator,
    fold_with: FoldWith,
    map_err: MapErr,
) -> impl Future<Output = Result<Accumulator, Err>>
       + use<Iterable, FutureOk, FutureErr, Accumulator, FoldWith, MapErr, Err>
where
    Iterable: IntoIterator,
    Iterable::Item:
        Future<Output = Result<FutureOk, FutureErr>> + Send + 'static,
    FutureOk: Send + 'static,
    FutureErr: Send + 'static,
    FoldWith: FnMut(Accumulator, FutureOk) -> Accumulator,
    MapErr: FnOnce(FutureErr) -> Err,
    JoinError: Into<Err>,
{
    try_join_complete_set(
        iterable.into_iter().collect(),
        accumulator,
        fold_with,
        map_err,
    )
}

#[inline]
pub fn try_join_all_with_capacity<Iterable, FutureErr, Err>(
    iterable: Iterable,
    capacity: NonZeroUsize,
) -> impl Future<Output = Result<(), Err>> + use<Iterable, FutureErr, Err>
where
    Iterable: IntoIterator,
    Iterable::Item: Future<Output = Result<(), FutureErr>> + Send + 'static,
    FutureErr: Into<Err> + Send + 'static,
    JoinError: Into<Err>,
{
    try_join_all_mapping_err_with_capacity(iterable, Into::into, capacity)
}

#[inline]
pub fn try_join_all_mapping_err_with_capacity<
    Iterable,
    FutureErr,
    MapErr,
    Err,
>(
    iterable: Iterable,
    map_err: MapErr,
    capacity: NonZeroUsize,
) -> impl Future<Output = Result<(), Err>> + use<Iterable, FutureErr, MapErr, Err>
where
    Iterable: IntoIterator,
    Iterable::Item: Future<Output = Result<(), FutureErr>> + Send + 'static,
    FutureErr: Send + 'static,
    MapErr: FnOnce(FutureErr) -> Err,
    JoinError: Into<Err>,
{
    try_join_all_folding_mapping_err_with_capacity(
        iterable,
        (),
        |(), ()| (),
        map_err,
        capacity,
    )
}

pub fn try_join_all_folding_with_capacity<
    Iterable,
    FutureOk,
    FutureErr,
    Accumulator,
    FoldWith,
    Err,
>(
    iterable: Iterable,
    accumulator: Accumulator,
    fold_with: FoldWith,
    capacity: NonZeroUsize,
) -> impl Future<Output = Result<Accumulator, Err>>
       + use<Iterable, FutureOk, FutureErr, Accumulator, FoldWith, Err>
where
    Iterable: IntoIterator,
    Iterable::Item:
        Future<Output = Result<FutureOk, FutureErr>> + Send + 'static,
    FutureOk: Send + 'static,
    FutureErr: Into<Err> + Send + 'static,
    FoldWith: FnMut(Accumulator, FutureOk) -> Accumulator,
    JoinError: Into<Err>,
{
    try_join_all_folding_mapping_err_with_capacity(
        iterable,
        accumulator,
        fold_with,
        Into::into,
        capacity,
    )
}

pub async fn try_join_all_folding_mapping_err_with_capacity<
    Iterable,
    FutureOk,
    FutureErr,
    Accumulator,
    FoldWith,
    MapErr,
    Err,
>(
    iterable: Iterable,
    mut accumulator: Accumulator,
    mut fold_with: FoldWith,
    map_err: MapErr,
    capacity: NonZeroUsize,
) -> Result<Accumulator, Err>
where
    Iterable: IntoIterator,
    Iterable::Item:
        Future<Output = Result<FutureOk, FutureErr>> + Send + 'static,
    FutureOk: Send + 'static,
    FutureErr: Send + 'static,
    FoldWith: FnMut(Accumulator, FutureOk) -> Accumulator,
    MapErr: FnOnce(FutureErr) -> Err,
    JoinError: Into<Err>,
{
    let mut iter = iterable.into_iter().fuse();

    let mut set: JoinSet<_> = (&mut iter).take(capacity.get()).collect();

    if set.is_empty() {
        return Ok(accumulator);
    }

    if set.len() == capacity.get() {
        while let Some(result) = set.join_next().await {
            accumulator = match result {
                Ok(Ok(output)) => fold_with(accumulator, output),
                Ok(Err(error)) => return Err(map_err(error)),
                Err(error) => return Err(error.into()),
            };

            if let Some(future) = iter.next() {
                set.spawn(future);
            } else {
                break;
            }
        }
    }

    try_join_complete_set(set, accumulator, fold_with, map_err).await
}

async fn try_join_complete_set<
    FutureOk,
    FutureErr,
    Accumulator,
    FoldWith,
    MapErr,
    Err,
>(
    mut set: JoinSet<Result<FutureOk, FutureErr>>,
    mut accumulator: Accumulator,
    mut fold_with: FoldWith,
    map_err: MapErr,
) -> Result<Accumulator, Err>
where
    FutureOk: 'static,
    FutureErr: 'static,
    FoldWith: FnMut(Accumulator, FutureOk) -> Accumulator,
    MapErr: FnOnce(FutureErr) -> Err,
    JoinError: Into<Err>,
{
    while let Some(result) = set.join_next().await {
        accumulator = match result {
            Ok(Ok(output)) => fold_with(accumulator, output),
            Ok(Err(error)) => return Err(map_err(error)),
            Err(error) => return Err(error.into()),
        };
    }

    Ok(accumulator)
}

#[tokio::test]
async fn test_join_set_folding() {
    async fn delayed_result(
        delay: std::time::Duration,
        value: u8,
    ) -> Result<u8, std::convert::Infallible> {
        tokio::time::sleep(delay).await;

        Ok(value)
    }

    let result = try_join_all_folding_mapping_err_with_capacity(
        [
            delayed_result(std::time::Duration::from_millis(350), 1),
            delayed_result(std::time::Duration::from_millis(150), 2),
            delayed_result(std::time::Duration::from_millis(50), 4),
            delayed_result(std::time::Duration::from_millis(250), 8),
            delayed_result(std::time::Duration::from_millis(450), 16),
        ],
        0,
        |acc, result| acc ^ result,
        |err| -> JoinError { match err {} },
        const { NonZeroUsize::new(3).unwrap() },
    )
    .await.unwrap();

    assert_eq!(result, 31);
}
