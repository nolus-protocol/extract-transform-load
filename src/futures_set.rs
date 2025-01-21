use std::{future::Future, num::NonZeroUsize};

use tokio::task::{JoinError, JoinSet};

#[inline]
pub fn try_join_all<Iterable, Err, FutureErr>(
    iterable: Iterable,
) -> impl Future<Output = Result<(), Err>>
       + Send
       + 'static
       + use<Iterable, Err, FutureErr>
where
    Iterable: IntoIterator,
    Iterable::Item: Future<Output = Result<(), FutureErr>> + Send + 'static,
    Err: 'static,
    JoinError: Into<Err>,
    FutureErr: Into<Err> + Send + 'static,
{
    try_join_all_folding(iterable, (), |(), ()| ())
}

pub fn try_join_all_folding<
    Iterable,
    Accumulator,
    FoldWith,
    Err,
    FutureOk,
    FutureErr,
>(
    iterable: Iterable,
    accumulator: Accumulator,
    fold_with: FoldWith,
) -> impl Future<Output = Result<Accumulator, Err>>
       + Send
       + 'static
       + use<Iterable, Accumulator, FoldWith, Err, FutureOk, FutureErr>
where
    Iterable: IntoIterator,
    Iterable::Item:
        Future<Output = Result<FutureOk, FutureErr>> + Send + 'static,
    Accumulator: Send + 'static,
    FoldWith: FnMut(Accumulator, FutureOk) -> Accumulator + Send + 'static,
    Err: 'static,
    JoinError: Into<Err>,
    FutureOk: Send + 'static,
    FutureErr: Into<Err> + Send + 'static,
{
    try_join_set_folding(accumulator, fold_with, iterable.into_iter().collect())
}

#[inline]
pub fn try_join_all_with_capacity<Iterable, Err, FutureErr>(
    iterable: Iterable,
    capacity: NonZeroUsize,
) -> impl Future<Output = Result<(), Err>>
       + Send
       + 'static
       + use<Iterable, Err, FutureErr>
where
    Iterable: IntoIterator + Send + 'static,
    Iterable::IntoIter: Send,
    Iterable::Item: Future<Output = Result<(), FutureErr>> + Send + 'static,
    Err: 'static,
    FutureErr: Into<Err> + Send + 'static,
    JoinError: Into<Err>,
{
    try_join_all_folding_with_capacity(iterable, (), |(), ()| (), capacity)
}

pub async fn try_join_all_folding_with_capacity<
    Iterable,
    Accumulator,
    FoldWith,
    Err,
    FutureOk,
    FutureErr,
>(
    iterable: Iterable,
    mut accumulator: Accumulator,
    mut fold_with: FoldWith,
    capacity: NonZeroUsize,
) -> Result<Accumulator, Err>
where
    Iterable: IntoIterator,
    Iterable::Item:
        Future<Output = Result<FutureOk, FutureErr>> + Send + 'static,
    FoldWith: FnMut(Accumulator, FutureOk) -> Accumulator,
    JoinError: Into<Err>,
    FutureOk: Send + 'static,
    FutureErr: Into<Err> + Send + 'static,
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
                Ok(Err(error)) => return Err(error.into()),
                Err(error) => return Err(error.into()),
            };

            if let Some(future) = iter.next() {
                set.spawn(future);
            } else {
                break;
            }
        }
    }

    try_join_set_folding(accumulator, fold_with, set).await
}

async fn try_join_set_folding<Accumulator, FoldWith, Err, FutureOk, FutureErr>(
    mut accumulator: Accumulator,
    mut fold_with: FoldWith,
    mut set: JoinSet<Result<FutureOk, FutureErr>>,
) -> Result<Accumulator, Err>
where
    FoldWith: FnMut(Accumulator, FutureOk) -> Accumulator,
    JoinError: Into<Err>,
    FutureOk: Send + 'static,
    FutureErr: Into<Err> + Send + 'static,
{
    while let Some(result) = set.join_next().await {
        accumulator = match result {
            Ok(Ok(output)) => fold_with(accumulator, output),
            Ok(Err(error)) => return Err(error.into()),
            Err(error) => return Err(error.into()),
        };
    }

    Ok(accumulator)
}
