use std::{convert::Infallible, future::Future, num::NonZeroUsize};

use tokio::task::{JoinError, JoinSet};

#[inline]
pub fn map_infallible<T>(infallible: Infallible) -> T {
    match infallible {}
}

pub async fn try_join_all<
    Iterable,
    FutureOk,
    FutureErr,
    MapJoinErr,
    MapFutureErr,
    Accumulator,
    FoldWith,
    FoldErr,
    MapFoldErr,
    Err,
>(
    iterable: Iterable,
    map_join_err: MapJoinErr,
    map_future_err: MapFutureErr,
    accumulator: Accumulator,
    fold_with: FoldWith,
    map_fold_err: MapFoldErr,
    capacity: Option<NonZeroUsize>,
) -> Result<Accumulator, Err>
where
    Iterable: IntoIterator,
    Iterable::Item:
        Future<Output = Result<FutureOk, FutureErr>> + Send + 'static,
    FutureOk: Send + 'static,
    FutureErr: Send + 'static,
    MapJoinErr: FnOnce(JoinError) -> Err,
    MapFutureErr: FnOnce(FutureErr) -> Err,
    FoldWith: FnMut(Accumulator, FutureOk) -> Result<Accumulator, FoldErr>,
    MapFoldErr: FnOnce(FoldErr) -> Err,
{
    let functors_and_accumulator = FunctorsAndAccumulator {
        map_join_err,
        map_future_err,
        accumulator,
        fold_with,
        map_fold_err,
    };

    let set: JoinSet<_>;

    if let Some(capacity) = capacity {
        let mut iter = iterable.into_iter().fuse();

        set = (&mut iter).take(capacity.get()).collect();

        if set.len() == capacity.get() {
            return functors_and_accumulator
                .join_set(set, |mut set| {
                    if let Some(future) = iter.next() {
                        set.spawn(future);
                    }

                    set
                })
                .await;
        }
    } else {
        set = iterable.into_iter().collect();
    }

    functors_and_accumulator
        .join_set(set, std::convert::identity)
        .await
}

struct FunctorsAndAccumulator<
    MapJoinErr,
    MapFutureErr,
    Accumulator,
    FoldWith,
    MapFoldErr,
> {
    map_join_err: MapJoinErr,
    map_future_err: MapFutureErr,
    accumulator: Accumulator,
    fold_with: FoldWith,
    map_fold_err: MapFoldErr,
}

impl<MapJoinErr, MapFutureErr, Accumulator, FoldWith, MapFoldErr>
    FunctorsAndAccumulator<
        MapJoinErr,
        MapFutureErr,
        Accumulator,
        FoldWith,
        MapFoldErr,
    >
{
    async fn join_set<FutureOk, FutureErr, FoldErr, Err, AfterFold>(
        self,
        mut set: JoinSet<Result<FutureOk, FutureErr>>,
        mut after_fold: AfterFold,
    ) -> Result<Accumulator, Err>
    where
        FutureOk: Send + 'static,
        FutureErr: Send + 'static,
        MapJoinErr: FnOnce(JoinError) -> Err,
        MapFutureErr: FnOnce(FutureErr) -> Err,
        FoldWith: FnMut(Accumulator, FutureOk) -> Result<Accumulator, FoldErr>,
        MapFoldErr: FnOnce(FoldErr) -> Err,
        AfterFold: FnMut(
            JoinSet<Result<FutureOk, FutureErr>>,
        ) -> JoinSet<Result<FutureOk, FutureErr>>,
    {
        let Self {
            map_join_err,
            map_future_err,
            mut accumulator,
            mut fold_with,
            map_fold_err,
        } = self;

        while let Some(result) = set.join_next().await {
            accumulator = match result {
                Ok(Ok(output)) => match fold_with(accumulator, output) {
                    Ok(accumulator) => accumulator,
                    Err(error) => return Err(map_fold_err(error)),
                },
                Ok(Err(error)) => return Err(map_future_err(error)),
                Err(error) => return Err(map_join_err(error)),
            };

            set = after_fold(set);
        }

        Ok(accumulator)
    }
}

#[tokio::test]
async fn test_join_set_folding() {
    async fn delayed_result(
        delay: std::time::Duration,
        value: u8,
    ) -> Result<u8, Infallible> {
        tokio::time::sleep(delay).await;

        Ok(value)
    }

    let result = try_join_all(
        [
            delayed_result(std::time::Duration::from_millis(350), 1),
            delayed_result(std::time::Duration::from_millis(150), 2),
            delayed_result(std::time::Duration::from_millis(50), 4),
            delayed_result(std::time::Duration::from_millis(250), 8),
            delayed_result(std::time::Duration::from_millis(450), 16),
        ],
        std::convert::identity,
        map_infallible,
        0,
        |acc, result| Ok(acc ^ result),
        map_infallible,
        const { Some(NonZeroUsize::new(3).unwrap()) },
    )
    .await
    .unwrap();

    assert_eq!(result, 31);
}
