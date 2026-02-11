use chrono::{DateTime, Utc};
use moka::future::Cache;
use std::future::Future;

use crate::error::Error;

/// Fetches a cached value or computes it using the provided async function.
/// Uses Moka's built-in stampede protection: only one caller executes
/// the fetch on a cache miss; concurrent callers wait for the result.
pub async fn cached_fetch<T, F, Fut>(
    cache: &Cache<String, T>,
    key: &str,
    fetch_fn: F,
) -> Result<T, Error>
where
    T: Clone + Send + Sync + 'static,
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<T, Error>>,
{
    cache
        .try_get_with_by_ref(key, fetch_fn())
        .await
        .map_err(|e| Error::TaskError(e.to_string()))
}

/// Time window filter parameters for historical endpoints.
/// Supports both period-based filtering (3m/6m/12m/all) and
/// timestamp-based filtering (from) for incremental syncing.
#[derive(Debug, Clone)]
pub struct TimeWindowParams {
    /// Number of months to look back (None = all time)
    pub months: Option<i32>,
    /// Only return records after this timestamp (exclusive)
    pub from: Option<DateTime<Utc>>,
}

/// Build a cache key for period-based endpoints.
/// Includes endpoint name, period, and optional from timestamp.
pub fn build_cache_key(
    endpoint: &str,
    period: &str,
    from: Option<DateTime<Utc>>,
) -> String {
    let from_key = from
        .map(|ts| ts.timestamp().to_string())
        .unwrap_or_else(|| "none".to_string());
    format!("{}_{}_{}", endpoint, period, from_key)
}

/// Build a cache key for protocol-specific period-based endpoints.
/// Includes endpoint name, protocol, period, and optional from timestamp.
pub fn build_cache_key_with_protocol(
    endpoint: &str,
    protocol: &str,
    period: &str,
    from: Option<DateTime<Utc>>,
) -> String {
    let from_key = from
        .map(|ts| ts.timestamp().to_string())
        .unwrap_or_else(|| "none".to_string());
    format!("{}_{}_{}_{}", endpoint, protocol, period, from_key)
}

/// Build a cache key for protocol-specific endpoints (no period).
/// Returns "endpoint_PROTOCOL" or "endpoint_total" if protocol is None.
pub fn build_protocol_cache_key(
    endpoint: &str,
    protocol: Option<&str>,
) -> String {
    match protocol {
        Some(p) => format!("{}_{}", endpoint, p.to_uppercase()),
        None => format!("{}_total", endpoint),
    }
}

/// Parse period query parameter to number of months for time window filtering.
/// Returns Some(months) for time-limited queries, None for "all" (no limit).
/// Default is 3 months if no period specified.
pub fn parse_period_months(
    period: &Option<String>,
) -> Result<Option<i32>, Error> {
    match period.as_deref() {
        None | Some("3m") => Ok(Some(3)),
        Some("6m") => Ok(Some(6)),
        Some("12m") => Ok(Some(12)),
        Some("all") => Ok(None),
        Some(p) => Err(Error::InvalidOption {
            option: format!("period '{}'. Valid options: 3m, 6m, 12m, all", p),
        }),
    }
}

/// Parse period and from parameters into TimeWindowParams.
pub fn parse_time_window(
    period: &Option<String>,
    from: Option<DateTime<Utc>>,
) -> Result<TimeWindowParams, Error> {
    let months = parse_period_months(period)?;
    Ok(TimeWindowParams { months, from })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    fn test_cache() -> Cache<String, i32> {
        Cache::builder()
            .time_to_live(Duration::from_secs(60))
            .max_capacity(100)
            .build()
    }

    #[tokio::test]
    async fn test_cached_fetch_miss_then_hit() {
        let cache = test_cache();

        // First call: cache miss, fetch executes
        let result = cached_fetch(&cache, "key1", || async { Ok(42) }).await;
        assert_eq!(result.unwrap(), 42);

        // Second call: cache hit, returns same value
        let result = cached_fetch(&cache, "key1", || async {
            panic!("should not be called on cache hit")
        })
        .await;
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_cached_fetch_error_propagation() {
        let cache = test_cache();

        let result: Result<i32, Error> =
            cached_fetch(&cache, "err_key", || async {
                Err(Error::TaskError("db connection failed".to_string()))
            })
            .await;

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("db connection failed"),
            "Error message was: {}",
            err_msg
        );
    }

    #[tokio::test]
    async fn test_cached_fetch_stampede_protection() {
        let cache = Arc::new(test_cache());
        let fetch_count = Arc::new(AtomicU32::new(0));

        let mut handles = vec![];
        for _ in 0..10 {
            let cache = cache.clone();
            let fetch_count = fetch_count.clone();
            handles.push(tokio::spawn(async move {
                cached_fetch(&cache, "stampede_key", || async {
                    fetch_count.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    Ok::<_, Error>(42)
                })
                .await
            }));
        }

        for handle in handles {
            let result = handle.await.unwrap();
            assert_eq!(result.unwrap(), 42);
        }

        // Moka coalesces concurrent fetches — expect 1 (or at most 2 due to timing)
        let count = fetch_count.load(Ordering::SeqCst);
        assert!(count <= 2, "Fetch was called {} times, expected 1-2", count);
    }

    #[tokio::test]
    async fn test_cached_fetch_expiry() {
        let cache: Cache<String, i32> = Cache::builder()
            .time_to_live(Duration::from_millis(100))
            .max_capacity(100)
            .build();

        // Populate
        let result = cached_fetch(&cache, "ttl_key", || async { Ok(1) }).await;
        assert_eq!(result.unwrap(), 1);

        // Wait for TTL to expire
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Should re-fetch after expiry
        let result = cached_fetch(&cache, "ttl_key", || async { Ok(2) }).await;
        assert_eq!(result.unwrap(), 2);
    }
}
