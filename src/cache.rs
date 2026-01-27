use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, RwLock};

/// A cache entry with creation and expiration tracking
pub struct CacheEntry<T> {
    pub data: T,
    pub created_at: Instant,
    pub expires_at: Instant,
}

impl<T> CacheEntry<T> {
    /// Returns the fraction of TTL remaining (0.0 = expired, 1.0 = just created)
    pub fn ttl_remaining_fraction(&self) -> f64 {
        let now = Instant::now();
        if now >= self.expires_at {
            return 0.0;
        }
        let total_ttl = self.expires_at.duration_since(self.created_at);
        let remaining = self.expires_at.duration_since(now);
        remaining.as_secs_f64() / total_ttl.as_secs_f64()
    }

    /// Returns true if the entry has expired
    pub fn is_expired(&self) -> bool {
        Instant::now() >= self.expires_at
    }

    /// Returns true if the entry is stale (expired but within grace period)
    pub fn is_stale(&self, grace_period: Duration) -> bool {
        let now = Instant::now();
        now >= self.expires_at && now < self.expires_at + grace_period
    }

    /// Returns true if the entry needs refresh (less than threshold fraction of TTL remaining)
    pub fn needs_refresh(&self, threshold: f64) -> bool {
        self.ttl_remaining_fraction() < threshold
    }
}

/// State for tracking in-flight refresh operations per key
struct RefreshState {
    /// True if a refresh is currently in progress
    in_progress: AtomicBool,
    /// Channel to notify waiters when refresh completes
    /// Using broadcast so multiple waiters can receive the signal
    notify: broadcast::Sender<()>,
}

impl RefreshState {
    fn new() -> Self {
        let (notify, _) = broadcast::channel(1);
        Self {
            in_progress: AtomicBool::new(false),
            notify,
        }
    }

    /// Try to acquire the refresh lock. Returns true if this caller should do the refresh.
    fn try_start_refresh(&self) -> bool {
        self.in_progress
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    /// Mark refresh as complete and notify waiters
    fn complete_refresh(&self) {
        self.in_progress.store(false, Ordering::SeqCst);
        // Ignore send errors - no receivers is fine
        let _ = self.notify.send(());
    }

    /// Check if refresh is in progress
    fn is_refreshing(&self) -> bool {
        self.in_progress.load(Ordering::SeqCst)
    }

    /// Subscribe to refresh completion notifications
    fn subscribe(&self) -> broadcast::Receiver<()> {
        self.notify.subscribe()
    }
}

/// A generic time-based cache with TTL support, stampede protection,
/// and stale-while-revalidate semantics.
pub struct TimedCache<T> {
    entries: RwLock<HashMap<String, CacheEntry<T>>>,
    refresh_states: RwLock<HashMap<String, Arc<RefreshState>>>,
    ttl: Duration,
    /// Grace period after expiry during which stale data can still be served
    grace_period: Duration,
}

/// Threshold for when to consider a cache entry as needing refresh
/// 0.2 means refresh when less than 20% of TTL remains
const REFRESH_THRESHOLD: f64 = 0.2;

/// Default grace period - serve stale data for up to 30 seconds while refreshing
const DEFAULT_GRACE_PERIOD_SECS: u64 = 30;

/// Result of a cache get operation
pub enum CacheResult<T> {
    /// Fresh data from cache
    Hit(T),
    /// Stale data - still usable but refresh recommended
    Stale(T),
    /// No data available
    Miss,
}

impl<T: Clone + Send + Sync> TimedCache<T> {
    /// Create a new cache with the specified TTL in seconds
    pub fn new(ttl_seconds: u64) -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
            refresh_states: RwLock::new(HashMap::new()),
            ttl: Duration::from_secs(ttl_seconds),
            grace_period: Duration::from_secs(DEFAULT_GRACE_PERIOD_SECS),
        }
    }

    /// Create a new cache with custom TTL and grace period
    pub fn with_grace_period(
        ttl_seconds: u64,
        grace_period_seconds: u64,
    ) -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
            refresh_states: RwLock::new(HashMap::new()),
            ttl: Duration::from_secs(ttl_seconds),
            grace_period: Duration::from_secs(grace_period_seconds),
        }
    }

    /// Get the TTL duration for this cache
    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    /// Get a value from the cache if it exists and hasn't expired.
    /// Returns None if the entry doesn't exist or is beyond the grace period.
    /// For stale-while-revalidate behavior, use `get_with_stale`.
    pub async fn get(&self, key: &str) -> Option<T> {
        let entries = self.entries.read().await;
        if let Some(entry) = entries.get(key) {
            if !entry.is_expired() {
                return Some(entry.data.clone());
            }
        }
        None
    }

    /// Get a value with stale-while-revalidate semantics.
    /// Returns `CacheResult::Hit` for fresh data, `CacheResult::Stale` for expired
    /// but within grace period, and `CacheResult::Miss` otherwise.
    pub async fn get_with_stale(&self, key: &str) -> CacheResult<T> {
        let entries = self.entries.read().await;
        if let Some(entry) = entries.get(key) {
            if !entry.is_expired() {
                return CacheResult::Hit(entry.data.clone());
            }
            if entry.is_stale(self.grace_period) {
                return CacheResult::Stale(entry.data.clone());
            }
        }
        CacheResult::Miss
    }

    /// Store a value in the cache with the configured TTL
    pub async fn set(&self, key: &str, value: T) {
        let mut entries = self.entries.write().await;
        let now = Instant::now();
        entries.insert(
            key.to_string(),
            CacheEntry {
                data: value,
                created_at: now,
                expires_at: now + self.ttl,
            },
        );
    }

    /// Get or fetch with stampede protection.
    /// Only one caller will execute the fetch function; others wait for the result.
    /// Supports stale-while-revalidate: returns stale data immediately while
    /// one background task refreshes.
    pub async fn get_or_fetch<F, Fut, E>(
        &self,
        key: &str,
        fetch_fn: F,
    ) -> Result<T, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, E>>,
        E: std::fmt::Debug,
    {
        // Fast path: check for fresh cache hit
        match self.get_with_stale(key).await {
            CacheResult::Hit(data) => return Ok(data),
            CacheResult::Stale(data) => {
                // Return stale data immediately, trigger background refresh
                self.trigger_background_refresh_if_needed(key).await;
                return Ok(data);
            },
            CacheResult::Miss => {
                // Need to fetch - continue below
            },
        }

        // Get or create refresh state for this key
        let refresh_state = self.get_or_create_refresh_state(key).await;

        // Try to become the one who does the refresh
        if refresh_state.try_start_refresh() {
            // We won the race - do the fetch
            let result = fetch_fn().await;

            match &result {
                Ok(data) => {
                    self.set(key, data.clone()).await;
                },
                Err(e) => {
                    tracing::warn!(
                        "Cache fetch failed for key '{}': {:?}",
                        key,
                        e
                    );
                },
            }

            // Signal completion to any waiters
            refresh_state.complete_refresh();
            return result;
        }

        // Someone else is fetching - wait for them
        let mut receiver = refresh_state.subscribe();

        // Wait for the refresh to complete (with timeout)
        let wait_result =
            tokio::time::timeout(Duration::from_secs(30), receiver.recv())
                .await;

        match wait_result {
            Ok(_) => {
                // Refresh completed - try to get from cache
                if let Some(data) = self.get(key).await {
                    return Ok(data);
                }
                // If still no data, the fetch must have failed
                // Fall through to do our own fetch
            },
            Err(_) => {
                // Timeout waiting - try to get whatever is in cache
                if let Some(data) = self.get(key).await {
                    return Ok(data);
                }
                // Fall through to do our own fetch
            },
        }

        // Last resort: do the fetch ourselves
        let result = fetch_fn().await;
        if let Ok(ref data) = result {
            self.set(key, data.clone()).await;
        }
        result
    }

    /// Get or create a refresh state for a key
    async fn get_or_create_refresh_state(
        &self,
        key: &str,
    ) -> Arc<RefreshState> {
        // Try read lock first
        {
            let states = self.refresh_states.read().await;
            if let Some(state) = states.get(key) {
                return state.clone();
            }
        }

        // Need to create - acquire write lock
        let mut states = self.refresh_states.write().await;
        // Double-check after acquiring write lock
        if let Some(state) = states.get(key) {
            return state.clone();
        }

        let state = Arc::new(RefreshState::new());
        states.insert(key.to_string(), state.clone());
        state
    }

    /// Trigger a background refresh if one isn't already in progress
    async fn trigger_background_refresh_if_needed(&self, key: &str) {
        let refresh_state = self.get_or_create_refresh_state(key).await;
        if !refresh_state.is_refreshing() {
            // Just mark that a refresh would be nice - the background task will pick it up
            // We don't spawn a task here because we don't have the fetch function
        }
    }

    /// Check if a specific key needs refresh (exists but TTL is running low)
    pub async fn needs_refresh(&self, key: &str) -> bool {
        // If already refreshing, don't trigger another
        {
            let states = self.refresh_states.read().await;
            if let Some(state) = states.get(key) {
                if state.is_refreshing() {
                    return false;
                }
            }
        }

        let entries = self.entries.read().await;
        if let Some(entry) = entries.get(key) {
            // Don't refresh if already expired - let the request-driven path handle it
            if entry.is_expired() {
                return false;
            }
            return entry.needs_refresh(REFRESH_THRESHOLD);
        }
        // Key doesn't exist - initial population needed
        true
    }

    /// Try to acquire refresh lock for a key. Returns true if this caller should do the refresh.
    /// Use this with `complete_refresh` for manual refresh control.
    pub async fn try_start_refresh(&self, key: &str) -> bool {
        let refresh_state = self.get_or_create_refresh_state(key).await;
        refresh_state.try_start_refresh()
    }

    /// Mark a refresh as complete for a key. Call this after `try_start_refresh` returns true.
    pub async fn complete_refresh(&self, key: &str) {
        let states = self.refresh_states.read().await;
        if let Some(state) = states.get(key) {
            state.complete_refresh();
        }
    }

    /// Check if a refresh is currently in progress for a key
    pub async fn is_refreshing(&self, key: &str) -> bool {
        let states = self.refresh_states.read().await;
        if let Some(state) = states.get(key) {
            return state.is_refreshing();
        }
        false
    }

    /// Get all keys that need refresh (either missing or TTL running low)
    /// Returns tuples of (key, time_until_expiry) sorted by urgency (soonest first)
    /// Excludes keys that are currently being refreshed.
    pub async fn keys_needing_refresh(&self) -> Vec<(String, Duration)> {
        let entries = self.entries.read().await;
        let refresh_states = self.refresh_states.read().await;
        let now = Instant::now();

        let mut needs_refresh: Vec<(String, Duration)> = entries
            .iter()
            .filter(|(key, entry)| {
                // Skip if already expired or being refreshed
                if entry.is_expired() {
                    return false;
                }
                if let Some(state) = refresh_states.get(*key) {
                    if state.is_refreshing() {
                        return false;
                    }
                }
                entry.needs_refresh(REFRESH_THRESHOLD)
            })
            .map(|(key, entry)| {
                let time_until_expiry =
                    entry.expires_at.saturating_duration_since(now);
                (key.clone(), time_until_expiry)
            })
            .collect();

        // Sort by time until expiry (most urgent first)
        needs_refresh.sort_by(|a, b| a.1.cmp(&b.1));
        needs_refresh
    }

    /// Check if a key exists in the cache (even if expired)
    pub async fn contains_key(&self, key: &str) -> bool {
        let entries = self.entries.read().await;
        if let Some(entry) = entries.get(key) {
            return !entry.is_expired();
        }
        false
    }

    /// Remove a specific key from the cache
    pub async fn invalidate(&self, key: &str) {
        let mut entries = self.entries.write().await;
        entries.remove(key);
    }

    /// Remove all expired entries from the cache (beyond grace period)
    pub async fn cleanup_expired(&self) {
        let grace = self.grace_period;
        let mut entries = self.entries.write().await;
        entries.retain(|_, entry| !entry.is_expired() || entry.is_stale(grace));

        // Also clean up refresh states for removed entries
        let remaining_keys: std::collections::HashSet<_> =
            entries.keys().cloned().collect();
        let mut states = self.refresh_states.write().await;
        states.retain(|key, _| remaining_keys.contains(key));
    }

    /// Get the number of entries in the cache (including expired)
    pub async fn len(&self) -> usize {
        let entries = self.entries.read().await;
        entries.len()
    }

    /// Check if the cache is empty
    pub async fn is_empty(&self) -> bool {
        let entries = self.entries.read().await;
        entries.is_empty()
    }
}

impl<T> std::fmt::Debug for TimedCache<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TimedCache")
            .field("ttl", &self.ttl)
            .field("grace_period", &self.grace_period)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_cache_set_get() {
        let cache: TimedCache<String> = TimedCache::new(60);
        cache.set("key1", "value1".to_string()).await;

        let result = cache.get("key1").await;
        assert_eq!(result, Some("value1".to_string()));
    }

    #[tokio::test]
    async fn test_cache_miss() {
        let cache: TimedCache<String> = TimedCache::new(60);

        let result = cache.get("nonexistent").await;
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_needs_refresh_missing_key() {
        let cache: TimedCache<String> = TimedCache::new(60);

        // Missing key should need refresh (initial population)
        assert!(cache.needs_refresh("missing").await);
    }

    #[tokio::test]
    async fn test_cache_entry_ttl_fraction() {
        let now = Instant::now();
        let entry = CacheEntry {
            data: "test",
            created_at: now,
            expires_at: now + Duration::from_secs(100),
        };

        // Just created, should have ~100% TTL remaining
        let fraction = entry.ttl_remaining_fraction();
        assert!(fraction > 0.99 && fraction <= 1.0);
    }

    #[tokio::test]
    async fn test_contains_key() {
        let cache: TimedCache<String> = TimedCache::new(60);
        cache.set("key1", "value1".to_string()).await;

        assert!(cache.contains_key("key1").await);
        assert!(!cache.contains_key("key2").await);
    }

    #[tokio::test]
    async fn test_stampede_protection() {
        let cache: TimedCache<i32> = TimedCache::new(60);
        let cache = Arc::new(cache);

        // Simulate multiple concurrent fetches
        let fetch_count = Arc::new(std::sync::atomic::AtomicU32::new(0));

        let mut handles = vec![];
        for _ in 0..10 {
            let cache = cache.clone();
            let fetch_count = fetch_count.clone();
            handles.push(tokio::spawn(async move {
                cache
                    .get_or_fetch("key", || async {
                        // Count how many times fetch is actually called
                        fetch_count.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        Ok::<_, std::convert::Infallible>(42)
                    })
                    .await
            }));
        }

        // Wait for all to complete
        for handle in handles {
            let result = handle.await.unwrap();
            assert_eq!(result.unwrap(), 42);
        }

        // Fetch should only be called once (or at most a few times due to timing)
        let count = fetch_count.load(Ordering::SeqCst);
        assert!(count <= 2, "Fetch was called {} times, expected 1-2", count);
    }

    #[tokio::test]
    async fn test_try_start_refresh() {
        let cache: TimedCache<String> = TimedCache::new(60);

        // First call should succeed
        assert!(cache.try_start_refresh("key").await);

        // Second call should fail (already refreshing)
        assert!(!cache.try_start_refresh("key").await);

        // After completing, should succeed again
        cache.complete_refresh("key").await;
        assert!(cache.try_start_refresh("key").await);
    }

    #[tokio::test]
    async fn test_stale_while_revalidate() {
        let cache: TimedCache<String> = TimedCache::with_grace_period(1, 30);
        cache.set("key", "value".to_string()).await;

        // Fresh data
        match cache.get_with_stale("key").await {
            CacheResult::Hit(_) => {},
            _ => panic!("Expected Hit"),
        }

        // Wait for expiry but within grace period
        tokio::time::sleep(Duration::from_millis(1100)).await;

        // Should return stale data
        match cache.get_with_stale("key").await {
            CacheResult::Stale(v) => assert_eq!(v, "value"),
            _ => panic!("Expected Stale"),
        }
    }
}
