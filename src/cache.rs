use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

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

    /// Returns true if the entry needs refresh (less than threshold fraction of TTL remaining)
    pub fn needs_refresh(&self, threshold: f64) -> bool {
        self.ttl_remaining_fraction() < threshold
    }
}

/// A generic time-based cache with TTL support and proactive refresh detection
pub struct TimedCache<T> {
    entries: RwLock<HashMap<String, CacheEntry<T>>>,
    ttl: Duration,
}

/// Threshold for when to consider a cache entry as needing refresh
/// 0.2 means refresh when less than 20% of TTL remains
const REFRESH_THRESHOLD: f64 = 0.2;

impl<T: Clone + Send + Sync> TimedCache<T> {
    /// Create a new cache with the specified TTL in seconds
    pub fn new(ttl_seconds: u64) -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
            ttl: Duration::from_secs(ttl_seconds),
        }
    }

    /// Get the TTL duration for this cache
    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    /// Get a value from the cache if it exists and hasn't expired.
    /// Performs lazy cleanup of expired entries.
    pub async fn get(&self, key: &str) -> Option<T> {
        // First try with read lock
        {
            let entries = self.entries.read().await;
            if let Some(entry) = entries.get(key) {
                if !entry.is_expired() {
                    return Some(entry.data.clone());
                }
            }
        }

        // If we found an expired entry, clean it up with write lock
        {
            let mut entries = self.entries.write().await;
            if let Some(entry) = entries.get(key) {
                if entry.is_expired() {
                    entries.remove(key);
                }
            }
        }

        None
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

    /// Check if a specific key needs refresh (exists but TTL is running low)
    pub async fn needs_refresh(&self, key: &str) -> bool {
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

    /// Get all keys that need refresh (either missing or TTL running low)
    /// Returns tuples of (key, time_until_expiry) sorted by urgency (soonest first)
    pub async fn keys_needing_refresh(&self) -> Vec<(String, Duration)> {
        let entries = self.entries.read().await;
        let now = Instant::now();
        let mut needs_refresh: Vec<(String, Duration)> = entries
            .iter()
            .filter(|(_, entry)| !entry.is_expired() && entry.needs_refresh(REFRESH_THRESHOLD))
            .map(|(key, entry)| {
                let time_until_expiry = entry.expires_at.saturating_duration_since(now);
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

    /// Remove all expired entries from the cache
    pub async fn cleanup_expired(&self) {
        let mut entries = self.entries.write().await;
        entries.retain(|_, entry| !entry.is_expired());
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
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

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
}
