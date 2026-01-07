use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// A cache entry with expiration time
pub struct CacheEntry<T> {
    pub data: T,
    pub expires_at: Instant,
}

/// A generic time-based cache with TTL support
pub struct TimedCache<T> {
    entries: RwLock<HashMap<String, CacheEntry<T>>>,
    ttl: Duration,
}

impl<T: Clone + Send + Sync> TimedCache<T> {
    /// Create a new cache with the specified TTL in seconds
    pub fn new(ttl_seconds: u64) -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
            ttl: Duration::from_secs(ttl_seconds),
        }
    }

    /// Get a value from the cache if it exists and hasn't expired
    pub async fn get(&self, key: &str) -> Option<T> {
        let entries = self.entries.read().await;
        if let Some(entry) = entries.get(key) {
            if Instant::now() < entry.expires_at {
                return Some(entry.data.clone());
            }
        }
        None
    }

    /// Store a value in the cache with the configured TTL
    pub async fn set(&self, key: &str, value: T) {
        let mut entries = self.entries.write().await;
        entries.insert(
            key.to_string(),
            CacheEntry {
                data: value,
                expires_at: Instant::now() + self.ttl,
            },
        );
    }

    /// Remove a specific key from the cache
    #[allow(dead_code)]
    pub async fn invalidate(&self, key: &str) {
        let mut entries = self.entries.write().await;
        entries.remove(key);
    }

    /// Remove all expired entries from the cache
    #[allow(dead_code)]
    pub async fn cleanup_expired(&self) {
        let mut entries = self.entries.write().await;
        let now = Instant::now();
        entries.retain(|_, entry| entry.expires_at > now);
    }
}

impl<T> std::fmt::Debug for TimedCache<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TimedCache")
            .field("ttl", &self.ttl)
            .finish()
    }
}
