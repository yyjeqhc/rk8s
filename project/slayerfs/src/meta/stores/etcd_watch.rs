//! Etcd Watch Worker for Cache Invalidation
//!
//! Monitors etcd changes and invalidates local cache to maintain consistency
//! across multiple clients.

use crate::meta::store::MetaError;
use etcd_client::{Client as EtcdClient, EventType, WatchOptions, WatchStream};
use log::{debug, error, info, warn};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// Cache invalidation events from etcd watch
#[derive(Debug, Clone)]
pub enum CacheInvalidationEvent {
    /// Invalidate specific inode cache
    InvalidateInode(i64),
    
    /// Invalidate parent's children cache (due to create/delete)
    InvalidateParentChildren(i64),
    
    /// Invalidate path cache with prefix
    InvalidatePathPrefix(String),
    
    /// Full cache invalidation (for safety)
    InvalidateAll,
}

/// Etcd watch worker configuration
#[derive(Debug, Clone)]
pub struct WatchConfig {
    /// Watch key prefix (default: all metadata keys)
    pub key_prefix: String,
    
    /// Buffer size for event channel
    pub event_buffer_size: usize,
    
    /// Enable debug logging
    pub debug: bool,
}

impl Default for WatchConfig {
    fn default() -> Self {
        Self {
            key_prefix: "".to_string(), // Watch all keys
            event_buffer_size: 1000,
            debug: false,
        }
    }
}

/// Etcd watch worker
///
/// # Responsibilities
/// 1. Watch etcd key changes (PUT/DELETE events)
/// 2. Parse changed keys and generate cache invalidation events
/// 3. Send events to MetaClient for cache invalidation
///
/// # Architecture
/// ```text
/// etcd Watch Stream
///       │
///       ▼
///   WatchWorker
///       │
///       ├─ Parse Key: f:10:file.txt → parent=10, name=file.txt
///       ├─ Parse Key: r:100 → inode=100
///       └─ Parse Key: c:10 → parent=10
///       │
///       ▼
///   mpsc::Sender<CacheInvalidationEvent>
///       │
///       ▼
///   MetaClient (invalidate cache)
/// ```
pub struct EtcdWatchWorker {
    client: EtcdClient,
    config: WatchConfig,
    event_tx: mpsc::Sender<CacheInvalidationEvent>,
    worker_handle: Option<JoinHandle<()>>,
}

impl EtcdWatchWorker {
    /// Create a new watch worker
    ///
    /// # Returns
    /// - `Self`: Worker instance
    /// - `mpsc::Receiver<CacheInvalidationEvent>`: Event receiver for MetaClient
    pub fn new(
        client: EtcdClient,
        config: WatchConfig,
    ) -> (Self, mpsc::Receiver<CacheInvalidationEvent>) {
        let (event_tx, event_rx) = mpsc::channel(config.event_buffer_size);

        let worker = Self {
            client,
            config,
            event_tx,
            worker_handle: None,
        };

        (worker, event_rx)
    }

    /// Start watch worker in background
    pub fn start(&mut self) -> Result<(), MetaError> {
        let client = self.client.clone();
        let config = self.config.clone();
        let event_tx = self.event_tx.clone();

        let handle = tokio::spawn(async move {
            if let Err(e) = Self::watch_loop(client, config, event_tx).await {
                error!("Watch worker fatal error: {}", e);
            }
        });

        self.worker_handle = Some(handle);
        info!("Etcd watch worker started");
        Ok(())
    }

    /// Stop watch worker
    pub async fn stop(&mut self) {
        if let Some(handle) = self.worker_handle.take() {
            handle.abort();
            info!("Etcd watch worker stopped");
        }
    }

    /// Main watch loop (runs in background task)
    async fn watch_loop(
        mut client: EtcdClient,
        config: WatchConfig,
        event_tx: mpsc::Sender<CacheInvalidationEvent>,
    ) -> Result<(), MetaError> {
        info!(
            "Starting etcd watch loop with prefix: '{}'",
            config.key_prefix
        );

        loop {
            // Create watch stream with prefix
            let options = WatchOptions::new().with_prefix();
            let (mut watcher, mut stream) = match client
                .watch(config.key_prefix.clone(), Some(options))
                .await
            {
                Ok((w, s)) => (w, s),
                Err(e) => {
                    error!("Failed to create watch stream: {}", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    continue;
                }
            };

            info!("Watch stream established");

            // Process watch events
            while let Some(resp) = stream.message().await.transpose() {
                match resp {
                    Ok(resp) => {
                        if resp.canceled() {
                            warn!("Watch canceled, reconnecting...");
                            break;
                        }

                        for event in resp.events() {
                            if let Err(e) = Self::handle_watch_event(event, &event_tx, &config).await {
                                error!("Failed to handle watch event: {}", e);
                            }
                        }
                    }
                    Err(e) => {
                        error!("Watch stream error: {}", e);
                        break;
                    }
                }
            }

            // Reconnect on stream close
            warn!("Watch stream closed, reconnecting in 1s...");
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }
    }

    /// Handle single watch event
    async fn handle_watch_event(
        event: &etcd_client::Event,
        event_tx: &mpsc::Sender<CacheInvalidationEvent>,
        config: &WatchConfig,
    ) -> Result<(), MetaError> {
        let event_type = event.event_type();
        let kv = match event.kv() {
            Some(kv) => kv,
            None => return Ok(()), // No key-value, skip
        };

        let key = String::from_utf8_lossy(kv.key()).to_string();

        if config.debug {
            debug!("Watch event: {:?} on key: {}", event_type, key);
        }

        // Parse key and generate invalidation events
        let invalidation_events = Self::parse_key_to_events(&key, event_type);

        for inv_event in invalidation_events {
            if config.debug {
                debug!("Generated invalidation: {:?}", inv_event);
            }

            // Send to MetaClient (non-blocking, drop if full)
            if event_tx.try_send(inv_event.clone()).is_err() {
                warn!("Event channel full, dropping event: {:?}", inv_event);
            }
        }

        Ok(())
    }

    /// Parse etcd key to cache invalidation events
    ///
    /// # Key Formats
    /// - `f:{parent}:{name}` - Forward index (parent, name) → inode
    /// - `r:{inode}` - Reverse index inode → metadata
    /// - `c:{inode}` - Children index inode → children set
    ///
    /// # Event Generation Rules
    /// - `f:*` change → Invalidate parent's children + path cache
    /// - `r:*` change → Invalidate inode cache + related paths
    /// - `c:*` change → Invalidate parent's children cache
    fn parse_key_to_events(key: &str, event_type: EventType) -> Vec<CacheInvalidationEvent> {
        let mut events = Vec::new();

        // Parse key prefix
        let parts: Vec<&str> = key.split(':').collect();
        if parts.is_empty() {
            return events;
        }

        match parts[0] {
            "f" if parts.len() >= 3 => {
                // Forward index: f:{parent}:{name}
                if let Ok(parent_ino) = parts[1].parse::<i64>() {
                    events.push(CacheInvalidationEvent::InvalidateParentChildren(parent_ino));
                    
                    // Also invalidate path cache with parent inode
                    // Note: We don't have full path here, so invalidate all paths of this inode
                    // This will be handled by MetaClient using inode_to_paths
                }
            }
            "r" if parts.len() >= 2 => {
                // Reverse index: r:{inode}
                if let Ok(inode) = parts[1].parse::<i64>() {
                    events.push(CacheInvalidationEvent::InvalidateInode(inode));
                }
            }
            "c" if parts.len() >= 2 => {
                // Children index: c:{parent_inode}
                if let Ok(parent_ino) = parts[1].parse::<i64>() {
                    events.push(CacheInvalidationEvent::InvalidateParentChildren(parent_ino));
                }
            }
            "slayerfs" if key.contains("next_inode_id") => {
                // ID counter change - no cache invalidation needed
            }
            _ => {
                // Unknown key format - safe fallback
                warn!("Unknown etcd key format: {}", key);
            }
        }

        events
    }
}

impl Drop for EtcdWatchWorker {
    fn drop(&mut self) {
        if let Some(handle) = self.worker_handle.take() {
            handle.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_forward_key() {
        let events = EtcdWatchWorker::parse_key_to_events("f:10:file.txt", EventType::Put);
        assert_eq!(events.len(), 1);
        assert!(matches!(
            events[0],
            CacheInvalidationEvent::InvalidateParentChildren(10)
        ));
    }

    #[test]
    fn test_parse_reverse_key() {
        let events = EtcdWatchWorker::parse_key_to_events("r:100", EventType::Put);
        assert_eq!(events.len(), 1);
        assert!(matches!(
            events[0],
            CacheInvalidationEvent::InvalidateInode(100)
        ));
    }

    #[test]
    fn test_parse_children_key() {
        let events = EtcdWatchWorker::parse_key_to_events("c:50", EventType::Delete);
        assert_eq!(events.len(), 1);
        assert!(matches!(
            events[0],
            CacheInvalidationEvent::InvalidateParentChildren(50)
        ));
    }

    #[test]
    fn test_parse_unknown_key() {
        let events = EtcdWatchWorker::parse_key_to_events("unknown:123", EventType::Put);
        assert_eq!(events.len(), 0); // No events for unknown keys
    }
}
