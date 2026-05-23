//! Shared QUIC runtime singleton.
//!
//! This module manages the process-level `QuicListeners` singleton that all
//! Xline servers share. Because `QuicListeners` binds to system ports, only
//! one instance may exist per process.
//!
//! # Lifecycle
//!
//! 1. The first call to [`SharedQuicRuntime::get_or_init`] creates the
//!    `QuicListeners` instance.
//! 2. Subsequent calls return a handle to the existing instance.
//! 3. [`SharedQuicRuntime::shutdown`] shuts down the listeners and clears
//!    the singleton. This is intended for **process-exit cleanup only**,
//!    not for between-test reset.
//!
//! # Why no `reset()`?
//!
//! `QuicListeners` is a process-level singleton that binds to OS ports.
//! Resetting it between tests creates ordering dependencies and prevents
//! parallel test execution. Tests should use unique ports or rely on
//! process exit for cleanup.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use dquic::prelude::QuicListeners;
use tracing::info;

use super::port_router::ServerRoutingInfo;

/// Global shared state for the QUIC accept loop.
pub(crate) struct SharedQuicState {
    /// The QUIC listeners instance (process-level singleton).
    pub(crate) listeners: Arc<QuicListeners>,
    /// Routing table: server_name → routing info.
    pub(crate) servers: Arc<tokio::sync::RwLock<HashMap<String, ServerRoutingInfo>>>,
}

/// Process-level QUIC runtime singleton.
///
/// Manages the `QuicListeners` instance and the server routing table.
/// All Xline servers in the same process share this runtime.
pub(crate) struct SharedQuicRuntime;

/// Global singleton for shared QUIC state.
static SHARED_QUIC: Mutex<Option<SharedQuicState>> = Mutex::new(None);

/// Tracks whether the singleton has been initialized.
/// Used instead of `Arc::strong_count` to reliably detect first init.
static QUIC_INITIALIZED: AtomicBool = AtomicBool::new(false);

impl SharedQuicRuntime {
    /// Get the lock guard on the global singleton.
    fn lock() -> MutexGuard<'static, Option<SharedQuicState>> {
        SHARED_QUIC.lock().expect("SHARED_QUIC lock poisoned")
    }

    /// Get or initialize the shared QUIC state.
    ///
    /// If the singleton is not yet initialized, creates a new `QuicListeners`
    /// with default server parameters and stores it.
    ///
    /// # Returns
    ///
    /// A tuple of `(Arc<QuicListeners>, SharedQuicHandle)`.
    ///
    /// # Errors
    ///
    /// Returns an error if `QuicListeners::builder().listen()` fails.
    pub(crate) fn get_or_init() -> anyhow::Result<(Arc<QuicListeners>, SharedQuicHandle)> {
        let mut guard = Self::lock();
        let is_first = if guard.is_none() {
            info!("Creating QuicListeners (FIRST server)");
            let listeners = QuicListeners::builder()
                .without_client_cert_verifier()
                .with_parameters(dquic::prelude::handy::server_parameters())
                .enable_0rtt()
                .with_alpns(["h3"])
                .listen(4096)
                .map_err(|e| anyhow::anyhow!("QuicListeners::builder failed: {e}"))?;

            let shared = SharedQuicState {
                listeners,
                servers: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            };
            *guard = Some(shared);
            QUIC_INITIALIZED.store(true, Ordering::Release);
            true
        } else {
            false
        };

        let state = guard
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("SHARED_QUIC not initialized (should not happen)"))?;

        let listeners = Arc::clone(&state.listeners);
        let servers = Arc::clone(&state.servers);

        Ok((listeners, SharedQuicHandle { servers, is_first }))
    }

    /// Shut down the shared QUIC runtime.
    ///
    /// This shuts down the `QuicListeners` and clears the singleton.
    /// Intended for **process-exit cleanup only**.
    pub(crate) fn shutdown() {
        let mut guard = Self::lock();
        if let Some(state) = guard.take() {
            info!("SharedQuicRuntime::shutdown: shutting down QuicListeners");
            state.listeners.shutdown();
            QUIC_INITIALIZED.store(false, Ordering::Release);
        }
    }
}

/// Handle to the shared QUIC state, returned by [`SharedQuicRuntime::get_or_init`].
///
/// Contains the server routing table and indicates whether this is the first
/// initialization (which determines who runs the accept loop).
pub(crate) struct SharedQuicHandle {
    /// Server routing table.
    pub(crate) servers: Arc<tokio::sync::RwLock<HashMap<String, ServerRoutingInfo>>>,
    /// Whether this handle was created during the first initialization.
    /// The first server is responsible for running the accept loop.
    pub(crate) is_first: bool,
}
