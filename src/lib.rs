//! flux — A Netty-like event-driven networking framework for Rust.
//!
//! ## Architecture
//!
//! ```text
//! Boss Thread (1)              Worker Threads (N = CPU cores)
//! ┌──────────────────┐        ┌──────────────────────┐
//! │ mio::Poll         │        │ mio::Poll (instance 0)│
//! │  listener socket  │assign──│  Session              │
//! │  accept() loop    │conn → │   ├─ TcpStream (FD)    │
//! └──────────────────┘        │   ├─ Pipeline          │
//!                             │   ├─ read/write buf    │
//!                             │   └─ handler callbacks │
//!                             └──────────────────────┘
//!                             ┌──────────────────────┐
//!                             │ mio::Poll (instance 1)│
//!                             │  ...                  │
//!                             └──────────────────────┘
//! ```
//!
//! A connection, once assigned to a worker, is handled exclusively by that
//! worker for its entire lifetime. There is no work-stealing, no task
//! migration, no async/await in the hot path.
//!
//! ## Usage (Phase 1)
//!
//! ```rust,ignore
//! use flux::{Server, ServerConfig};
//! use flux::handler::{Pipeline, InboundHandler, HandlerContext};
//! use std::sync::Arc;
//!
//! let server = Server::new(ServerConfig {
//!     port: 9001,
//!     worker_threads: num_cpus::get(),
//!     pipeline_initializer: Some(Arc::new(|pipeline: &mut Pipeline| {
//!         pipeline.add_inbound(Box::new(MyHandler));
//!     })),
//! });
//! server.run();
//! ```

pub mod boss;
pub mod handler;
pub mod session;
pub mod worker;

use std::fmt;
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use mio::unix::pipe;

use crate::handler::Pipeline;

// ──────────────────────────────────────────
// Public API
// ──────────────────────────────────────────

/// Configuration for the flux server.
#[derive(Clone)]
pub struct ServerConfig {
    /// TCP port to bind to.
    pub port: u16,

    /// Number of worker threads. Each worker runs its own `mio::Poll` event loop.
    /// Default: number of CPU cores.
    pub worker_threads: usize,

    /// Called once per new connection to populate its [`Pipeline`].
    ///
    /// If `None` (default), the pipeline starts empty — all inbound events
    /// are no-ops, outbound writes go straight to the socket.
    pub pipeline_initializer: Option<Arc<dyn Fn(&mut Pipeline) + Send + Sync>>,
}

impl fmt::Debug for ServerConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServerConfig")
            .field("port", &self.port)
            .field("worker_threads", &self.worker_threads)
            .field("pipeline_initializer", &self.pipeline_initializer.as_ref().map(|_| ".."))
            .finish()
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            port: 9001,
            worker_threads: num_cpus::get(),
            pipeline_initializer: None,
        }
    }
}

/// The flux server.
///
/// Constructed with [`ServerConfig`], spawns worker threads on [`Server::run`].
pub struct Server {
    config: ServerConfig,
}

// Review: What is the purpose of this impl block ?
// When is this block called ? What is the purpose of this run method ?
impl Server {
    /// Create a new server with the given configuration.
    pub fn new(config: ServerConfig) -> Self {
        assert!(config.worker_threads > 0, "worker_threads must be >= 1");
        Self { config }
    }

    /// Start the server.
    ///
    /// Spawns worker threads, then runs the boss accept loop on the calling
    /// thread. Blocks until Ctrl+C is received, then shuts down gracefully:
    /// stops accepting, drains workers, joins all threads.
    pub fn run(self) {
        log::info!(
            "flux: starting server on port {} with {} worker(s)",
            self.config.port,
            self.config.worker_threads,
        );

        // Shared shutdown flag + pipe. The ctrlc handler sets the flag AND
        // writes to the pipe to wake the boss from poll(None). This is the
        // same self-pipe trick used by Netty's Selector.wakeup() and Go's
        // netpollBreak — no polling, no timeouts.
        let shutdown = Arc::new(AtomicBool::new(false));
        let (shutdown_tx, shutdown_rx) =
            pipe::new().expect("failed to create boss shutdown pipe");
        let s = shutdown.clone();
        let _ = ctrlc::set_handler(move || {
            log::info!("flux: Ctrl+C received, shutting down...");
            s.store(true, Ordering::SeqCst);
            let _ = (&shutdown_tx).write(&[1u8]);
        });

        // Drain pipes: workers → boss. One pipe per worker. When a worker
        // frees an mpsc slot it writes 1 byte so the boss can resume
        // accept() after backpressure.
        let mut drain_senders = Vec::with_capacity(self.config.worker_threads);
        let mut drain_receivers = Vec::with_capacity(self.config.worker_threads);
        for _ in 0..self.config.worker_threads {
            let (tx, rx) = pipe::new().expect("failed to create drain pipe");
            drain_senders.push(tx);
            drain_receivers.push(rx);
        }

        // Spawn workers.
        let initializer = self.config.pipeline_initializer.clone();
        let workers: Vec<worker::WorkerHandle> = drain_senders
            .into_iter()
            .enumerate()
            .map(|(id, tx)| worker::Worker::spawn(id, initializer.clone(), tx))
            .collect();

        // Run the boss accept loop. Blocks until shutdown pipe wakes it.
        boss::Boss::run(self.config.port, &workers, shutdown, shutdown_rx, drain_receivers);

        // Boss returned — shutdown initiated. Drop the WorkerHandle vec:
        // closing the pipe senders wakes each worker's epoll immediately
        // (EOF on pipe read end), and closing the mpsc senders signals
        // channel disconnect so workers drain sessions and exit.
        let handles: Vec<_> = workers.into_iter().map(|w| w.thread).collect();
        for handle in handles {
            let _ = handle.join();
        }

        println!("flux: server shut down.");
    }
}

// ──────────────────────────────────────────
// Private helpers
// ──────────────────────────────────────────

/// Minimal CPU count detection (avoids pulling in the `num_cpus` crate).
mod num_cpus {
    pub fn get() -> usize {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
    }
}
