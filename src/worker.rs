use std::collections::{HashMap, VecDeque};
use std::io::{self, Read, Write};
use std::net::SocketAddr;
use std::sync::mpsc::{self, TryRecvError};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use mio::{Events, Interest, Poll, Token};
use mio::net::TcpStream;
use mio::unix::pipe;

use crate::handler::{Pipeline, PipelineExecutor};
use crate::session::Session;

// ──────────────────────────────────────────
// Tokens
// ──────────────────────────────────────────

/// Token for the wake-up pipe. Session tokens start at 1 and never reach
/// `usize::MAX`, so there is no collision.
const WAKE_TOKEN: Token = Token(usize::MAX);

/// Maximum number of `read()` calls per `process_readable` invocation.
///
/// Once exhausted the session is pushed onto `carry_over` and given another
/// pass at the end of the current event-loop iteration before `poll()` is
/// called again. This bounds the time any single connection can monopolise
/// the worker thread, preventing head-of-line blocking on co-located sessions.
const READ_BUDGET: usize = 16;

// ──────────────────────────────────────────
// AcceptedConnection — sent from boss to worker
// ──────────────────────────────────────────

/// A freshly accepted TCP connection, handed from the boss to a worker.
pub struct AcceptedConnection {
    pub stream: TcpStream,
    pub peer_addr: SocketAddr,
}

// ──────────────────────────────────────────
// WorkerHandle — held by the boss
// ──────────────────────────────────────────

/// A handle to a running worker thread.
///
/// The boss sends accepted connections through `tx` and then writes a single
/// byte to `notify` to wake the worker's `epoll_wait`. This is how the worker
/// can use `poll(None)` (infinite sleep) without polling or timeouts.
pub struct WorkerHandle {
    pub id: usize,
    pub tx: mpsc::SyncSender<AcceptedConnection>,
    pub thread: thread::JoinHandle<()>,
    notify: pipe::Sender,
}

impl WorkerHandle {
    /// Try to assign a connection to this worker. Non-blocking.
    ///
    /// Returns `Ok(())` if the connection was queued, `Err(conn)` if the worker's
    /// channel is full or disconnected.
    pub fn try_assign(&self, conn: AcceptedConnection) -> Result<(), AcceptedConnection> {
        self.tx.try_send(conn).map_err(|e| match e {
            mpsc::TrySendError::Full(c) => c,
            mpsc::TrySendError::Disconnected(c) => c,
        })
    }

    /// Wake the worker's event loop. Called by the boss after a successful
    /// `try_assign` so the worker knows to drain the channel.
    pub fn notify(&self) {
        // Writing 1 byte to the pipe makes it readable, waking epoll_wait.
        // If the pipe buffer is full (shouldn't happen in practice), ignore.
        let _ = (&self.notify).write(&[1u8]);
    }
}

// ──────────────────────────────────────────
// Worker — one event loop per CPU core
// ──────────────────────────────────────────

/// A worker thread: one OS thread, one `mio::Poll` instance.
///
/// ## Wake-up mechanism
///
/// The worker registers a Unix pipe alongside all session FDs. The boss writes
/// 1 byte to the pipe after queuing a new connection. This lets the worker use
/// `poll(None)` — infinite sleep — without timeouts. Same pattern as Go's
/// `netpollBreak` eventfd.
///
/// ## Responsibilities
///
/// 1. **Poll** — `epoll_wait(None)` on session FDs + wake pipe.
/// 2. **Receive** — drain new connections from the boss channel.
/// 3. **Dispatch** — delegate I/O events to the pipeline executor.
pub struct Worker {
    id: usize,
    poll: Poll,
    events: Events,
    rx: mpsc::Receiver<AcceptedConnection>,
    pipe_rx: pipe::Receiver,
    drain_tx: pipe::Sender,
    connections: HashMap<Token, Session>,
    next_token: usize,
    next_session_id: u64,
    initializer: Option<Arc<dyn Fn(&mut Pipeline) + Send + Sync>>,
    executor: PipelineExecutor,
    /// Sessions that hit `READ_BUDGET` before `WouldBlock` this iteration.
    /// Drained at the end of each loop before re-entering `poll()` so their
    /// remaining kernel buffer data is not stranded waiting for a new ET edge.
    carry_over: VecDeque<Token>,
}

impl Worker {
    /// Spawn a worker on a new OS thread. Returns a [`WorkerHandle`] that the
    /// boss uses to send accepted connections.
    ///
    /// `initializer` is called once per new connection to populate the pipeline.
    /// If `None`, the pipeline starts empty (all events are no-ops).
    pub fn spawn(
        id: usize,
        initializer: Option<Arc<dyn Fn(&mut Pipeline) + Send + Sync>>,
        drain_tx: pipe::Sender,
    ) -> WorkerHandle {
        let (tx, rx) = mpsc::sync_channel::<AcceptedConnection>(256);

        // Create the wake-up pipe. The receiver stays with the worker and gets
        // registered with epoll. The sender goes to the boss via WorkerHandle.
        let (pipe_tx, mut pipe_rx) =
            pipe::new().expect("failed to create worker wake-up pipe");

        let handle = thread::Builder::new()
            .name(format!("flux-worker-{}", id))
            .spawn(move || {
                let poll = Poll::new().expect("failed to create worker poll");

                // Register the pipe receiver so boss wake-ups trigger epoll.
                poll.registry()
                    .register(&mut pipe_rx, WAKE_TOKEN, Interest::READABLE)
                    .expect("failed to register wake-up pipe with poll");

                let mut worker = Worker {
                    id,
                    poll,
                    events: Events::with_capacity(1024),
                    rx,
                    pipe_rx,
                    drain_tx,
                    connections: HashMap::with_capacity(1024),
                    next_token: 1,
                    next_session_id: 1,
                    initializer,
                    executor: PipelineExecutor::new(),
                    carry_over: VecDeque::new(),
                };
                worker.run();
            })
            .expect("failed to spawn worker thread");

        WorkerHandle {
            id,
            tx,
            thread: handle,
            notify: pipe_tx,
        }
    }

    /// The worker event loop.
    ///
    /// Uses `poll(None)` (infinite sleep) when there is no carry-over work.
    /// Switches to `poll(Some(0))` (non-blocking) when sessions hit the read
    /// budget mid-iteration so their remaining data is drained before the
    /// worker sleeps again.
    fn run(&mut self) {
        log::info!("flux: worker {} started", self.id);

        let mut read_buf: Vec<u8> = Vec::with_capacity(8192);

        loop {
            // ── 1. Check for shutdown (channel disconnected) ──
            if !self.handle_connections() {
                log::info!("worker {}: shutting down...", self.id);
                self.drain_all_sessions();
                return;
            }

            // ── 2. Poll ──
            // Sleep indefinitely when there is no deferred work. Switch to a
            // non-blocking check when carry-over tokens exist so we collect
            // any new epoll events and then immediately continue draining.
            let timeout = if self.carry_over.is_empty() {
                None
            } else {
                Some(Duration::ZERO)
            };
            if let Err(e) = self.poll.poll(&mut self.events, timeout) {
                if e.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                log::error!("worker {}: poll error: {}", self.id, e);
                continue;
            }

            // ── 3. Collect event readiness ──
            let pending: Vec<(Token, bool, bool, bool)> = self
                .events
                .iter()
                .map(|e| {
                    (
                        e.token(),
                        e.is_readable(),
                        e.is_writable(),
                        e.is_error() || e.is_read_closed(),
                    )
                })
                .collect();

            // ── 4. Process fresh epoll events ──
            for (token, readable, writable, closed) in pending {
                if token == WAKE_TOKEN {
                    self.drain_pipe();
                    if !self.handle_connections() {
                        log::info!("worker {}: shutting down...", self.id);
                        self.drain_all_sessions();
                        return;
                    }
                    continue;
                }

                if closed || readable {
                    self.process_readable(token, &mut read_buf);
                }
                if writable {
                    self.process_writable(token);
                }
            }

            // ── 5. Drain carry-over tokens (generation swap) ──
            // mem::take swaps self.carry_over with a fresh empty VecDeque.
            // Tokens that hit the budget AGAIN during this pass are pushed
            // into the new self.carry_over (next generation) and handled on
            // the following iteration — bounding work per outer loop turn.
            let carry = std::mem::take(&mut self.carry_over);
            for token in carry {
                self.process_readable(token, &mut read_buf);
            }
        }
    }

    // ── Connection intake ──

    /// Handle new connections from the boss channel.
    ///
    /// Returns `false` if the boss has dropped the sender (shutdown),
    /// `true` if the channel is still alive (even if empty).
    fn handle_connections(&mut self) -> bool {
        loop {
            match self.rx.try_recv() {
                Ok(conn) => {
            // Signal boss: we drained a connection, an mpsc slot is free.
            let _ = (&mut self.drain_tx).write(&[1u8]);

            let token = Token(self.next_token);
            self.next_token = self.next_token.wrapping_add(1);

            // 1. Build and populate the pipeline.
            let mut pipeline = Pipeline::new();
            if let Some(ref init) = self.initializer {
                init(&mut pipeline);
            }

            // 2. Create the session.
            let mut session = Session::new(
                conn.stream,
                pipeline,
                conn.peer_addr,
                self.next_session_id,
            );
            self.next_session_id += 1;

            log::debug!(
                "worker {}: registering session {} (peer {}) as {:?}",
                self.id,
                session.session_id(),
                session.peer_addr(),
                token,
            );

            // 3. Register the FD with mio for READABLE interest.
            if let Err(e) = self.poll.registry().register(
                session.stream_direct(),
                token,
                Interest::READABLE,
            ) {
                log::error!(
                    "worker {}: failed to register session {}: {}",
                    self.id,
                    session.session_id(),
                    e,
                );
                continue;
            }

            // 4. Fire on_connect through the pipeline.
            self.executor.fire_connect(&session);

            // 5. If on_connect wrote data, register WRITABLE immediately so
            //    the worker can flush it without waiting for the client to
            //    send first (which may never happen for server-push patterns).
            update_writable(&self.poll, &mut session, token, self.id);

            // 6. Insert into the connection map.
            self.connections.insert(token, session);
                }
                Err(TryRecvError::Empty) => return true,
                Err(TryRecvError::Disconnected) => {
                    log::debug!("worker {}: boss channel disconnected", self.id);
                    return false;
                }
            }
        }
    }

    // ── I/O event processing ──

    /// Process a readable or closed event on a connection.
    ///
    /// mio uses edge-triggered epoll (`EPOLLET`) on Linux. A single `read()`
    /// is not sufficient — after one call the fd may still have data in the
    /// kernel buffer, but no new edge transition will occur, so `poll()` will
    /// not fire for this fd again until the client sends more data. We loop
    /// `read_from_fd` until `WouldBlock` to fully drain the kernel buffer
    /// within a single epoll notification.
    fn process_readable(&mut self, token: Token, read_buf: &mut Vec<u8>) {
        let mut session = match self.connections.remove(&token) {
            Some(s) => s,
            None => return,
        };

        let mut reads: usize = 0;
        loop {
            if reads >= READ_BUDGET {
                // Budget exhausted — kernel buffer may still hold data but
                // continuing would starve co-located sessions. Re-insert and
                // defer: the carry-over queue will give this token another
                // READ_BUDGET pass at the end of this iteration (or next if
                // it was already a carry-over), without blocking on poll().
                self.carry_over.push_back(token);
                self.connections.insert(token, session);
                return;
            }

            match session.read_from_fd(read_buf) {
                Ok(0) => {
                    log::debug!(
                        "worker {}: session {} EOF",
                        self.id,
                        session.session_id(),
                    );
                    // Peer sent FIN. Flush pending writes, send our FIN, then
                    // fire on_disconnect. terminate() covers all three steps.
                    self.terminate(session);
                    return;
                }
                Ok(n) => {
                    reads += 1;
                    if self.executor.fire_read(&session, &read_buf[..n]) {
                        self.terminate(session);
                        return;
                    }
                    // Handler left the session open — continue draining.
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    // Kernel buffer fully drained — no carry-over needed.
                    break;
                }
                Err(e) => {
                    log::error!(
                        "worker {}: read error on session {}: {}",
                        self.id,
                        session.session_id(),
                        e,
                    );
                    if self.executor.fire_error(&session, &e) {
                        self.terminate(session);
                        return;
                    }
                    // Handler decided to keep the session alive despite the error.
                    break;
                }
            }
        }

        // Kernel buffer fully drained — session still open.
        // Reconcile WRITABLE interest with the session's write buffer.
        update_writable(&self.poll, &mut session, token, self.id);
        self.connections.insert(token, session);
    }

    /// Process a writable event on a connection.
    fn process_writable(&mut self, token: Token) {
        let mut session = match self.connections.remove(&token) {
            Some(s) => s,
            None => return,
        };

        if let Err(e) = session.flush_to_fd() {
            log::error!(
                "worker {}: write error on session {}: {}",
                self.id,
                session.session_id(),
                e,
            );
            if self.executor.fire_error(&session, &e) {
                self.terminate(session);
                return;
            }
        }

        update_writable(&self.poll, &mut session, token, self.id);
        self.connections.insert(token, session);
    }

    // ── Helpers ──

    /// Flush pending writes, send FIN, fire on_disconnect, then drop.
    fn terminate(&mut self, mut session: Session) {
        let _ = session.flush_to_fd();
        let _ = session.shutdown();
        self.executor.fire_disconnect(&session);
        // `session` dropped → TcpStream::drop → close(fd)
    }

    /// Terminate every active session and clear the map.
    ///
    /// Calls [`terminate`] on each session so pending writes are flushed,
    /// a FIN is sent to the peer, and `on_disconnect` fires — matching the
    /// normal per-connection close path.
    ///
    /// [`terminate`]: Worker::terminate
    fn drain_all_sessions(&mut self) {
        let sessions: Vec<_> = self.connections.drain().map(|(_, s)| s).collect();
        for session in sessions {
            self.terminate(session);
        }
    }

    /// Drain the wake-up pipe so it stops appearing readable.
    ///
    /// mio uses edge-triggered epoll (`EPOLLET`). A single read is not
    /// sufficient — if the boss wrote N bytes (one per queued connection)
    /// we must drain until `WouldBlock` or the pipe won't fire again until
    /// new data arrives, leaving connections stranded in the mpsc channel.
    fn drain_pipe(&mut self) {
        let mut buf = [0u8; 64];
        loop {
            match self.pipe_rx.read(&mut buf) {
                Ok(0) => break, // EOF: pipe sender closed (boss shutdown)
                Ok(_) => continue,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(_) => break,
            }
        }
    }
}

/// Reconcile mio WRITABLE interest with the session's write buffer.
///
/// Idempotent — registers or deregisters WRITABLE to match whether the
/// session has pending writes. Avoids spurious epoll wakeups from
/// permanently-writable TCP sockets.
fn update_writable(poll: &Poll, session: &mut Session, token: Token, worker_id: usize) {
    let wants = session.has_pending_writes();
    let is = session.is_writable_registered();
    if wants == is {
        return;
    }
    let interest = if wants {
        session.set_writable_registered(true);
        Interest::READABLE | Interest::WRITABLE
    } else {
        session.set_writable_registered(false);
        Interest::READABLE
    };
    if let Err(e) = poll.registry().reregister(session.stream_direct(), token, interest) {
        log::error!(
            "worker {}: failed to update WRITABLE for session {}: {}",
            worker_id,
            session.session_id(),
            e,
        );
    }
}
