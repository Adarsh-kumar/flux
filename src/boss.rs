use std::io::{self, Read};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use mio::{Events, Interest, Poll, Token};
use mio::net::TcpListener;
use mio::unix::pipe;

use crate::worker::{AcceptedConnection, WorkerHandle};

const LISTENER_TOKEN: Token = Token(0);
const SHUTDOWN_TOKEN: Token = Token(1);
const DRAIN_TOKEN_BASE: usize = 2;

/// The boss: accepts TCP connections and distributes them to workers.
///
/// SRP: accept + distribute. The boss does NOT own workers, manage threads,
/// or handle shutdown lifecycle. It borrows workers for distribution and
/// blocks on `poll(None)` until either a new connection arrives, a worker
/// drains (backpressure release), or the shutdown pipe is written to.
pub struct Boss<'w> {
    listener: TcpListener,
    poll: Poll,
    events: Events,
    shutdown_rx: pipe::Receiver,
    drain_receivers: Vec<pipe::Receiver>,
    workers: &'w [WorkerHandle],
    next_worker: usize,
    shutdown: Arc<AtomicBool>,
    accept_paused: bool,
    /// Spin counter for accept_pending early returns — diagnostic only.
    spin_count: u64,
    /// A connection that could not be distributed because all worker queues
    /// were full. Held here (rather than dropped) so the OS does not send
    /// RST. Flushed by `try_resume` when a drain signal arrives.
    pending: Option<AcceptedConnection>,
}

impl<'w> Boss<'w> {
    /// Run the accept loop on the calling thread.
    ///
    /// Blocks on `poll(None)` until either a new connection arrives or the
    /// shutdown pipe is written to by the Ctrl+C handler. Then returns.
    /// The caller (Server) owns the workers and handles drop + join.
    pub fn run(
        port: u16,
        workers: &[WorkerHandle],
        shutdown: Arc<AtomicBool>,
        mut shutdown_rx: pipe::Receiver,
        mut drain_receivers: Vec<pipe::Receiver>,
    ) {
        let addr = format!("0.0.0.0:{}", port);
        let mut listener = match TcpListener::bind(addr.parse().unwrap()) {
            Ok(l) => l,
            Err(e) => {
                log::error!("Failed to bind to port {}: {}", port, e);
                return;
            }
        };

        let poll = match Poll::new() {
            Ok(p) => p,
            Err(e) => {
                log::error!("Failed to create boss poll: {}", e);
                return;
            }
        };

        // Register listener socket.
        poll.registry()
            .register(&mut listener, LISTENER_TOKEN, Interest::READABLE)
            .expect("failed to register listener with poll");

        // Register shutdown pipe — written to by the Ctrl+C handler.
        poll.registry()
            .register(&mut shutdown_rx, SHUTDOWN_TOKEN, Interest::READABLE)
            .expect("failed to register shutdown pipe with poll");

        // Register drain pipes — one per worker. Workers write 1 byte when
        // they free an mpsc slot so the boss can resume accept().
        for (i, rx) in drain_receivers.iter_mut().enumerate() {
            poll.registry()
                .register(rx, Token(DRAIN_TOKEN_BASE + i), Interest::READABLE)
                .expect("failed to register drain pipe with poll");
        }

        let mut boss = Boss {
            listener,
            poll,
            events: Events::with_capacity(128),
            shutdown_rx,
            drain_receivers,
            workers,
            next_worker: 0,
            shutdown,
            accept_paused: false,
            spin_count: 0,
            pending: None,
        };

        log::info!("flux: boss started, listening on port {}", port);

        loop {
            // Block indefinitely — wakes on new connection or shutdown signal.
            if let Err(e) = boss.poll.poll(&mut boss.events, None) {
                if e.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                log::error!("boss poll error: {}", e);
                break;
            }

            // Collect readiness first to avoid borrow conflict:
            // events.iter() borrows self immutably, but accept_pending()
            // and drain_shutdown_pipe() need &mut self.
            let pending: Vec<(Token, bool)> = boss
                .events
                .iter()
                .map(|e| (e.token(), e.is_readable()))
                .collect();

            let mut should_break = false;
            for (token, readable) in pending {
                match token {
                    SHUTDOWN_TOKEN if readable => {
                        boss.drain_shutdown_pipe();
                        if boss.shutdown.load(Ordering::SeqCst) {
                            log::info!("flux: boss received shutdown signal");
                            should_break = true;
                        }
                    }
                    LISTENER_TOKEN if readable => {
                        boss.accept_pending();
                    }
                    t if readable && t.0 >= DRAIN_TOKEN_BASE => {
                        boss.drain_drain_pipe(t.0 - DRAIN_TOKEN_BASE);
                        boss.try_resume();
                    }
                    _ => {}
                }
            }
            if should_break {
                break;
            }
        }
    }

    fn accept_pending(&mut self) {
        if self.accept_paused {
            self.spin_count += 1;
            return;
        }
        loop {
            match self.listener.accept() {
                Ok((stream, addr)) => {
                    log::debug!("accepted connection from {}", addr);
                    let conn = AcceptedConnection { stream, peer_addr: addr };
                    if !self.distribute(conn) {
                        // All workers full — distribute() set accept_paused and
                        // stored the connection in self.pending. Stop the loop.
                        break;
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) => {
                    log::error!("accept error: {}", e);
                    break;
                }
            }
        }
    }

    /// Drain the shutdown pipe so it stops appearing readable.
    fn drain_shutdown_pipe(&mut self) {
        let mut buf = [0u8; 64];
        let _ = self.shutdown_rx.read(&mut buf);
    }

    /// Drain a backpressure release pipe by index.
    ///
    /// Must loop until `WouldBlock` — mio uses edge-triggered epoll so a
    /// partial read leaves bytes behind and the pipe never fires again.
    fn drain_drain_pipe(&mut self, idx: usize) {
        let mut buf = [0u8; 64];
        loop {
            match self.drain_receivers[idx].read(&mut buf) {
                Ok(0) => break,
                Ok(_) => continue,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(_) => break,
            }
        }
    }

    /// Resume accepting after a backpressure pause.
    ///
    /// Tries to flush `self.pending` first. Only clears `accept_paused` and
    /// re-opens the accept loop if there is no pending connection or the
    /// pending one was successfully distributed. If workers are still full,
    /// stays paused — `distribute` re-stores the connection in `self.pending`.
    fn try_resume(&mut self) {
        if let Some(conn) = self.pending.take() {
            if !self.distribute(conn) {
                return; // still full — distribute() re-stored pending
            }
        }
        self.accept_paused = false;
        eprintln!("BOSS: resumed accept — spun {} times while paused", self.spin_count);
        self.spin_count = 0;
        self.accept_pending();
    }

    /// Try to distribute a connection across workers (round-robin).
    ///
    /// Returns `true` if a worker accepted it. Returns `false` if all worker
    /// queues are full — in that case the connection is stored in
    /// `self.pending` (so it is not dropped/RST'd) and `accept_paused` is set.
    fn distribute(&mut self, conn: AcceptedConnection) -> bool {
        let mut conn = conn;
        let worker_count = self.workers.len();

        for offset in 0..worker_count {
            let idx = (self.next_worker + offset) % worker_count;
            match self.workers[idx].try_assign(conn) {
                Ok(()) => {
                    self.next_worker = (idx + 1) % worker_count;
                    self.workers[idx].notify();
                    return true;
                }
                Err(c) => conn = c,
            }
        }

        log::warn!(
            "all {} worker(s) backed up — pausing accept",
            worker_count,
        );
        self.pending = Some(conn);
        self.accept_paused = true;
        false
    }
}
