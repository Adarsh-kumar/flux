use std::io::{self, Read};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use mio::{Events, Interest, Poll, Token};
use mio::net::TcpListener;
use mio::unix::pipe;

use crate::worker::{AcceptedConnection, WorkerHandle};

const LISTENER_TOKEN: Token = Token(0);
const SHUTDOWN_TOKEN: Token = Token(1);

/// The boss: accepts TCP connections and distributes them to workers.
///
/// SRP: accept + distribute. The boss does NOT own workers, manage threads,
/// or handle shutdown lifecycle. It borrows workers for distribution and
/// blocks on `poll(None)` until either a new connection arrives or the
/// shutdown pipe is written to by the Ctrl+C handler.
pub struct Boss<'w> {
    listener: TcpListener,
    poll: Poll,
    events: Events,
    shutdown_rx: pipe::Receiver,
    workers: &'w [WorkerHandle],
    next_worker: usize,
    shutdown: Arc<AtomicBool>,
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

        let mut boss = Boss {
            listener,
            poll,
            events: Events::with_capacity(128),
            shutdown_rx,
            workers,
            next_worker: 0,
            shutdown,
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
                    _ => {}
                }
            }
            if should_break {
                break;
            }
        }
    }

    fn accept_pending(&mut self) {
        loop {
            match self.listener.accept() {
                Ok((stream, addr)) => {
                    log::debug!("accepted connection from {}", addr);
                    self.distribute(stream, addr);
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

    fn distribute(&mut self, stream: mio::net::TcpStream, addr: std::net::SocketAddr) {
        let mut conn = AcceptedConnection { stream, peer_addr: addr };
        let worker_count = self.workers.len();

        for offset in 0..worker_count {
            let idx = (self.next_worker + offset) % worker_count;
            match self.workers[idx].try_assign(conn) {
                Ok(()) => {
                    self.next_worker = (idx + 1) % worker_count;
                    self.workers[idx].notify();
                    return;
                }
                Err(c) => conn = c,
            }
        }

        log::warn!(
            "all {} worker(s) backed up — dropping connection from {}",
            worker_count,
            addr,
        );
    }
}
