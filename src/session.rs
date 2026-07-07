use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::io::{self, Read, Write};
use std::net::SocketAddr;

use mio::net::TcpStream;

use crate::handler::{HandlerContext, Pipeline};

// ──────────────────────────────────────────
// Session — per-connection I/O + pipeline
// ──────────────────────────────────────────

/// A Session wraps a TCP connection's FD, its handler pipeline, and its
/// write buffer. It is the flux equivalent of Netty's `NioSocketChannel`.
///
/// ## Read path
///
/// The read buffer lives **outside** Session — the worker owns it and passes
/// it to [`read_from_fd`]. This avoids a borrow conflict: pipeline dispatch
/// needs `&mut Session`, but a read-buffer `&[u8]` returned by Session would
/// borrow `Session` immutably. By keeping the buffer external, the worker
/// reads first, then dispatches with data it already owns.
///
/// ## Write path
///
/// `write_buf` and `writable_registered` use interior mutability (`RefCell` /
/// `Cell`) so that handlers can call `ctx.session().write()` through a shared
/// `&Session` reference.
///
/// ## Lifecycle
///
/// 1. Created on TCP accept by the worker.
/// 2. Pipeline populated by the user's initializer.
/// 3. FD registered with mio for `READABLE`.
/// 4. I/O events flow through `read_from_fd` / `flush_to_fd` → `Pipeline`.
/// 5. Dropped when the connection closes (EOF, error, or explicit close).
///
/// [`read_from_fd`]: Session::read_from_fd
pub struct Session {
    stream: TcpStream,
    pipeline: RefCell<Pipeline>,
    /// Outgoing data queued by handlers via `ctx.session().write()`.
    write_buf: RefCell<VecDeque<u8>>,
    /// Whether the FD is currently registered for `WRITABLE` interest with mio.
    writable_registered: Cell<bool>,
    peer_addr: SocketAddr,
    session_id: u64,
}

impl Session {
    /// Create a new session.
    pub fn new(
        stream: TcpStream,
        pipeline: Pipeline,
        peer_addr: SocketAddr,
        session_id: u64,
    ) -> Self {
        Self {
            stream,
            pipeline: RefCell::new(pipeline),
            write_buf: RefCell::new(VecDeque::with_capacity(4096)),
            writable_registered: Cell::new(false),
            peer_addr,
            session_id,
        }
    }

    // ── Read path ──

    /// Read available data from the socket into `buf`.
    ///
    /// `buf` is resized to at least 8192, filled from the socket, then
    /// truncated to the actual bytes read.
    ///
    /// Returns the number of bytes read. 0 means EOF (peer closed).
    pub fn read_from_fd(&mut self, buf: &mut Vec<u8>) -> io::Result<usize> {
        let cap = buf.capacity().max(8192);
        buf.resize(cap, 0);
        let n = self.stream.read(buf)?;
        buf.truncate(n);
        Ok(n)
    }

    // ── Write path ──

    /// Append data to the outbound write buffer.
    ///
    /// Runs the outbound pipeline first (handlers in reverse order see the
    /// data via `on_write`), then buffers the result. Callable from handlers
    /// via `ctx.session().write(data)`.
    ///
    /// The worker must ensure the FD is registered for `WRITABLE` interest.
    pub fn write(&self, data: &[u8]) {
        // If the pipeline is already borrowed (e.g. during a fire_read
        // dispatch), skip the outbound pipeline. The data is still buffered
        // and will be flushed to the socket on the next writable event.
        if let Ok(mut pipeline) = self.pipeline.try_borrow_mut() {
            let mut ctx = HandlerContext::new(self);
            pipeline.fire_write(&mut ctx, data);
        }
        self.write_buf.borrow_mut().extend(data);
    }

    /// Flush the write buffer to the socket.
    ///
    /// Writes as much as possible without blocking. Returns `Ok(())` even
    /// when blocked — the caller retries on the next writable event.
    pub fn flush_to_fd(&mut self) -> io::Result<()> {
        let mut buf = self.write_buf.borrow_mut();
        while !buf.is_empty() {
            let contiguous = buf.make_contiguous();
            match self.stream.write(contiguous) {
                Ok(0) => break,
                Ok(n) => {
                    buf.drain(..n);
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// Whether there is data waiting to be flushed.
    pub fn has_pending_writes(&self) -> bool {
        !self.write_buf.borrow().is_empty()
    }

    /// Whether the FD is currently registered for `WRITABLE` with mio.
    pub fn is_writable_registered(&self) -> bool {
        self.writable_registered.get()
    }

    /// Set the `WRITABLE` registration flag.
    pub fn set_writable_registered(&self, registered: bool) {
        self.writable_registered.set(registered);
    }

    // ── Lifecycle ──

    /// Gracefully shut down the write side of the connection.
    ///
    /// Sends FIN to the peer. The read side remains open so we can
    /// still receive any data the peer sends before closing.
    pub fn shutdown(&mut self) -> io::Result<()> {
        self.stream.shutdown(std::net::Shutdown::Write)
    }

    // ── Pipeline access ──

    /// Access the handler pipeline via interior mutability.
    ///
    /// Use `session.pipeline().borrow_mut()` to get `&mut Pipeline` for event
    /// dispatch, or `borrow()` for read-only access.
    pub fn pipeline(&self) -> &RefCell<Pipeline> {
        &self.pipeline
    }

    // ── Metadata ──

    /// Unique session identifier.
    pub fn session_id(&self) -> u64 {
        self.session_id
    }

    /// The peer's socket address.
    pub fn peer_addr(&self) -> std::net::SocketAddr {
        self.peer_addr
    }

    /// Direct mutable access to the inner [`TcpStream`].
    ///
    /// This is `pub(crate)` — the worker uses it during connection setup
    /// (mio registration) before the session enters the connection map.
    /// Once inserted, only [`read_from_fd`] and [`flush_to_fd`] touch the FD.
    ///
    /// [`read_from_fd`]: Session::read_from_fd
    /// [`flush_to_fd`]: Session::flush_to_fd
    pub(crate) fn stream_direct(&mut self) -> &mut TcpStream {
        &mut self.stream
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        log::debug!(
            "session {} (peer {}) dropped — connection closed",
            self.session_id,
            self.peer_addr,
        );
    }
}
