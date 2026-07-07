# flux — Low-Level Design

## 1. Architecture Overview

```
┌─ OS Thread (main) ───────────────────────────────────────────────────────┐
│                                                                          │
│  ┌───────────────┐   channel + pipe    ┌──────────────────────────┐      │
│  │  Boss         │ ──────────────────→ │  Worker 0                │      │
│  │   mio::Poll   │                     │   mio::Poll              │      │
│  │   ├─ listener │                     │   ├─ pipe_rx (WAKE)      │      │
│  │   └─ shdn_pipe│                     │   ├─ session-1 FD        │      │
│  │   accept()    │                     │   ├─ session-2 FD        │      │
│  │   distribute  │                     │   └─ ...                 │      │
│  └───────────────┘                     └──────────────────────────┘      │
│                                        ┌──────────────────────────┐      │
│                                        │  Worker 1                │      │
│                                        │   ...                    │      │
│                                        └──────────────────────────┘      │
│                                        ┌──────────────────────────┐      │
│                                        │  Worker N                │      │
│                                        │   ...                    │      │
│                                        └──────────────────────────┘      │
└──────────────────────────────────────────────────────────────────────────┘
```

- One boss OS thread, `N` worker OS threads (default: CPU core count).
- Each thread has its own `mio::Poll` instance (one `epoll` fd per thread).
- A connection is **pinned** to a single worker for its entire lifetime. No migration, no work-stealing, no async/await in the hot path.

### Polling model

| Thread | `poll` timeout | Registered FDs | Wakes on |
|--------|---------------|----------------|----------|
| Boss | `None` (blocks indefinitely) | listener socket + shutdown pipe | New TCP connection, or shutdown pipe written by ctrlc handler |
| Worker | `500ms` (safety net) | session sockets + wake pipe | Session I/O, boss wake-up (new connections + explicit shutdown notify) |

The boss uses the **self-pipe trick** (same as Netty's `Selector.wakeup()` and Go's `netpollBreak`): the ctrlc handler writes a byte to the shutdown pipe, which interrupts `epoll_wait` immediately. No polling, no timeouts.

Workers are woken by dropping `WorkerHandle` (pipe sender close → EOF on receiver → `epoll` wakes immediately). Their 500ms timeout is a safety net for edge cases like a lost pipe close.

### Shutdown flow

```
Ctrl+C → ctrlc handler sets AtomicBool + writes to boss shutdown pipe
  → Boss wakes from poll(None), checks flag, breaks
  → Server drops WorkerHandle vec → pipe senders close (EOF wakes epoll)
    + mpsc senders close (channel disconnect)
  → Workers detect channel disconnect → drain_all_sessions → exit
  → Server joins all worker threads
```

---

## 2. Module Map & SRP

```
src/
  lib.rs          ← Public API: Server, ServerConfig. Shutdown lifecycle.
  boss.rs         ← TCP accept loop, round-robin distribution. Borrows workers.
  worker.rs       ← Per-core event loop, session lifecycle, I/O orchestration.
  session.rs      ← Per-connection wrapper: FD, pipeline, write buffer, shutdown.
  handler/
    mod.rs        ← Re-exports.
    traits.rs     ← InboundHandler / OutboundHandler / ByteToMessageDecoder.
    pipeline.rs   ← Handler chain, type filtering, handler iteration (fire_*).
    context.rs    ← HandlerContext — per-event state, secondary re-dispatch.
    executor.rs   ← PipelineExecutor — runs events, owns context + byte→message decode.
```

### 2.1 `Server` / `ServerConfig` (`lib.rs`)

**SRP:** Entry point, configuration holder, shutdown lifecycle orchestrator. Owns workers. Creates Ctrl+C handler. Drops workers and joins threads after boss returns.

| Field / Method | Purpose |
|---------------|---------|
| `ServerConfig.port` | TCP port |
| `ServerConfig.worker_threads` | Number of workers (OS threads) |
| `ServerConfig.pipeline_initializer` | `Option<Arc<dyn Fn(&mut Pipeline) + Send + Sync>>`. Called once per new connection. |
| `Server::new(config)` | Validates `worker_threads >= 1` |
| `Server::run(self)` | Spawns workers, registers Ctrl+C, runs boss, drops workers, joins threads |

### 2.2 `Boss` (`boss.rs`)

**SRP:** Accept TCP connections and distribute them to workers. Borrows workers — does NOT own them or manage threads. Checks shutdown flag and returns when set.

| Field | Purpose |
|-------|---------|
| `listener: TcpListener` | Server socket, registered with epoll for `READABLE` |
| `workers: &[WorkerHandle]` | Borrowed worker handles for distribution |
| `next_worker: usize` | Round-robin cursor |
| `shutdown: Arc<AtomicBool>` | Checked each loop iteration |

**Distribution:** round-robin with backpressure. `try_assign` via bounded mpsc (256). Falls through to next worker on `Full`. Drops connection if all full. Calls `notify()` on success to wake worker via pipe.

### 2.3 `Worker` (`worker.rs`)

**SRP:** Run an event loop on a dedicated OS thread. Owns the session map. Orchestrates I/O detection and delegates pipeline dispatch to `PipelineExecutor`. Detects shutdown via channel disconnect.

| Field | Purpose |
|-------|---------|
| `poll: Poll` | Worker's own `mio::Poll` instance |
| `rx: mpsc::Receiver<AcceptedConnection>` | Receives connections from boss. Bounded 256. |
| `pipe_rx: pipe::Receiver` | Registered at `WAKE_TOKEN`. Boss writes 1 byte per connection. |
| `connections: HashMap<Token, Session>` | All active sessions, keyed by mio token |
| `executor: PipelineExecutor` | Runs pipeline events. One instance for all sessions. |

**Event loop:**

```
loop {
    handle_connections()              ← checks shutdown + drains channel
    poll(500ms)                       ← I/O + safety-net interval
    (Interrupted → continue)           ← EINTR on signal before shutdown pipe delivered
    for each event:
      WAKE_TOKEN → drain_pipe(), handle_connections()
      readable/closed → process_readable()
      writable → process_writable()
}
```

**handle_connections → bool:** Returns `false` (shutdown) when mpsc returns `TryRecvError::Disconnected`. Returns `true` otherwise. Drains all pending connections: builds pipeline, calls initializer, creates session, registers FD, fires `fire_connect`.

**process_readable:** Remove session from map → `read_from_fd` → `fire_read` → if close requested → `terminate`. On EOF: `terminate`. On error: `fire_error`, handler decides close vs keep. On success: `ensure_writable` + re-insert.

**process_writable:** Remove → `flush_to_fd` → if empty: `deregister_writable` → re-insert. On error: `fire_error`, handler decides.

**terminate:** `flush_to_fd` → `shutdown(Write)` → `fire_disconnect` → drop (close fd).

**ensure_writable / deregister_writable:** Register/deregister `WRITABLE` interest dynamically. Only sessions with pending writes get writable events — avoids O(N) spurious wakeups.

### 2.4 `Session` (`session.rs`)

**SRP:** Owns one TCP connection's FD, pipeline, and write buffer. Provides I/O + lifecycle API.

| Field | Purpose |
|-------|---------|
| `stream: TcpStream` | Socket FD |
| `pipeline: RefCell<Pipeline>` | Per-connection handler chain |
| `write_buf: RefCell<VecDeque<u8>>` | Outbound data queued by handlers |
| `writable_registered: Cell<bool>` | Whether `WRITABLE` is registered with mio |

**Key methods:**

| Method | Purpose |
|--------|---------|
| `read_from_fd(&mut self, buf: &mut Vec<u8>) → io::Result<usize>` | Read from socket into external buffer. 0 = EOF. |
| `write(&self, data: &[u8])` | Runs outbound pipeline (if not borrowed), buffers data |
| `flush_to_fd(&mut self) → io::Result<()>` | Drain write_buf to socket. Stops on WouldBlock. |
| `shutdown(&mut self) → io::Result<()>` | `shutdown(Write)` — send FIN to peer |
| `pipeline(&self) → &RefCell<Pipeline>` | Pipeline access via interior mutability |

**External read buffer:** Worker owns the read buffer, avoiding `&session.read_data()` vs `&mut session.pipeline()` borrow conflict.

**try_borrow_mut in write():** If pipeline is already borrowed for read dispatch, outbound pipeline is skipped. Data is still buffered.

---

## 3. Handler Module

### 3.1 Traits (`traits.rs`)

```rust
pub trait InboundHandler: Send + 'static {
    fn accepted_types(&self) -> Vec<TypeId> { vec![] }
    fn on_connect(&mut self, ctx: &mut HandlerContext<'_>) {}
    fn on_read(&mut self, ctx: &mut HandlerContext<'_>, msg: &dyn Any) {}
    fn on_disconnect(&mut self, ctx: &mut HandlerContext<'_>) {}
    fn on_error(&mut self, ctx: &mut HandlerContext<'_>, err: &io::Error) {
        ctx.close();  // default: unhandled errors close the connection
    }
}
```

**Type-based dispatch:** `accepted_types()` returns `Vec<TypeId>`. Empty = receive all messages. Non-empty = only matching types fire `on_read`. Raw bytes from the socket are boxed as `Vec<u8>`.

**Error default:** `on_error` calls `ctx.close()` — unhandled I/O errors close the connection. Override to recover without closing.

**Lifecycle defaults:** `on_connect` and `on_disconnect` are no-ops. Notification only.

### 3.2 Pipeline (`pipeline.rs`)

**Two-layer dispatch:**

| Layer | Methods | Purpose |
|-------|---------|---------|
| Internal `pub(crate)` | `fire_connect`, `fire_read`, `fire_disconnect`, `fire_error`, `fire_write` | Iterate handlers with provided context |

**fire_read:** Single pass through handlers, checking each handler's `accepted_types()` against `msg.type_id()`. Empty list = catch-all, non-empty = only matching types.

**Decoder:** An optional `ByteToMessageDecoder` converts raw `Vec<u8>` into typed messages before dispatch. Set via `set_decoder()`. When configured, handlers never see raw bytes.

**Iteration order:** Inbound 0→N, Outbound N→0.

### 3.3 HandlerContext (`context.rs`)

| Method | Purpose |
|--------|---------|
| `ctx.session() → &Session` | Session access for `write()`, `session_id()`, `peer_addr()` |
| `ctx.stop_inbound()` / `ctx.stop_outbound()` | Halt propagation |
| `ctx.close()` | Request session close after this event |

### 3.4 PipelineExecutor (`executor.rs`)

Runs pipeline events for a session. Owns `HandlerContext` creation and byte→message decoding — the single seam where synchronous vs async dispatch will diverge in Phase 2.

| Method | What it does |
|--------|-------------|
| `fire_connect(&session)` | Creates `HandlerContext`, calls `pipeline.fire_connect(&ctx)` |
| `fire_read(&session, &dyn Any)` | If message is `Vec<u8>`: calls `pipeline.decode()`, dispatches each decoded message through handlers (with re-dispatch drain). Otherwise dispatches as-is. Returns `ctx.close_requested`. |
| `fire_disconnect(&session)` | Creates `HandlerContext`, calls `pipeline.fire_disconnect(&ctx)` |
| `fire_error(&session, &io::Error)` | Creates `HandlerContext`, calls `pipeline.fire_error(&ctx, err)`, returns `ctx.close_requested` |

---

## 4. Connection Lifecycle

```
CLIENT          BOSS                    WORKER                  PIPELINE/HANDLERS
  │              │                        │                        │
  ├─ SYN ──────→│                        │                        │
  │              ├─ accept()              │                        │
  │              ├─ try_assign(conn) ────→│ (mpsc channel)         │
  │              ├─ notify() ────────────→│ (wakes poll)           │
  │              │                        │                        │
  │              │                        ├─ handle_connections()  │
  │              │                        ├─ Pipeline::new()       │
  │              │                        ├─ init(&mut pipeline) ─→│ add handlers
  │              │                        ├─ Session::new()        │
  │              │                        ├─ poll.register(FD)     │
  │              │                        ├─ fire_connect() ────→│ fire_connect
  │              │                        │                        │  → on_connect(ctx)
  │              │                        ├─ insert(connections)   │
  │              │                        │                        │
  ├─ data ──────→│                        │                        │
  │              │                        ├─ poll wakes            │
  │              │                        ├─ read_from_fd()        │
  │              │                        ├─ fire_read(msg) ────→│ fire_read (single pass)
  │              │                        │                        │  → on_read(ctx, msg)
  │              │                        │                        │    ├─ ctx.session().write(resp)
  │              │                        │                        │    └─ ctx.close()
  │              │                        ├─ ensure_writable()     │
  │              │                        │                        │
  │              │                        ├─ poll wakes (WRITABLE) │
  │              │                        ├─ flush_to_fd()         │
  │  ←─ data ───┤                        │                        │
  │              │                        │                        │
  ├─ FIN ──────→│                        │                        │
  │              │                        ├─ poll wakes (EOF)      │
  │              │                        ├─ read_from_fd → 0      │
  │              │                        ├─ terminate()          │
  │              │                        │   ├─ flush_to_fd()     │
  │              │                        │   ├─ shutdown(Write)   │ send FIN
  │              │                        │   ├─ fire_disconn()─→│ fire_disconnect → on_disconnect
  │              │                        │   └─ drop session      │ close(fd)
```

---

## 5. Read Data Flow

```
process_readable(token, read_buf)
  ├─ session = connections.remove(&token)
  ├─ session.read_from_fd(read_buf)
  │   └─ stream.read(buf) → Ok(0)=EOF, Ok(n)=data, Err=error
  │
  ├─ Ok(0) → terminate(session)
  │
  ├─ Ok(n) → msg = Box::new(read_buf[..n].to_vec()) as Box<dyn Any>
  │          close = executor.fire_read(&session, &*msg)
  │          ├─ true → terminate(session)
  │          └─ false → ensure_writable → insert(token, session)
  │
  ├─ Err(WouldBlock) → ensure_writable → insert(token, session)
  │
  └─ Err(e) → close = executor.fire_error(&session, &e)
              ├─ true → terminate(session)
              └─ false → insert(token, session)
```

---

## 6. Write Data Flow

**Handler writes data:**

```
ctx.session().write(data)
  └─ Session::write(data)
       ├─ pipeline.try_borrow_mut()
       │   └─ fire_write(ctx, data)     ← outbound handlers (reverse order)
       └─ write_buf.extend(data)
```

**Worker flushes to socket:**

```
process_writable(token)
  ├─ session = connections.remove(&token)
  ├─ session.flush_to_fd()
  │   └─ loop: stream.write(buf front) → Ok(n) or WouldBlock
  ├─ deregister_writable(&mut session, token)
  └─ insert(token, session)
```

---

## 7. Cross-Thread Communication

| Channel | Producer | Consumer | Purpose |
|---------|----------|----------|---------|
| `mpsc::SyncSender<AcceptedConnection>` (bounded 256) | Boss | Worker | Carries TcpStream FDs |
| `pipe::Sender` / `pipe::Receiver` | Boss | Worker | 1-byte wake-up signal (new connection) |
| `pipe::Sender` / `pipe::Receiver` | ctrlc handler | Boss | 1-byte wake-up signal (shutdown) |

The pipe bridges `epoll_wait` and the mpsc channel — same pattern as Go's `netpollBreak` eventfd. The boss also has its own shutdown pipe so the ctrlc handler can interrupt `epoll_wait` immediately (no polling).

---

## 8. Token Allocation

| Token | Owner | Purpose |
|-------|-------|---------|
| `Token(0)` | Boss | Listener socket |
| `Token(1)` | Boss | Shutdown pipe (`SHUTDOWN_TOKEN`) |
| `Token(1..)` | Worker N | Session FDs (per-poll — no clash with boss Token(1)) |
| `Token(usize::MAX)` | Worker N | Wake-up pipe (`WAKE_TOKEN`) |

---

## 9. Interior Mutability

| Field | Container | Reason |
|-------|-----------|--------|
| `Pipeline` in Session | `RefCell<Pipeline>` | `ctx.session()` shared access + `&mut Pipeline` dispatch |
| `write_buf` in Session | `RefCell<VecDeque<u8>>` | Handlers write through `&Session` |
| `writable_registered` in Session | `Cell<bool>` | Toggled from both `&Session` and `&mut Session` |

All single-threaded — no `Mutex` needed.

---

## 10. Dependencies

| Crate | Version | Features | Purpose |
|-------|---------|----------|---------|
| `mio` | 1 | `net`, `os-poll`, `os-ext` | epoll/kqueue + Unix pipes |
| `log` | 0.4 | — | Logging facade |
| `ctrlc` | 3 | — | Cross-platform Ctrl+C handling |
