# flux

A Netty-inspired, Event-driven TCP networking framework for Rust.

## Purpose

Flux is an event driven networking framework in rust (inspired by netty in java). A boss thread accepts connections and hands them off to a pool of worker threads. Each worker runs its own `epoll` loop and handles its assigned connections for their entire lifetime — no work stealing, no task migration. The pipeline model gives you composable, type-safe handler chains to define custom pipeline to be run for the accepted connections. 

## How it works

```
Boss thread                    Worker threads (one per CPU core)
────────────────               ──────────────────────────────────
accept() new connections  →    each connection pinned to one worker
                               one mio::Poll instance per worker
                               pipeline dispatches typed messages
```

When a connection comes in, the boss assigns it to a worker via a bounded channel and wakes the worker using a Unix pipe. The worker registers the socket with its own `epoll` instance and owns it until it closes. No locks, no shared mutable state — each worker is an island.

## Pipelines and handlers

You define behaviour by building a pipeline of handlers per connection. The framework calls your handlers as events arrive.

```rust
use flux::{Server, ServerConfig};
use flux::handler::{Pipeline, InboundHandler, HandlerContext};
use std::any::Any;
use std::sync::Arc;

struct EchoHandler;

impl InboundHandler for EchoHandler {
    fn on_read(&mut self, ctx: &mut HandlerContext<'_>, msg: &dyn Any) {
        let bytes = msg.downcast_ref::<Vec<u8>>().unwrap();
        ctx.session().write(bytes);
    }
}

fn main() {
    let server = Server::new(ServerConfig {
        port: 9001,
        worker_threads: 4,
        pipeline_initializer: Some(Arc::new(|pipeline: &mut Pipeline| {
            pipeline.add_inbound(Box::new(EchoHandler));
        })),
    });
    server.run(); // blocks until Ctrl+C
}
```

## Decoding structured messages

Raw TCP is a byte stream — it has no concept of message boundaries. You plug in a `ByteToMessageDecoder` to turn bytes into typed messages. Once a decoder is set, your handlers never see raw bytes; they only see the domain types your decoder produces.

```rust
use flux::handler::{ByteToMessageDecoder, Pipeline};
use std::any::{Any, TypeId};

struct MyRequest { version: String, body: String }

struct MyDecoder;

impl ByteToMessageDecoder for MyDecoder {
    fn decode(&mut self, buf: &[u8]) -> Option<Box<dyn Any>> {
        // parse buf, return Some(Box::new(MyRequest { ... })) when complete
        // return None if you need more bytes
        todo!()
    }
}

// In your pipeline initializer:
// pipeline.set_decoder(Box::new(MyDecoder));
// pipeline.add_inbound(Box::new(MyHandler)); // receives MyRequest, never raw bytes
```

Handlers declare which types they care about via `accepted_types()`. A handler that returns an empty list sees everything; one that returns `vec![TypeId::of::<MyRequest>()]` only fires for that type.

## Running the demo

The repo ships with two demo binaries.

**XML wire-protocol demo** (main binary):
```
cargo run
echo '<request version="1.0">hello</request>' | nc localhost 9001
```

**Type-dispatch demo** (shows login/chat routing):
```
cargo run --bin typed_demo
echo "login:alice:s3cret" | nc localhost 9002
echo "chat:lobby:hello everyone" | nc localhost 9002
```

## Running the tests

```
cargo test
```

The integration tests cover the full pipeline path: decoder framing (including TCP fragmentation), type-based dispatch, lifecycle events (`on_connect`, `on_disconnect`), `ctx.close()`, large payloads (validates the ET epoll read loop), and multi-worker echo.

## Design constraints

- One OS thread per worker, one `epoll` fd per thread
- Connections are pinned — never migrated between workers
- All pipeline dispatch is single-threaded per connection — no `Mutex` needed inside handlers

## Roadmap

flux is Phase 1 of a longer arc. The current model (dedicated thread per worker, connection pinned for life) is a solid foundation, but there's a lot of room to grow it into a production-grade framework.


**Async pipeline dispatch** — let handlers `await` without blocking the worker's event loop. This means `PipelineExecutor` grows a future-based dispatch path alongside the current synchronous one, letting you opt in per pipeline.

**Outbound pipeline transformations** — today outbound handlers can observe writes but not transform them. A proper outbound chain (framing, compression, TLS) needs each handler to produce new bytes for the next. This requires an output-buffer threading design through `fire_write`.

**TLS support** — first-class TLS via `rustls`, plugged in as an outbound/inbound handler pair so the rest of the pipeline stays protocol-agnostic.

**Graceful backpressure** — when a worker's accept channel is full, connections are currently dropped. A proper backpressure signal back to the boss would make the framework more resilient under load.

**Metrics and observability** — per-worker connection counts, read/write throughput, and pipeline event latencies exposed via a standard interface.
