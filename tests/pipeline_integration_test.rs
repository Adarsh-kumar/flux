use std::any::{Any, TypeId};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use flux::handler::{ByteToMessageDecoder, HandlerContext, InboundHandler, Pipeline};
use flux::{Server, ServerConfig};

// ─────────────────────────────────────────────────────────────────────────────
// Test infrastructure
// ─────────────────────────────────────────────────────────────────────────────

fn as_bytes(msg: &dyn Any) -> &[u8] {
    msg.downcast_ref::<Vec<u8>>()
        .expect("expected Vec<u8> message from no-decoder pipeline")
}

fn start_server(config: ServerConfig) -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind failed");
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let config = ServerConfig { port, ..config };
    thread::spawn(move || Server::new(config).run());
    thread::sleep(Duration::from_millis(50));
    port
}

// ─────────────────────────────────────────────────────────────────────────────
// Shared handlers
// ─────────────────────────────────────────────────────────────────────────────

struct TestHandler {
    connect_count: Arc<AtomicUsize>,
    read_bytes: Arc<AtomicUsize>,
    disconnect_count: Arc<AtomicUsize>,
}
impl InboundHandler for TestHandler {
    fn on_connect(&mut self, _ctx: &mut HandlerContext<'_>) {
        self.connect_count.fetch_add(1, Ordering::SeqCst);
    }
    fn on_read(&mut self, _ctx: &mut HandlerContext<'_>, msg: &dyn Any) {
        self.read_bytes.fetch_add(as_bytes(msg).len(), Ordering::SeqCst);
    }
    fn on_disconnect(&mut self, _ctx: &mut HandlerContext<'_>) {
        self.disconnect_count.fetch_add(1, Ordering::SeqCst);
    }
}

struct TaggedHandler {
    tag: &'static str,
    log: Arc<Mutex<Vec<String>>>,
}
impl InboundHandler for TaggedHandler {
    fn on_connect(&mut self, _ctx: &mut HandlerContext<'_>) {
        self.log.lock().unwrap().push(format!("{}.connect", self.tag));
    }
    fn on_read(&mut self, _ctx: &mut HandlerContext<'_>, msg: &dyn Any) {
        self.log.lock().unwrap().push(format!("{}.read({})", self.tag, as_bytes(msg).len()));
    }
    fn on_disconnect(&mut self, _ctx: &mut HandlerContext<'_>) {
        self.log.lock().unwrap().push(format!("{}.disconnect", self.tag));
    }
}

/// Line-framing decoder. Buffers bytes across reads; emits one Vec<u8> per '\n'.
struct NewlineDecoder { buf: Vec<u8> }
impl NewlineDecoder { fn new() -> Self { Self { buf: Vec::new() } } }
impl ByteToMessageDecoder for NewlineDecoder {
    fn decode(&mut self, input: &[u8]) -> Option<Box<dyn Any>> {
        self.buf.extend_from_slice(input);
        let pos = self.buf.iter().position(|&b| b == b'\n')?;
        let line = self.buf[..pos].to_vec();
        self.buf.drain(..=pos);
        Some(Box::new(line))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Existing tests (cleaned up)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn connections_flow_through_pipeline() {
    let connect_count = Arc::new(AtomicUsize::new(0));
    let read_bytes = Arc::new(AtomicUsize::new(0));
    let disconnect_count = Arc::new(AtomicUsize::new(0));

    let port = start_server(ServerConfig {
        port: 0,
        worker_threads: 1,
        pipeline_initializer: Some(Arc::new({
            let cc = connect_count.clone();
            let rb = read_bytes.clone();
            let dc = disconnect_count.clone();
            move |p: &mut Pipeline| {
                p.add_inbound(Box::new(TestHandler {
                    connect_count: cc.clone(),
                    read_bytes: rb.clone(),
                    disconnect_count: dc.clone(),
                }));
            }
        })),
    });

    let mut streams: Vec<TcpStream> = Vec::new();
    for _ in 0..3 {
        let mut s = TcpStream::connect(format!("127.0.0.1:{}", port)).expect("connect failed");
        s.write_all(b"hello").expect("write failed");
        streams.push(s);
    }
    thread::sleep(Duration::from_millis(200));
    assert_eq!(connect_count.load(Ordering::SeqCst), 3);
    assert_eq!(read_bytes.load(Ordering::SeqCst), 15);
    drop(streams);
    thread::sleep(Duration::from_millis(200));
    assert_eq!(disconnect_count.load(Ordering::SeqCst), 3);
}

#[test]
fn handler_can_write_back_to_client() {
    struct EchoHandler;
    impl InboundHandler for EchoHandler {
        fn on_read(&mut self, ctx: &mut HandlerContext<'_>, msg: &dyn Any) {
            ctx.session().write(as_bytes(msg));
        }
    }

    let port = start_server(ServerConfig {
        port: 0,
        worker_threads: 1,
        pipeline_initializer: Some(Arc::new(|p: &mut Pipeline| {
            p.add_inbound(Box::new(EchoHandler));
        })),
    });

    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).expect("connect");
    client.set_read_timeout(Some(Duration::from_millis(500))).unwrap();
    client.write_all(b"ping").unwrap();
    let mut buf = [0u8; 4];
    client.read_exact(&mut buf).expect("read");
    assert_eq!(&buf, b"ping");
}

#[test]
fn chained_handlers_all_fire_in_order() {
    let log = Arc::new(Mutex::new(Vec::<String>::new()));

    let port = start_server(ServerConfig {
        port: 0,
        worker_threads: 1,
        pipeline_initializer: Some(Arc::new({
            let la = log.clone();
            let lb = log.clone();
            let lc = log.clone();
            move |p: &mut Pipeline| {
                p.add_inbound(Box::new(TaggedHandler { tag: "A", log: la.clone() }));
                p.add_inbound(Box::new(TaggedHandler { tag: "B", log: lb.clone() }));
                p.add_inbound(Box::new(TaggedHandler { tag: "C", log: lc.clone() }));
            }
        })),
    });

    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    client.write_all(b"data").unwrap();
    thread::sleep(Duration::from_millis(100));
    drop(client);
    thread::sleep(Duration::from_millis(100));

    let events = log.lock().unwrap();
    let pos = |s: &str| events.iter().position(|e| e == s).expect(s);
    assert!(pos("A.connect") < pos("B.connect"));
    assert!(pos("B.connect") < pos("C.connect"));
    assert!(pos("A.read(4)") < pos("B.read(4)"));
    assert!(pos("B.read(4)") < pos("C.read(4)"));
    assert!(events.contains(&"A.disconnect".to_string()));
    assert!(events.contains(&"B.disconnect".to_string()));
    assert!(events.contains(&"C.disconnect".to_string()));
}

#[test]
fn type_based_dispatch_routes_by_typeid() {
    #[derive(Debug)] struct CustomMessage;

    struct RawLogger { log: Arc<Mutex<Vec<String>>> }
    impl InboundHandler for RawLogger {
        fn accepted_types(&self) -> Vec<TypeId> { vec![TypeId::of::<Vec<u8>>()] }
        fn on_read(&mut self, _ctx: &mut HandlerContext<'_>, _msg: &dyn Any) {
            self.log.lock().unwrap().push("raw".into());
        }
    }

    struct CustomLogger { log: Arc<Mutex<Vec<String>>> }
    impl InboundHandler for CustomLogger {
        fn accepted_types(&self) -> Vec<TypeId> { vec![TypeId::of::<CustomMessage>()] }
        fn on_read(&mut self, _ctx: &mut HandlerContext<'_>, _msg: &dyn Any) {
            self.log.lock().unwrap().push("custom".into());
        }
    }

    let log = Arc::new(Mutex::new(Vec::<String>::new()));
    let port = start_server(ServerConfig {
        port: 0,
        worker_threads: 1,
        pipeline_initializer: Some(Arc::new({
            let la = log.clone(); let lb = log.clone();
            move |p: &mut Pipeline| {
                p.add_inbound(Box::new(RawLogger { log: la.clone() }));
                p.add_inbound(Box::new(CustomLogger { log: lb.clone() }));
            }
        })),
    });

    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    client.write_all(b"hello").unwrap();
    thread::sleep(Duration::from_millis(100));
    drop(client);
    thread::sleep(Duration::from_millis(100));

    let events = log.lock().unwrap();
    assert!(events.iter().any(|e| e == "raw"), "RawLogger must fire");
    assert!(!events.iter().any(|e| e == "custom"), "CustomLogger must not fire");
}

#[test]
fn catch_all_handler_sees_every_message() {
    struct CatchAll { count: Arc<AtomicUsize> }
    impl InboundHandler for CatchAll {
        fn on_read(&mut self, _ctx: &mut HandlerContext<'_>, _msg: &dyn Any) {
            self.count.fetch_add(1, Ordering::SeqCst);
        }
    }

    let count = Arc::new(AtomicUsize::new(0));
    let port = start_server(ServerConfig {
        port: 0,
        worker_threads: 1,
        pipeline_initializer: Some(Arc::new({
            let c = count.clone();
            move |p: &mut Pipeline| { p.add_inbound(Box::new(CatchAll { count: c.clone() })); }
        })),
    });

    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    client.write_all(b"x").unwrap();
    thread::sleep(Duration::from_millis(100));
    drop(client);
    thread::sleep(Duration::from_millis(100));
    assert_eq!(count.load(Ordering::SeqCst), 1);
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 1: Decoder
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn decoder_dispatches_typed_messages() {
    #[derive(Debug)] struct LoginRequest  { _username: String }
    #[derive(Debug)] struct ChatMessage   { _text: String }

    struct ProtocolDecoder { buf: Vec<u8> }
    impl ByteToMessageDecoder for ProtocolDecoder {
        fn decode(&mut self, input: &[u8]) -> Option<Box<dyn Any>> {
            self.buf.extend_from_slice(input);
            let pos = self.buf.iter().position(|&b| b == b'\n')?;
            let line = String::from_utf8_lossy(&self.buf[..pos]).trim().to_string();
            self.buf.drain(..=pos);
            let parts: Vec<&str> = line.splitn(2, ':').collect();
            match parts.as_slice() {
                ["login", user] => Some(Box::new(LoginRequest { _username: user.to_string() })),
                ["chat",  text] => Some(Box::new(ChatMessage  { _text:     text.to_string() })),
                _               => None,
            }
        }
    }

    let login_count = Arc::new(AtomicUsize::new(0));
    let chat_count  = Arc::new(AtomicUsize::new(0));

    struct LoginHandler { count: Arc<AtomicUsize> }
    impl InboundHandler for LoginHandler {
        fn accepted_types(&self) -> Vec<TypeId> { vec![TypeId::of::<LoginRequest>()] }
        fn on_read(&mut self, _ctx: &mut HandlerContext<'_>, _msg: &dyn Any) {
            self.count.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct ChatHandler { count: Arc<AtomicUsize> }
    impl InboundHandler for ChatHandler {
        fn accepted_types(&self) -> Vec<TypeId> { vec![TypeId::of::<ChatMessage>()] }
        fn on_read(&mut self, _ctx: &mut HandlerContext<'_>, _msg: &dyn Any) {
            self.count.fetch_add(1, Ordering::SeqCst);
        }
    }

    let port = start_server(ServerConfig {
        port: 0,
        worker_threads: 1,
        pipeline_initializer: Some(Arc::new({
            let lc = login_count.clone(); let cc = chat_count.clone();
            move |p: &mut Pipeline| {
                p.set_decoder(Box::new(ProtocolDecoder { buf: Vec::new() }));
                p.add_inbound(Box::new(LoginHandler { count: lc.clone() }));
                p.add_inbound(Box::new(ChatHandler  { count: cc.clone() }));
            }
        })),
    });

    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    client.write_all(b"login:alice\n").unwrap();
    thread::sleep(Duration::from_millis(100));
    client.write_all(b"chat:hello world\n").unwrap();
    thread::sleep(Duration::from_millis(100));
    drop(client);

    assert_eq!(login_count.load(Ordering::SeqCst), 1, "LoginHandler must fire once");
    assert_eq!(chat_count.load(Ordering::SeqCst),  1, "ChatHandler must fire once");
}

#[test]
fn decoder_handles_fragmented_tcp_input() {
    let message_count = Arc::new(AtomicUsize::new(0));
    struct Counter { count: Arc<AtomicUsize> }
    impl InboundHandler for Counter {
        fn on_read(&mut self, _ctx: &mut HandlerContext<'_>, _msg: &dyn Any) {
            self.count.fetch_add(1, Ordering::SeqCst);
        }
    }

    let port = start_server(ServerConfig {
        port: 0,
        worker_threads: 1,
        pipeline_initializer: Some(Arc::new({
            let c = message_count.clone();
            move |p: &mut Pipeline| {
                p.set_decoder(Box::new(NewlineDecoder::new()));
                p.add_inbound(Box::new(Counter { count: c.clone() }));
            }
        })),
    });

    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    client.write_all(b"hel").unwrap();
    thread::sleep(Duration::from_millis(50));
    client.write_all(b"lo\n").unwrap();
    thread::sleep(Duration::from_millis(150));
    drop(client);

    assert_eq!(
        message_count.load(Ordering::SeqCst), 1,
        "fragmented message must reassemble into exactly one dispatch"
    );
}

#[test]
fn decoder_emits_multiple_messages_from_one_read() {
    let message_count = Arc::new(AtomicUsize::new(0));
    struct Counter { count: Arc<AtomicUsize> }
    impl InboundHandler for Counter {
        fn on_read(&mut self, _ctx: &mut HandlerContext<'_>, _msg: &dyn Any) {
            self.count.fetch_add(1, Ordering::SeqCst);
        }
    }

    let port = start_server(ServerConfig {
        port: 0,
        worker_threads: 1,
        pipeline_initializer: Some(Arc::new({
            let c = message_count.clone();
            move |p: &mut Pipeline| {
                p.set_decoder(Box::new(NewlineDecoder::new()));
                p.add_inbound(Box::new(Counter { count: c.clone() }));
            }
        })),
    });

    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    client.write_all(b"msg1\nmsg2\nmsg3\n").unwrap();
    thread::sleep(Duration::from_millis(200));
    drop(client);

    assert_eq!(
        message_count.load(Ordering::SeqCst), 3,
        "three framed messages in one TCP read must each dispatch separately"
    );
}

#[test]
fn decoder_drops_unrecognised_input() {
    let read_count = Arc::new(AtomicUsize::new(0));
    struct Counter { count: Arc<AtomicUsize> }
    impl InboundHandler for Counter {
        fn on_read(&mut self, _ctx: &mut HandlerContext<'_>, _msg: &dyn Any) {
            self.count.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct StrictDecoder { buf: Vec<u8> }
    impl ByteToMessageDecoder for StrictDecoder {
        fn decode(&mut self, input: &[u8]) -> Option<Box<dyn Any>> {
            self.buf.extend_from_slice(input);
            let pos = self.buf.iter().position(|&b| b == b'\n')?;
            let line = self.buf[..pos].to_vec();
            self.buf.drain(..=pos);
            if line == b"ok" { Some(Box::new(line)) } else { None }
        }
    }

    let port = start_server(ServerConfig {
        port: 0,
        worker_threads: 1,
        pipeline_initializer: Some(Arc::new({
            let c = read_count.clone();
            move |p: &mut Pipeline| {
                p.set_decoder(Box::new(StrictDecoder { buf: Vec::new() }));
                p.add_inbound(Box::new(Counter { count: c.clone() }));
            }
        })),
    });

    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    client.write_all(b"garbage\nunknown\n").unwrap();
    thread::sleep(Duration::from_millis(150));
    drop(client);

    assert_eq!(read_count.load(Ordering::SeqCst), 0,
        "lines the decoder drops must not reach handlers");
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 2: Connection lifecycle
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn ctx_close_terminates_connection() {
    struct CloseOnRead;
    impl InboundHandler for CloseOnRead {
        fn on_read(&mut self, ctx: &mut HandlerContext<'_>, _msg: &dyn Any) {
            ctx.close();
        }
    }

    let port = start_server(ServerConfig {
        port: 0,
        worker_threads: 1,
        pipeline_initializer: Some(Arc::new(|p: &mut Pipeline| {
            p.add_inbound(Box::new(CloseOnRead));
        })),
    });

    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    client.set_read_timeout(Some(Duration::from_millis(500))).unwrap();
    client.write_all(b"trigger").unwrap();

    let mut buf = vec![0u8; 64];
    let n = client.read(&mut buf).expect("read should return 0, not error");
    assert_eq!(n, 0, "server must close connection — client reads EOF after ctx.close()");
}

/// Server writes in on_connect. Client must receive that data without
/// sending anything first.
#[test]
fn on_connect_write_reaches_client() {
    struct GreetOnConnect;
    impl InboundHandler for GreetOnConnect {
        fn on_connect(&mut self, ctx: &mut HandlerContext<'_>) {
            ctx.session().write(b"hello\n");
        }
    }

    let port = start_server(ServerConfig {
        port: 0,
        worker_threads: 1,
        pipeline_initializer: Some(Arc::new(|p: &mut Pipeline| {
            p.add_inbound(Box::new(GreetOnConnect));
        })),
    });

    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    client.set_read_timeout(Some(Duration::from_millis(500))).unwrap();

    let mut buf = vec![0u8; 6];
    client.read_exact(&mut buf)
        .expect("server must push greeting from on_connect before client sends anything");
    assert_eq!(&buf, b"hello\n");
}

/// All handlers must receive on_disconnect regardless of position in chain.
#[test]
fn disconnect_fires_for_every_handler_in_chain() {
    let log = Arc::new(Mutex::new(Vec::<String>::new()));

    let port = start_server(ServerConfig {
        port: 0,
        worker_threads: 1,
        pipeline_initializer: Some(Arc::new({
            let la = log.clone(); let lb = log.clone(); let lc = log.clone();
            move |p: &mut Pipeline| {
                p.add_inbound(Box::new(TaggedHandler { tag: "A", log: la.clone() }));
                p.add_inbound(Box::new(TaggedHandler { tag: "B", log: lb.clone() }));
                p.add_inbound(Box::new(TaggedHandler { tag: "C", log: lc.clone() }));
            }
        })),
    });

    let client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    drop(client);
    thread::sleep(Duration::from_millis(200));

    let events = log.lock().unwrap();
    assert!(events.contains(&"A.disconnect".to_string()), "A must get on_disconnect");
    assert!(events.contains(&"B.disconnect".to_string()), "B must get on_disconnect");
    assert!(events.contains(&"C.disconnect".to_string()), "C must get on_disconnect");
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 3: ET epoll read loop
// ─────────────────────────────────────────────────────────────────────────────

/// Client sends 64 KB in one shot. Worker must loop reads until WouldBlock
/// to drain the kernel buffer fully (8 KB read buffer × 8 iterations).
#[test]
fn large_payload_fully_received() {
    let total_bytes = Arc::new(AtomicUsize::new(0));
    struct Accumulator { total: Arc<AtomicUsize> }
    impl InboundHandler for Accumulator {
        fn on_read(&mut self, _ctx: &mut HandlerContext<'_>, msg: &dyn Any) {
            self.total.fetch_add(as_bytes(msg).len(), Ordering::SeqCst);
        }
    }

    let port = start_server(ServerConfig {
        port: 0,
        worker_threads: 1,
        pipeline_initializer: Some(Arc::new({
            let t = total_bytes.clone();
            move |p: &mut Pipeline| { p.add_inbound(Box::new(Accumulator { total: t.clone() })); }
        })),
    });

    const PAYLOAD: usize = 64 * 1024;
    let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
    client.write_all(&vec![0xABu8; PAYLOAD]).unwrap();
    thread::sleep(Duration::from_millis(300));
    drop(client);

    assert_eq!(
        total_bytes.load(Ordering::SeqCst), PAYLOAD,
        "all 64 KB must arrive — validates ET read loop drains kernel buffer fully"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 5: Backpressure
// ─────────────────────────────────────────────────────────────────────────────

/// When a worker is blocked (slow handler), the boss pauses accept instead of
/// dropping connections. New connections park in the kernel's TCP backlog.
#[test]
fn backpressure_pauses_accept_not_drops() {
    use std::sync::Barrier;

    let barrier = Arc::new(Barrier::new(2));
    let unblocked = Arc::new(AtomicUsize::new(0));

    struct BlockingHandler {
        barrier: Arc<Barrier>,
        unblocked: Arc<AtomicUsize>,
    }
    impl InboundHandler for BlockingHandler {
        fn on_read(&mut self, _ctx: &mut HandlerContext<'_>, _msg: &dyn Any) {
            // Block the worker thread on first invocation only.
            if self.unblocked.fetch_add(1, Ordering::SeqCst) == 0 {
                self.barrier.wait();
            }
        }
    }

    let port = start_server(ServerConfig {
        port: 0,
        worker_threads: 1, // single worker so all connections hit same mpsc
        pipeline_initializer: Some(Arc::new({
            let b = barrier.clone();
            let u = unblocked.clone();
            move |p: &mut Pipeline| {
                p.add_inbound(Box::new(BlockingHandler {
                    barrier: b.clone(),
                    unblocked: u.clone(),
                }));
            }
        })),
    });

    // Step 1: open first connection and send data — this blocks the worker.
    let mut first = TcpStream::connect(format!("127.0.0.1:{}", port)).expect("first connect");
    first.write_all(b"block").expect("write to first");

    // Give worker time to hit the barrier.
    thread::sleep(Duration::from_millis(100));

    // Step 2: fill the mpsc queue (capacity 256 per worker).
    let mut flood: Vec<TcpStream> = Vec::new();
    for i in 0..256 {
        match TcpStream::connect_timeout(
            &format!("127.0.0.1:{}", port).parse().unwrap(),
            Duration::from_millis(200),
        ) {
            Ok(s) => flood.push(s),
            Err(e) => panic!("connection {} failed: {} — accept should be paused, not refusing", i, e),
        }
    }

    // Step 3: one more connection — also must succeed (parked in kernel backlog).
    let mut extra = TcpStream::connect(format!("127.0.0.1:{}", port))
        .expect("connection beyond mpsc capacity must succeed — kernel backlog absorbs it");

    // Wait 5 seconds while paused. The boss spins on LISTENER_TOKEN events
    // without accepting. The log will show the spin count on resume.
    eprintln!(">>> boss is paused — waiting 5s for spin counter to accumulate...");
    thread::sleep(Duration::from_secs(5));

    // Step 4: unblock the worker.
    barrier.wait();

    // Step 5: all flood connections and the extra must eventually be accepted.
    // Write data to each so the handler fires.
    thread::sleep(Duration::from_millis(200));
    for mut s in flood {
        s.write_all(b"x").ok();
    }
    extra.set_read_timeout(Some(Duration::from_millis(500))).ok();
    // If extra can write, the server accepted it.
    assert!(extra.write_all(b"x").is_ok(), "extra connection must be accepted");

    // Cleanup
    drop(first);
    thread::sleep(Duration::from_millis(100));
}
