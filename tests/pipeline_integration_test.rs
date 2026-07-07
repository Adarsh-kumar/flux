use std::any::Any;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use flux::handler::{HandlerContext, InboundHandler, Pipeline};
use flux::{Server, ServerConfig};

// ── Helper: extract raw bytes from an Any message ──────────

fn as_bytes(msg: &dyn Any) -> &[u8] {
    msg.downcast_ref::<Vec<u8>>()
        .expect("expected Vec<u8> message")
}

// ── TestHandler ────────────────────────────────────────────

/// Handler that records counts via shared atomics.
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
        self.read_bytes
            .fetch_add(as_bytes(msg).len(), Ordering::SeqCst);
    }

    fn on_disconnect(&mut self, _ctx: &mut HandlerContext<'_>) {
        self.disconnect_count.fetch_add(1, Ordering::SeqCst);
    }
}

// ── TaggedHandler ──────────────────────────────────────────

/// Handler that pushes a tag string into a shared Vec when events fire.
struct TaggedHandler {
    tag: &'static str,
    log: Arc<Mutex<Vec<String>>>,
    stop_after: bool,
}

impl InboundHandler for TaggedHandler {
    fn on_connect(&mut self, ctx: &mut HandlerContext<'_>) {
        self.log
            .lock()
            .unwrap()
            .push(format!("{}.connect", self.tag));
        if self.stop_after {
            ctx.stop_inbound();
        }
    }

    fn on_read(&mut self, ctx: &mut HandlerContext<'_>, msg: &dyn Any) {
        self.log.lock().unwrap().push(format!(
            "{}.read({})",
            self.tag,
            as_bytes(msg).len()
        ));
        if self.stop_after {
            ctx.stop_inbound();
        }
    }

    fn on_disconnect(&mut self, ctx: &mut HandlerContext<'_>) {
        self.log
            .lock()
            .unwrap()
            .push(format!("{}.disconnect", self.tag));
        if self.stop_after {
            ctx.stop_inbound();
        }
    }
}

// ── start_server helper ────────────────────────────────────

fn start_server(config: ServerConfig) -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("failed to bind");
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let config = ServerConfig { port, ..config };
    thread::spawn(move || {
        Server::new(config).run();
    });

    port
}

// ── Tests ──────────────────────────────────────────────────

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

    thread::sleep(Duration::from_millis(50));

    let mut streams: Vec<TcpStream> = Vec::new();
    for _ in 0..3 {
        let mut s =
            TcpStream::connect(format!("127.0.0.1:{}", port)).expect("connect failed");
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

    thread::sleep(Duration::from_millis(50));

    let mut client =
        TcpStream::connect(format!("127.0.0.1:{}", port)).expect("connect failed");
    client.write_all(b"ping").expect("write failed");
    thread::sleep(Duration::from_millis(100));

    let mut buf = [0u8; 4];
    client.read_exact(&mut buf).expect("read failed");
    assert_eq!(&buf, b"ping");
}

#[test]
fn chained_handlers_all_fire_in_order() {
    let log = Arc::new(Mutex::new(Vec::new()));

    let port = start_server(ServerConfig {
        port: 0,
        worker_threads: 1,
        pipeline_initializer: Some(Arc::new({
            let la = log.clone();
            let lb = log.clone();
            let lc = log.clone();
            move |p: &mut Pipeline| {
                p.add_inbound(Box::new(TaggedHandler {
                    tag: "A",
                    log: la.clone(),
                    stop_after: false,
                }));
                p.add_inbound(Box::new(TaggedHandler {
                    tag: "B",
                    log: lb.clone(),
                    stop_after: false,
                }));
                p.add_inbound(Box::new(TaggedHandler {
                    tag: "C",
                    log: lc.clone(),
                    stop_after: false,
                }));
            }
        })),
    });

    thread::sleep(Duration::from_millis(50));

    let mut client =
        TcpStream::connect(format!("127.0.0.1:{}", port)).expect("connect failed");
    client.write_all(b"data").expect("write failed");
    thread::sleep(Duration::from_millis(100));
    drop(client);
    thread::sleep(Duration::from_millis(100));

    let events = log.lock().unwrap();

    assert!(events.iter().position(|e| e == "A.connect").unwrap()
        < events.iter().position(|e| e == "B.connect").unwrap());
    assert!(events.contains(&"C.connect".to_string()));
    assert!(events.contains(&"A.read(4)".to_string()));
    assert!(events.contains(&"B.read(4)".to_string()));
    assert!(events.contains(&"C.read(4)".to_string()));
    assert!(events.contains(&"A.disconnect".to_string()));
    assert!(events.contains(&"C.disconnect".to_string()));
}

#[test]
fn stop_inbound_blocks_downstream_handlers() {
    let log = Arc::new(Mutex::new(Vec::new()));

    let port = start_server(ServerConfig {
        port: 0,
        worker_threads: 1,
        pipeline_initializer: Some(Arc::new({
            let la = log.clone();
            let lb = log.clone();
            let lc = log.clone();
            move |p: &mut Pipeline| {
                p.add_inbound(Box::new(TaggedHandler {
                    tag: "A",
                    log: la.clone(),
                    stop_after: false,
                }));
                p.add_inbound(Box::new(TaggedHandler {
                    tag: "B",
                    log: lb.clone(),
                    stop_after: true, // blocks C
                }));
                p.add_inbound(Box::new(TaggedHandler {
                    tag: "C",
                    log: lc.clone(),
                    stop_after: false,
                }));
            }
        })),
    });

    thread::sleep(Duration::from_millis(50));

    let mut client =
        TcpStream::connect(format!("127.0.0.1:{}", port)).expect("connect failed");
    client.write_all(b"data").expect("write failed");
    thread::sleep(Duration::from_millis(100));
    drop(client);
    thread::sleep(Duration::from_millis(100));

    let events = log.lock().unwrap();
    assert!(events.contains(&"A.connect".to_string()));
    assert!(events.contains(&"B.connect".to_string()));
    assert!(!events.contains(&"C.connect".to_string()));
    assert!(events.contains(&"A.read(4)".to_string()));
    assert!(events.contains(&"B.read(4)".to_string()));
    assert!(!events.contains(&"C.read(4)".to_string()));
}

// ── Type-based dispatch tests ──────────────────────────────

use std::any::TypeId;

/// A custom message type (not Vec<u8>).
#[derive(Debug)]
struct CustomMessage {
    payload: String,
}

/// Handler that only accepts CustomMessage.
struct CustomHandler {
    log: Arc<Mutex<Vec<String>>>,
}

impl InboundHandler for CustomHandler {
    fn accepted_types(&self) -> Vec<TypeId> {
        vec![TypeId::of::<CustomMessage>()]
    }

    fn on_read(&mut self, _ctx: &mut HandlerContext<'_>, msg: &dyn Any) {
        let cm = msg.downcast_ref::<CustomMessage>().unwrap();
        self.log
            .lock()
            .unwrap()
            .push(format!("custom: {}", cm.payload));
    }
}

/// Handler that only accepts Vec<u8> and converts to CustomMessage.
/// In a real app this would be FrameDecoder.
struct RawToCustom {
    log: Arc<Mutex<Vec<String>>>,
}

impl InboundHandler for RawToCustom {
    fn accepted_types(&self) -> Vec<TypeId> {
        vec![TypeId::of::<Vec<u8>>()]
    }

    fn on_read(&mut self, _ctx: &mut HandlerContext<'_>, msg: &dyn Any) {
        let bytes = msg.downcast_ref::<Vec<u8>>().unwrap();
        let text = String::from_utf8_lossy(bytes);
        self.log
            .lock()
            .unwrap()
            .push(format!("raw: {}", text.trim()));
    }
}

#[test]
fn type_based_dispatch_routes_by_typeid() {
    let log = Arc::new(Mutex::new(Vec::new()));

    let port = start_server(ServerConfig {
        port: 0,
        worker_threads: 1,
        pipeline_initializer: Some(Arc::new({
            let la = log.clone();
            let lb = log.clone();
            move |p: &mut Pipeline| {
                // Handler A: only sees Vec<u8>
                p.add_inbound(Box::new(RawToCustom { log: la.clone() }));
                // Handler B: only sees CustomMessage
                p.add_inbound(Box::new(CustomHandler { log: lb.clone() }));
            }
        })),
    });

    thread::sleep(Duration::from_millis(50));

    let mut client =
        TcpStream::connect(format!("127.0.0.1:{}", port)).expect("connect failed");
    client.write_all(b"hello").expect("write failed");
    thread::sleep(Duration::from_millis(100));
    drop(client);
    thread::sleep(Duration::from_millis(100));

    let events = log.lock().unwrap();
    println!("events: {:?}", *events);

    // RawToCustom received the Vec<u8>.
    assert!(events.iter().any(|e| e == "raw: hello"));

    // CustomHandler did NOT receive anything — the message was Vec<u8>,
    // not CustomMessage.
    assert!(!events.iter().any(|e| e.starts_with("custom:")));
}

#[test]
fn catch_all_handler_sees_everything() {
    let log = Arc::new(Mutex::new(Vec::new()));

    struct CatchAll {
        log: Arc<Mutex<Vec<String>>>,
    }
    impl InboundHandler for CatchAll {
        // accepted_types() returns empty → sees everything
        fn on_read(&mut self, _ctx: &mut HandlerContext<'_>, msg: &dyn Any) {
            self.log.lock().unwrap().push(format!(
                "catch_all: {:?}",
                msg.type_id()
            ));
        }
    }

    let port = start_server(ServerConfig {
        port: 0,
        worker_threads: 1,
        pipeline_initializer: Some(Arc::new({
            let l = log.clone();
            move |p: &mut Pipeline| {
                p.add_inbound(Box::new(CatchAll { log: l.clone() }));
            }
        })),
    });

    thread::sleep(Duration::from_millis(50));

    let mut client =
        TcpStream::connect(format!("127.0.0.1:{}", port)).expect("connect failed");
    client.write_all(b"x").expect("write failed");
    thread::sleep(Duration::from_millis(100));
    drop(client);

    let events = log.lock().unwrap();
    assert_eq!(events.len(), 1);
    assert!(events[0].contains("catch_all"));
}
