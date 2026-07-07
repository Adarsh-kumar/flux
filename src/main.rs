//! flux demo — XML wire protocol with ByteToMessageDecoder.
//!
//! Pipeline: XmlDecoder → VersionValidator → ResponseHandler
//!
//! XmlDecoder (ByteToMessageDecoder) parses raw bytes into XmlRequest.
//! VersionValidator checks if the version is supported.
//! ResponseHandler returns 200 or an error message.
//!
//! Run with `cargo run`, then test with:
//!   echo '<request version="1.0">hello</request>' | nc localhost 9001
//!   echo '<request version="2.0">hello</request>' | nc localhost 9001
//! Press Ctrl-C to stop.

use std::any::{Any, TypeId};
use std::sync::Arc;

use flux::handler::{ByteToMessageDecoder, HandlerContext, InboundHandler, Pipeline};
use flux::{Server, ServerConfig};

// ── Message type ──────────────────────────────────────────

/// Parsed XML request from the wire.
#[derive(Debug)]
struct XmlRequest {
    version: String,
    body: String,
}

// ── Decoder: XmlDecoder ───────────────────────────────────

/// Parses raw bytes like `<request version="1.0">body</request>`
/// into an `XmlRequest`. Caches incomplete data across reads.
struct XmlDecoder {
    buf: Vec<u8>,
}

impl ByteToMessageDecoder for XmlDecoder {
    fn decode(&mut self, new_bytes: &[u8]) -> Option<Box<dyn Any>> {
        self.buf.extend_from_slice(new_bytes);
        let s = String::from_utf8_lossy(&self.buf);

        // Look for a complete <request ...>...</request> frame.
        let end = s.find("</request>")?;
        let frame = &s[..end + "</request>".len()];

        // Extract version attribute: version="X.Y"
        let version = frame
            .split("version=\"")
            .nth(1)?
            .split('"')
            .next()?
            .to_string();

        // Extract body between > and </request>
        let body_start = frame.find('>')? + 1;
        let body_end = frame.rfind("</request>")?;
        let body = frame[body_start..body_end].trim().to_string();

        // Remove the consumed frame from the buffer.
        let consumed = frame.len();
        self.buf.drain(..consumed);

        Some(Box::new(XmlRequest { version, body }))
    }
}

// ── Handler A: VersionValidator ───────────────────────────

/// Accepts only XmlRequest. Checks if the version is supported.
struct VersionValidator {
    supported: Vec<String>,
}

impl InboundHandler for VersionValidator {
    fn accepted_types(&self) -> Vec<TypeId> {
        vec![TypeId::of::<XmlRequest>()]
    }

    fn on_read(&mut self, ctx: &mut HandlerContext<'_>, msg: &dyn Any) {
        let req = msg.downcast_ref::<XmlRequest>().unwrap();
        if self.supported.contains(&req.version) {
            println!(
                "[Validator] version {} supported — request body: {}",
                req.version, req.body
            );
            ctx.session().write(b"200 OK\n");
        } else {
            println!(
                "[Validator] version {} NOT supported (supported: {:?})",
                req.version, self.supported
            );
            ctx.session()
                .write(format!("400 Unsupported version: {}\n", req.version).as_bytes());
        }
    }

    fn on_disconnect(&mut self, ctx: &mut HandlerContext<'_>) {
        println!("[Validator] session {} disconnected", ctx.session().session_id());
    }
}

// ── Main ──────────────────────────────────────────────────

fn main() {
    let config = ServerConfig {
        port: 9001,
        worker_threads: num_cpus::get(),
        pipeline_initializer: Some(Arc::new(|p: &mut Pipeline| {
            p.set_decoder(Box::new(XmlDecoder { buf: Vec::new() }));
            p.add_inbound(Box::new(VersionValidator {
                supported: vec!["1.0".into(), "1.1".into()],
            }));
        })),
    };

    println!("flux XML wire-protocol demo on port 9001");
    println!("Pipeline: XmlDecoder → VersionValidator → ResponseHandler\n");
    println!("Test commands:");
    println!("  echo '<request version=\"1.0\">hello</request>' | nc localhost 9001");
    println!("  echo '<request version=\"2.0\">hello</request>' | nc localhost 9001");
    println!("\nSupported versions: 1.0, 1.1");
    println!("Press Ctrl-C to stop.\n");

    Server::new(config).run();
}

mod num_cpus {
    pub fn get() -> usize {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
    }
}
