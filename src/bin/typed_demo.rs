//! flux type-dispatch demo.
//!
//! Pipeline: RawParser (decoder) → CloseAfterRead → LoginHandler → ChatHandler
//!
//! RawParser implements ByteToMessageDecoder: parses lines like "login:alice:secret"
//! or "chat:room:hello" into LoginRequest / ChatMessage. Downstream handlers
//! only receive their declared types.
//!
//! Run:   cargo run --bin typed_demo
//! Test:  echo "login:alice:s3cret" | nc localhost 9002
//!        echo "chat:lobby:hello everyone" | nc localhost 9002
//!        echo "unknown:something" | nc localhost 9002

use std::any::{Any, TypeId};
use std::sync::Arc;

use flux::handler::{ByteToMessageDecoder, HandlerContext, InboundHandler, Pipeline};
use flux::{Server, ServerConfig};

// ── Message types ─────────────────────────────────────────

#[derive(Debug)]
struct LoginRequest {
    username: String,
    password: String,
}

#[derive(Debug)]
struct ChatMessage {
    room: String,
    text: String,
}

// ── Decoder: RawParser (byte → typed messages) ────────────

struct RawParser;

impl ByteToMessageDecoder for RawParser {
    fn decode(&mut self, buf: &[u8]) -> Option<Box<dyn Any>> {
        let line = String::from_utf8_lossy(buf);
        let line = line.trim();
        if line.is_empty() {
            return None;
        }
        let parts: Vec<&str> = line.splitn(3, ':').collect();
        match parts.as_slice() {
            ["login", user, pass] => {
                println!("[RawParser] → LoginRequest(user={}, pass=***)", user);
                Some(Box::new(LoginRequest {
                    username: user.to_string(),
                    password: pass.to_string(),
                }))
            }
            ["chat", room, text] => {
                println!("[RawParser] → ChatMessage(room={}, text={})", room, text);
                Some(Box::new(ChatMessage {
                    room: room.to_string(),
                    text: text.to_string(),
                }))
            }
            _ => {
                println!("[RawParser] unknown format: {}", line);
                None
            }
        }
    }
}

// ── Handler A: CloseAfterRead (closes after first message) ─

struct CloseAfterRead;

impl InboundHandler for CloseAfterRead {
    fn on_read(&mut self, ctx: &mut HandlerContext<'_>, _msg: &dyn Any) {
        ctx.close();
    }
}

// ── Handler B: LoginHandler (only accepts LoginRequest) ────

struct LoginHandler;

impl InboundHandler for LoginHandler {
    fn accepted_types(&self) -> Vec<TypeId> {
        vec![TypeId::of::<LoginRequest>()]
    }

    fn on_read(&mut self, _ctx: &mut HandlerContext<'_>, msg: &dyn Any) {
        let req = msg.downcast_ref::<LoginRequest>().unwrap();
        println!(
            "[LoginHandler] processing login — user={}, pass={}",
            req.username, req.password
        );
    }
}

// ── Handler C: ChatHandler (only accepts ChatMessage) ──────

struct ChatHandler;

impl InboundHandler for ChatHandler {
    fn accepted_types(&self) -> Vec<TypeId> {
        vec![TypeId::of::<ChatMessage>()]
    }

    fn on_read(&mut self, _ctx: &mut HandlerContext<'_>, msg: &dyn Any) {
        let cm = msg.downcast_ref::<ChatMessage>().unwrap();
        println!(
            "[ChatHandler]  processing chat — room={}, text={}",
            cm.room, cm.text
        );
    }
}

// ── Main ──────────────────────────────────────────────────

fn main() {
    let config = ServerConfig {
        port: 9002,
        worker_threads: 1,
        pipeline_initializer: Some(Arc::new(|p: &mut Pipeline| {
            p.set_decoder(Box::new(RawParser));
            p.add_inbound(Box::new(CloseAfterRead));
            p.add_inbound(Box::new(LoginHandler));
            p.add_inbound(Box::new(ChatHandler));
        })),
    };

    println!("flux type-dispatch demo on port 9002");
    println!("Pipeline: RawParser(decoder) → CloseAfterRead → LoginHandler → ChatHandler\n");
    println!("Test commands:");
    println!("  echo \"login:alice:s3cret\" | nc localhost 9002");
    println!("  echo \"chat:lobby:hello world\" | nc localhost 9002");
    println!("  echo \"unknown:format\" | nc localhost 9002");
    println!("\nPress Ctrl-C to stop.\n");

    Server::new(config).run();
}
