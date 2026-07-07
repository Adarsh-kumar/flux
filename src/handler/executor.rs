use std::any::Any;
use std::io;

use super::context::HandlerContext;
use crate::session::Session;

/// Runs pipeline events for a session. Owns context creation — this is where
/// synchronous vs async dispatch will diverge in Phase 2.
pub struct PipelineExecutor;

impl PipelineExecutor {
    pub fn new() -> Self {
        Self
    }

    pub fn fire_connect(&self, session: &Session) {
        let mut ctx = HandlerContext::new(session);
        session.pipeline().borrow_mut().fire_connect(&mut ctx);
    }

    pub fn fire_read(&self, session: &Session, msg: &dyn Any) -> bool {
        let mut ctx = HandlerContext::new(session);
        let mut pipeline = session.pipeline().borrow_mut();

        // Decode bytes into messages (identity pass-through when no decoder).
        let bytes = msg
            .downcast_ref::<Vec<u8>>()
            .expect("worker always sends Vec<u8>");
        let messages = pipeline.decode(bytes);

        for msg in &messages {
            ctx.inbound_blocked = false;
            pipeline.fire_read(&mut ctx, msg.as_ref());
        }

        ctx.close_requested
    }

    pub fn fire_disconnect(&self, session: &Session) {
        let mut ctx = HandlerContext::new(session);
        session.pipeline().borrow_mut().fire_disconnect(&mut ctx);
    }

    pub fn fire_error(&self, session: &Session, err: &io::Error) -> bool {
        let mut ctx = HandlerContext::new(session);
        session.pipeline().borrow_mut().fire_error(&mut ctx, err);
        ctx.close_requested
    }
}

impl Default for PipelineExecutor {
    fn default() -> Self {
        Self::new()
    }
}
