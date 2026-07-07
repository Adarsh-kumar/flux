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

    /// Decode `bytes` into typed messages and dispatch each through the pipeline.
    ///
    /// Returns `true` if any handler requested the session be closed.
    pub fn fire_read(&self, session: &Session, bytes: &[u8]) -> bool {
        let mut ctx = HandlerContext::new(session);
        let mut pipeline = session.pipeline().borrow_mut();

        let messages = pipeline.decode(bytes);

        for msg in &messages {
            pipeline.fire_read(&mut ctx, msg.as_ref());
            if ctx.close_requested {
                break;
            }
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
