use crate::session::Session;

/// Context passed to every handler method.
///
/// `HandlerContext` is the handler's window into the pipeline and session.
/// Through it, a handler can:
///
/// - **Access the session** — `ctx.session()` to write data, get IDs, etc.
/// - **Control propagation** — `ctx.stop_inbound()` / `ctx.stop_outbound()`
/// - **Close the session** — `ctx.close()`
///
/// Byte-to-message conversion happens via [`ByteToMessageDecoder`], not
/// through the context.
///
/// [`ByteToMessageDecoder`]: super::traits::ByteToMessageDecoder
pub struct HandlerContext<'s> {
    session: &'s Session,
    pub(crate) close_requested: bool,
    pub(crate) inbound_blocked: bool,
    pub(crate) outbound_blocked: bool,
}

impl<'s> HandlerContext<'s> {
    /// Create a context referencing the given session.
    pub fn new(session: &'s Session) -> Self {
        Self {
            session,
            close_requested: false,
            inbound_blocked: false,
            outbound_blocked: false,
        }
    }

    /// The session this handler is attached to.
    pub fn session(&self) -> &Session {
        self.session
    }

    /// Halt further inbound propagation for the current event.
    pub fn stop_inbound(&mut self) {
        self.inbound_blocked = true;
    }

    /// Halt further outbound propagation for the current event.
    pub fn stop_outbound(&mut self) {
        self.outbound_blocked = true;
    }

    /// Request that the session be closed.
    pub fn close(&mut self) {
        self.close_requested = true;
    }
}
