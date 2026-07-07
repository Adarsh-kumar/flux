use crate::session::Session;

/// Context passed to every handler method.
///
/// `HandlerContext` is the handler's window into the session.
/// Through it, a handler can:
///
/// - **Access the session** — `ctx.session()` to write data, get IDs, etc.
/// - **Close the session** — `ctx.close()`
///
/// Routing — deciding which handlers see which messages — is handled by the
/// framework via [`InboundHandler::accepted_types`]. Handlers do not need to
/// control propagation manually.
///
/// [`InboundHandler::accepted_types`]: super::traits::InboundHandler::accepted_types
pub struct HandlerContext<'s> {
    session: &'s Session,
    pub(crate) close_requested: bool,
}

impl<'s> HandlerContext<'s> {
    /// Create a context referencing the given session.
    pub fn new(session: &'s Session) -> Self {
        Self {
            session,
            close_requested: false,
        }
    }

    /// The session this handler is attached to.
    pub fn session(&self) -> &Session {
        self.session
    }

    /// Request that the session be closed after this handler returns.
    pub fn close(&mut self) {
        self.close_requested = true;
    }
}
