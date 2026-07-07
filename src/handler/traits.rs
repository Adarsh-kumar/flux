use std::any::{Any, TypeId};

use super::context::HandlerContext;

// ──────────────────────────────────────────
// InboundHandler
// ──────────────────────────────────────────

/// Handles inbound events (data arriving from the network).
///
/// Every method has a default no-op implementation — override only the
/// events you care about. Handlers are invoked in insertion order.
///
/// ## Type-based routing
///
/// Override [`accepted_types`] to declare which message types this handler
/// handles. The pipeline calls `on_read` only for matching types. Return an
/// empty `Vec` to receive **all** types (the default — useful for loggers
/// and pass-through handlers).
///
/// A negotiation handler is simply an inbound handler that accepts the
/// negotiation message type. The decoder produces the right type; the
/// framework routes it here. No propagation control is needed.
///
/// ```ignore
/// use std::any::TypeId;
/// # use flux::handler::{InboundHandler, HandlerContext};
/// # struct MyHandler;
/// # struct NegoRequest;
///
/// impl InboundHandler for MyHandler {
///     fn accepted_types(&self) -> Vec<TypeId> {
///         vec![TypeId::of::<NegoRequest>()]
///     }
///     fn on_read(&mut self, ctx: &mut HandlerContext<'_>, msg: &dyn std::any::Any) {
///         let req = msg.downcast_ref::<NegoRequest>().unwrap();
///         // msg is guaranteed to be NegoRequest — pipeline filtered it
///     }
/// }
/// ```
///
/// [`accepted_types`]: InboundHandler::accepted_types
pub trait InboundHandler: Send + 'static {
    /// Return the set of message `TypeId`s this handler processes.
    ///
    /// Return an empty `Vec` (the default) to receive **all** message types.
    /// This is appropriate for loggers, counters, and pass-through handlers
    /// that don't filter by type.
    fn accepted_types(&self) -> Vec<TypeId> {
        vec![]
    }

    /// Called when a new connection is established.
    fn on_connect(&mut self, _ctx: &mut HandlerContext<'_>) {}

    /// Called when a message arrives for this handler.
    ///
    /// If [`accepted_types`] returns a non-empty list, `msg` is guaranteed to
    /// be one of the declared types. Use [`Any::downcast_ref`] to extract it.
    ///
    /// [`accepted_types`]: InboundHandler::accepted_types
    /// [`Any::downcast_ref`]: std::any::Any::downcast_ref
    fn on_read(&mut self, _ctx: &mut HandlerContext<'_>, _msg: &dyn Any) {}

    /// Called when the connection is closed (peer EOF, error, or explicit close).
    fn on_disconnect(&mut self, _ctx: &mut HandlerContext<'_>) {}

    /// Called when an I/O error occurs on the connection.
    ///
    /// Default calls `ctx.close()` — unhandled errors close the connection.
    /// Override to handle errors without closing (e.g., log and recover).
    fn on_error(&mut self, ctx: &mut HandlerContext<'_>, _err: &std::io::Error) {
        ctx.close();
    }
}

// ──────────────────────────────────────────
// OutboundHandler
// ──────────────────────────────────────────

/// Handles outbound events (data being written to the network).
///
/// Outbound handlers are invoked in **reverse** insertion order — the last
/// handler added is called first. This mirrors Netty's outbound pipeline
/// semantics (e.g., an encryption handler wraps the raw write).
pub trait OutboundHandler: Send + 'static {
    /// Called before data is written to the socket.
    fn on_write(&mut self, _ctx: &mut HandlerContext<'_>, _data: &[u8]) {}
}

// ──────────────────────────────────────────
// ByteToMessageDecoder
// ──────────────────────────────────────────

/// Converts raw bytes into typed messages for the pipeline.
///
/// Set via [`Pipeline::set_decoder`]. The decoder is stateful — it accumulates
/// bytes internally and returns one message per [`decode`] call. The framework
/// calls [`decode`] in a loop until it returns `None`.
///
/// [`decode`]: ByteToMessageDecoder::decode
/// [`Pipeline::set_decoder`]: super::pipeline::Pipeline::set_decoder
pub trait ByteToMessageDecoder: Send + 'static {
    /// Feed a chunk of bytes into the decoder. Returns `Some(msg)` when a
    /// complete message is decoded, `None` if more bytes are needed.
    fn decode(&mut self, buf: &[u8]) -> Option<Box<dyn Any>>;
}
