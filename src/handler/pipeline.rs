use std::any::Any;

use super::context::HandlerContext;
use super::traits::{ByteToMessageDecoder, InboundHandler, OutboundHandler};

// ──────────────────────────────────────────
// Pipeline — per-connection handler chain
// ──────────────────────────────────────────

/// An ordered chain of inbound and outbound handlers for a single connection.
///
/// ## Event flow
///
/// ```text
/// Inbound  (read, connect, disconnect, error):
///   Handler 0 → Handler 1 → ... → Handler N
///
/// Outbound (write):
///   Handler N → ... → Handler 1 → Handler 0   (reverse order)
/// ```
///
/// ## Propagation control
///
/// By default, events flow through *every* handler. A handler calls
/// [`HandlerContext::stop_inbound`] or [`HandlerContext::stop_outbound`]
/// to halt the chain. This is how NegoHandler will prevent downstream
/// handlers from processing data before negotiation completes.
///
/// ## Lifecycle
///
/// One `Pipeline` is created per connection. When the connection is
/// accepted, the pipeline is constructed, handlers are added, and
/// [`fire_connect`] is called. The pipeline lives until the connection
/// is dropped.
///
/// [`HandlerContext::stop_inbound`]: super::context::HandlerContext::stop_inbound
/// [`HandlerContext::stop_outbound`]: super::context::HandlerContext::stop_outbound
/// [`fire_connect`]: Pipeline::fire_connect
pub struct Pipeline {
    inbound: Vec<Box<dyn InboundHandler>>,
    outbound: Vec<Box<dyn OutboundHandler>>,
    decoder: Option<Box<dyn ByteToMessageDecoder>>,
}

impl Pipeline {
    /// Create an empty pipeline.
    pub fn new() -> Self {
        Self {
            inbound: Vec::new(),
            outbound: Vec::new(),
            decoder: None,
        }
    }

    // ── Pipeline construction ──

    /// Append an inbound handler to the end of the chain.
    pub fn add_inbound(&mut self, handler: Box<dyn InboundHandler>) {
        self.inbound.push(handler);
    }

    /// Append an outbound handler to the end of the chain.
    ///
    /// Remember: outbound handlers fire in *reverse* order — the last
    /// handler added is the first one called on write.
    pub fn add_outbound(&mut self, handler: Box<dyn OutboundHandler>) {
        self.outbound.push(handler);
    }

    /// Set the byte-to-message decoder for this pipeline.
    pub fn set_decoder(&mut self, decoder: Box<dyn ByteToMessageDecoder>) {
        self.decoder = Some(decoder);
    }

    /// Decode raw bytes into typed messages. Feeds the current chunk on the
    /// first call, then drains cached messages with `&[]` until the decoder
    /// returns `None`.
    pub(crate) fn decode(&mut self, bytes: &[u8]) -> Vec<Box<dyn Any>> {
        let mut messages = Vec::new();
        match &mut self.decoder {
            Some(decoder) => {
                if let Some(msg) = decoder.decode(bytes) {
                    messages.push(msg);
                    while let Some(msg) = decoder.decode(&[]) {
                        messages.push(msg);
                    }
                }
            }
            None => {
                messages.push(Box::new(bytes.to_vec()));
            }
        }
        messages
    }

    /// Number of inbound handlers in this pipeline.
    pub fn inbound_len(&self) -> usize {
        self.inbound.len()
    }

    /// Number of outbound handlers in this pipeline.
    pub fn outbound_len(&self) -> usize {
        self.outbound.len()
    }

    // ── Inbound event dispatch ──

    /// Fire `on_connect` on every inbound handler.
    ///
    /// The caller is responsible for creating the `HandlerContext`.
    pub(crate) fn fire_connect(&mut self, ctx: &mut HandlerContext<'_>) {
        for handler in &mut self.inbound {
            handler.on_connect(ctx);
            if ctx.inbound_blocked {
                break;
            }
        }
    }

    /// Fire `on_read` on every inbound handler that accepts this message type.
    ///
    /// Single pass through handlers. Type filtering uses `accepted_types()`.
    pub(crate) fn fire_read(&mut self, ctx: &mut HandlerContext<'_>, msg: &dyn Any) {
        let type_id = msg.type_id();
        for handler in &mut self.inbound {
            let accepted = handler.accepted_types();
            if !accepted.is_empty() && !accepted.contains(&type_id) {
                continue;
            }
            handler.on_read(ctx, msg);
            if ctx.inbound_blocked {
                break;
            }
        }
    }

    /// Fire `on_disconnect` on every inbound handler.
    pub(crate) fn fire_disconnect(&mut self, ctx: &mut HandlerContext<'_>) {
        for handler in &mut self.inbound {
            handler.on_disconnect(ctx);
            if ctx.inbound_blocked {
                break;
            }
        }
    }

    /// Fire `on_error` on every inbound handler.
    pub(crate) fn fire_error(&mut self, ctx: &mut HandlerContext<'_>, err: &std::io::Error) {
        for handler in &mut self.inbound {
            handler.on_error(ctx, err);
            if ctx.inbound_blocked {
                break;
            }
        }
    }

    // ── Outbound event dispatch ──

    /// Fire `on_write` on every outbound handler, in reverse order.
    pub(crate) fn fire_write(&mut self, ctx: &mut HandlerContext<'_>, data: &[u8]) {
        for handler in self.outbound.iter_mut().rev() {
            handler.on_write(ctx, data);
            if ctx.outbound_blocked {
                break;
            }
        }
    }
}
