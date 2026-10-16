//! Observes the packet decoder (`auto_artifactarium`) through its tracing
//! output.
//!
//! The decoder's API only returns decoded commands, so it can't tell us how
//! the key exchange is going.  Its log events do, so they are recognized here
//! and queued for the monitor, which drains the queue after every packet it
//! feeds to the decoder.

use std::fmt;
use std::sync::Mutex;

use tracing::field::{Field, Visit};
use tracing::level_filters::LevelFilter;
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::filter::filter_fn;
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;

const DECODER_TARGET: &str = "auto_artifactarium";

/// Bounds the queue in case nothing drains it.
const MAX_QUEUED: usize = 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DecoderEvent {
    /// The decoder has the key for this game version's connection setup.
    InitialKeyFound { version: u64 },
    /// The decoder has no key for this game version's connection setup.
    InitialKeyMissing { version: u64 },
    /// The login token was decoded, and the session key will be derived from
    /// it.
    LoginTokenSeen,
    /// The session key for the current connection couldn't be derived.
    SessionKeyNotFound,
    /// A session key that was working stopped decrypting packets.
    SessionKeyLost,
    /// Any other error reported by the decoder.
    OtherError(String),
}

static QUEUE: Mutex<Vec<DecoderEvent>> = Mutex::new(Vec::new());

/// Takes the events reported since the last call.
pub fn drain() -> Vec<DecoderEvent> {
    let mut queue = QUEUE
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    std::mem::take(&mut *queue)
}

fn enqueue(event: DecoderEvent) {
    let mut queue = QUEUE
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if queue.len() < MAX_QUEUED {
        queue.push(event);
    }
}

fn classify(level: Level, message: &str, version: Option<u64>) -> Option<DecoderEvent> {
    let version = version.unwrap_or_default();
    match message {
        "found initial decryption key" => Some(DecoderEvent::InitialKeyFound { version }),
        "didn't find decryption key" => Some(DecoderEvent::InitialKeyMissing { version }),
        "setting new possible session seeds" => Some(DecoderEvent::LoginTokenSeen),
        "Couldn't bruteforce from deduced keys" => Some(DecoderEvent::SessionKeyNotFound),
        "Session key dead, relaunch game" => Some(DecoderEvent::SessionKeyLost),
        // Always follows "didn't find decryption key".
        "No dispatch key found" => None,
        _ if level == Level::ERROR => Some(DecoderEvent::OtherError(message.to_owned())),
        _ => None,
    }
}

/// A tracing layer that feeds decoder log events into the queue.
pub fn layer<S>() -> impl Layer<S>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    let filter = filter_fn(|metadata| {
        metadata.is_event()
            && metadata.target().starts_with(DECODER_TARGET)
            && *metadata.level() <= Level::INFO
    })
    .with_max_level_hint(LevelFilter::INFO);

    DecoderEventLayer.with_filter(filter)
}

struct DecoderEventLayer;

impl<S: Subscriber> Layer<S> for DecoderEventLayer {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let mut fields = EventFields::default();
        event.record(&mut fields);

        if let Some(event) = classify(*event.metadata().level(), &fields.message, fields.version) {
            enqueue(event);
        }
    }
}

#[derive(Default)]
struct EventFields {
    message: String,
    version: Option<u64>,
}

impl Visit for EventFields {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if field.name() == "message" {
            self.message = format!("{value:?}");
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.message = value.to_owned();
        }
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        if field.name() == "version" {
            self.version = Some(value);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_exchange_milestones_are_recognized() {
        assert_eq!(
            classify(Level::INFO, "found initial decryption key", Some(42)),
            Some(DecoderEvent::InitialKeyFound { version: 42 })
        );
        assert_eq!(
            classify(Level::INFO, "setting new possible session seeds", None),
            Some(DecoderEvent::LoginTokenSeen)
        );
        assert_eq!(
            classify(Level::ERROR, "Couldn't bruteforce from deduced keys", None),
            Some(DecoderEvent::SessionKeyNotFound)
        );
    }

    #[test]
    fn routine_messages_are_ignored() {
        assert_eq!(classify(Level::INFO, "received", None), None);
        assert_eq!(classify(Level::WARN, "disconnected packet", None), None);
        assert_eq!(classify(Level::ERROR, "No dispatch key found", None), None);
    }

    #[test]
    fn unknown_errors_are_passed_through() {
        assert_eq!(
            classify(Level::ERROR, "Didn't get magic in try_new!", None),
            Some(DecoderEvent::OtherError(
                "Didn't get magic in try_new!".into()
            ))
        );
    }
}
