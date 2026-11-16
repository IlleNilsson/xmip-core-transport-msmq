//! What an MSMQ Location is configured with: the declaration
//! [`MsmqTransport`] reads its settings through (ADR-0064, amendment
//! 2026-09-26).

use std::time::Duration;

use transport::Configured;
use transport::error::Result;
use xcore::settings::{Applies, Kind, Presence, Read, Setting, Settings};

use crate::MsmqTransport;

impl Configured for MsmqTransport {
    /// The address is where a Receive Location listens as the queue's HTTP
    /// end; a Send Location's queue is its target.
    const SETTINGS: &'static Settings = &Settings {
        technology: env!("CARGO_PKG_NAME"),
        settings: &[
            Setting {
                name: "host",
                kind: Kind::Text,
                presence: Presence::Required,
                meaning: "The machine a Send Location signs its message identifiers as.",
                applies: Applies::Send,
            },
            transport::configured::timeout(
                Applies::Both,
                None,
                "How long a peer that stops mid-message is waited on.",
            ),
        ],
    };

    const SCHEMES: &'static [&'static str] = &["msmq", "msmqs"];
    /// The whole address: its scheme says whether TLS guards the endpoint.
    fn own_terms(address: &str) -> &str {
        address
    }

    fn timeout_kept(&mut self) -> Option<&mut dyn transport::configured::TimeoutKept> {
        Some(&mut self.timeout)
    }

    fn configured(address: &str, settings: &Read) -> Result<Self> {
        // A Receive Location signs nothing, so it reads no host.
        Ok(Self::new(
            address,
            settings.optional_text("host").unwrap_or_default(),
        ))
    }

    /// Each caller's turn on a Receive Location taking at most `turn`.
    fn turning_within(mut self, turn: Duration) -> Self {
        self.inbound = self.inbound.turning_within(turn);
        self
    }

    /// Each callers turn on a Receive Location, and an HTTP/2 peers frames
    /// in a span both ways, counted on clock.
    fn timed_by(mut self, clock: &std::sync::Arc<dyn xcore::Clock>) -> Self {
        self.inbound = self.inbound.timed_by(clock);
        self.connections = self.connections.timed_by(clock);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_receive_location_turns_each_caller_within_the_nodes_span() {
        let address = "msmq://127.0.0.1:0/orders";
        let built = MsmqTransport::open(address, Applies::Receive, &[]).expect("built");
        assert_eq!(built.inbound.turn(), transport::serving::TURN);
        let turning = built.turning_within(Duration::from_secs(5));
        assert_eq!(turning.inbound.turn(), Duration::from_secs(5));
    }
}
