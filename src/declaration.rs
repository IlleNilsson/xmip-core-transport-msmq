//! What a Location of this technology is configured with: its declaration,
//! which `Configured` reads, built with the crate's `declaration` feature
//! alone and the code behind the default (ADR-0064, amendments 2026-09-26,
//! 2026-10-22 and 2026-11-26), for the library the surfaces load to carry
//! (open problem 48).

use xcore::settings::{Applies, Kind, Presence, Setting, Settings};

/// Every setting a Location takes beyond its address, where a Receive
/// Location listens as the queue's HTTP end; a Send Location's queue is its
/// target.
pub const SETTINGS: &Settings = &Settings {
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

/// The schemes a Location's address is entered with to choose this
/// technology: `msmqs` for the queue's HTTP end behind TLS.
pub const SCHEMES: &[&str] = &["msmq", "msmqs"];
