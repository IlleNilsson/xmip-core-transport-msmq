//! This technology's declaration: the transport guide's "The declaration",
//! `module/core/capability/transport/doc/adding-a-transport.md`.

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

/// This technology's schemes, as [`transport::Configured::SCHEMES`] defines
/// them: `msmqs` for the queue's HTTP end behind TLS.
pub const SCHEMES: &[&str] = &["msmq", "msmqs"];
