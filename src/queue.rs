//! The queue a send target names, as the HTTP URL a message is posted to.
//!
//! A target is `msmq://<host>/<queue>`, the queue's HTTP URL
//! `http://<host>/msmq/<queue>`, or MSMQ's own format name for it,
//! `DIRECT=HTTP://<host>/msmq/<queue>`; each has a guarded form sent over
//! HTTPS.

use net::{Schemes, Target};
use transport::error::{Result, protocol_error};

/// The schemes a queue is written in: `msmq://host/queue` and the HTTP
/// URL `http://host/msmq/queue` for the one, `msmqs://` and `https://`
/// for the queue behind TLS; either HTTP form also as MSMQ's `DIRECT=`
/// format name, in any case.
const SCHEMES: Schemes = Schemes {
    plain: &["http", "msmq"],
    secure: &["https", "msmqs"],
};

/// The queue URL a target names: `msmq://host/queue`, `http://host/msmq/queue`
/// or `DIRECT=HTTP://host/msmq/queue`, each as `http://host/msmq/queue`; and
/// their guarded forms `msmqs://host/queue`, `https://host/msmq/queue` or
/// `DIRECT=HTTPS://host/msmq/queue`, each as `https://host/msmq/queue`.
///
/// # Errors
/// Where the target names no host or no queue.
pub fn queue_url(target: &str) -> Result<String> {
    let url = target
        .get(..7)
        .filter(|head| head.eq_ignore_ascii_case("DIRECT="))
        .map_or(target, |_| &target[7..]);
    let named = Target::parse(url)
        .ok()
        .filter(|named| named.is(SCHEMES.plain) || named.is(SCHEMES.secure))
        .ok_or_else(|| protocol_error(format!("{target:?} is not an MSMQ queue")))?;
    let queue = if named.is(&["msmq", "msmqs"]) {
        named.path()
    } else {
        named.path().strip_prefix("msmq/").unwrap_or_default()
    };
    if named.authority().is_empty() || queue.is_empty() {
        return Err(protocol_error(format!(
            "{target:?} names no host and queue"
        )));
    }
    let scheme = if named.is(SCHEMES.secure) {
        "https"
    } else {
        "http"
    };
    Ok(format!("{scheme}://{}/msmq/{queue}", named.authority()))
}
