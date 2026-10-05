#![forbid(unsafe_code)]

//! Streams that arrive as MSMQ messages over HTTP. One message is one
//! Stream.
//!
//! MSMQ is the queue every Windows shop of the `BizTalk` era already runs,
//! and between machines it speaks SRMP: the message as a SOAP envelope —
//! who it is for, what it is, when it was sent — with the body attached
//! after it in a `multipart/related` POST to `http://<host>/msmq/<queue>`
//! (MS-MQSRM). A Receive Location is the queue's HTTP end: it takes one
//! POST, reads the envelope (`envelope.rs`) and the attachment it names
//! (`mime.rs`), answers `200 OK`, and hands the attachment on as the Stream,
//! bytes as they are. A Send Location composes the same envelope around a
//! Stream and POSTs it, over the http technology the manifest names as the
//! carrier. The binary MSMQ protocol on port 1801 (MS-MQQB) is a machine's
//! own and is not spoken here; SRMP is what MSMQ itself uses to cross one.
//!
//! A queue is not an artefact anyone claims: MSMQ delivers a message once,
//! and the `200 OK` is the receipt, so [`Transport::claims`] answers
//! `None`. The one ceiling is MSMQ's own: a message is at most four
//! mebibytes, and a Stream over that is refused before a request is
//! formed.
//!
//! The origin URI is the queue with the message id as its fragment:
//! `msmq://node-b/orders#uuid:7@node-a`. A send target is
//! `msmq://<host>/<queue>`, the queue's HTTP URL
//! `http://<host>/msmq/<queue>`, or MSMQ's own format name for it,
//! `DIRECT=HTTP://<host>/msmq/<queue>` (`queue.rs`). Each has a guarded form —
//! `msmqs://`, `https://`, `DIRECT=HTTPS://` — sent over HTTPS through the
//! http technology's endpoint, as as2, as4 and webdav are; TLS is its `tls`
//! feature (ADR-0033), and without it an https queue is refused rather
//! than written in the clear.
//!
//! The transport is its own far end (ADR-0051): [`Loopback`] stands the
//! queue's HTTP end up on an ephemeral port and takes the one POST.

pub mod envelope;
pub mod mime;
mod queue;

use std::net::TcpListener;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub use envelope::Envelope;
use http::endpoint::{Connections, Offer};
use http::inbound::{Heard, Inbound};
use http::server;
use net::Endpoint;
use net::ceiling;
use net::http::{Request, Response};
pub use queue::queue_url;
use transport::Configured;
use transport::error::{Result, TransportError, protocol_error};
use transport::listening::Listening;
use transport::loopback::{FarEnd, LOOPBACK_TIMEOUT, Loopback};
use transport::{Arrived, Directions, Taken, Transport, Verdict, socket};
use xcore::settings::{Applies, Kind, Presence, Read, Setting, Settings};

/// The most one MSMQ message carries: four mebibytes.
#[must_use]
pub const fn ceiling() -> usize {
    4 * 1024 * 1024
}

/// The boundary every message this transport composes travels under.
const BOUNDARY: &str = "MSMQ - SOAP boundary, 12345";

pub struct MsmqTransport {
    bind: String,
    host: String,
    next: AtomicU64,
    timeout: Option<Duration>,
    /// The connections kept to the queues' HTTP ends.
    connections: Connections,
    /// The listener a Receive Location keeps, and senders' connections.
    inbound: Inbound,
}

/// What one POST earns at a far end: the message it carries and `200`, or
/// `400` and the refusal where it is not an SRMP message.
fn answer(request: &Request) -> (Result<Taken>, Response) {
    match take(request) {
        Ok(taken) => (Ok(taken), Response::new(200)),
        Err(error) => {
            let refusal = refusal(&error);
            (Err(error), refusal)
        }
    }
}

/// What a POST that is not an SRMP message is answered: `400` and why.
fn refusal(error: &TransportError) -> Response {
    Response::new(400).body(error.message.as_bytes())
}

/// What a sender is answered once its message's receive cycle has ended:
/// `200` when it was accepted, as an MSMQ queue's HTTP end answers; `401`,
/// `403` or `422` when it was refused (`http::server::refused`), a client
/// error the sender does not send again; `503` when Xmip could not
/// complete the cycle, so the sender keeps the message and sends it again.
fn verdict(verdict: Verdict) -> Response {
    match verdict {
        Verdict::Accepted => Response::new(200),
        Verdict::Refused(_) | Verdict::Failed => server::verdict(verdict),
    }
}

impl MsmqTransport {
    /// Listen at `bind` as the queue's HTTP end, and sign messages as
    /// `host`.
    #[must_use]
    pub fn new(bind: impl Into<String>, host: impl Into<String>) -> Self {
        Self {
            bind: bind.into(),
            host: host.into(),
            next: AtomicU64::new(1),
            timeout: None,
            connections: Connections::new(),
            inbound: Inbound::new(),
        }
    }

    /// Give up on a peer that stops mid-message.
    #[must_use]
    pub const fn timing_out_after(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Bind and report the address actually assigned.
    ///
    /// # Errors
    /// Where the address is taken, malformed, or not permitted.
    pub fn bind(&self) -> Result<(TcpListener, String)> {
        socket::bind_tcp(&self.bind)
    }

    /// Take one message from an already-bound listener: one POST, read,
    /// answered.
    ///
    /// # Errors
    /// Where the connection could not be accepted or read, or the request
    /// was not an SRMP message — which is answered `400` and refused.
    pub fn accept_one(&self, listener: &TcpListener) -> Result<Taken> {
        server::serve_one(listener, self.timeout, answer)?
    }

    /// The POST that carries `bytes` to the queue at `url`, its message id
    /// `uuid:<key>@<host>` where there is a key, the same on every attempt,
    /// and `uuid:<n>@<host>` by this transport's count where there is none.
    ///
    /// # Errors
    /// Where the Stream is over the [`ceiling`].
    pub fn compose(&self, url: &str, bytes: &[u8], key: Option<&str>) -> Result<Request> {
        ceiling::within(bytes.len(), ceiling(), "one MSMQ message carries")?;
        let n = self.next.fetch_add(1, Ordering::Relaxed);
        let id = match key {
            Some(key) => format!("uuid:{key}@{}", self.host),
            None => format!("uuid:{n}@{}", self.host),
        };
        let body_id = format!("body{n}@{}", self.host);
        let envelope = Envelope::new(&id, url, &body_id, now());
        let parts = [
            mime::part(
                "text/xml",
                &format!("envelope{n}@{}", self.host),
                envelope::compose(&envelope).as_bytes(),
            ),
            mime::part("application/octet-stream", &body_id, bytes),
        ];
        let endpoint = Endpoint::parse(url)?;
        Ok(Request::new("POST", endpoint.path())
            .header("Host", &endpoint.authority())
            .header("Content-Type", &mime::content_type(BOUNDARY))
            .header("SOAPAction", "\"MSMQMessage\"")
            .header("Proxy-Accept", "NonInteractiveClient")
            .body(&codec::mime::write(BOUNDARY, &parts)))
    }
}

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
            Setting {
                name: "timeout",
                kind: Kind::Duration,
                presence: Presence::Optional,
                meaning: "How long a peer that stops mid-message is waited on.",
                applies: Applies::Both,
            },
        ],
    };

    fn configured(address: &str, settings: &Read) -> Result<Self> {
        // A Receive Location signs nothing, so it reads no host.
        let transport = Self::new(address, settings.optional_text("host").unwrap_or_default());
        Ok(match settings.optional_duration("timeout") {
            Some(timeout) => transport.timing_out_after(timeout),
            None => transport,
        })
    }
}

/// The Stream a request carries, with the queue and id it carries it under.
///
/// # Errors
/// Where the request is not a POST to `/msmq/<queue>`, not
/// `multipart/related`, carries no SRMP envelope or one that is not UTF-8
/// text, or names an attachment that is not there. The attachment is bytes
/// and is carried as it arrived.
pub fn take(request: &Request) -> Result<Taken> {
    if request.method != "POST" {
        return Err(protocol_error(format!(
            "a {} where MSMQ POSTs",
            request.method
        )));
    }
    let queue = request
        .path
        .strip_prefix("/msmq/")
        .filter(|queue| !queue.is_empty())
        .ok_or_else(|| protocol_error(format!("{:?} is not /msmq/<queue>", request.path)))?;
    let boundary = codec::mime::boundary_of(
        request.header_value("Content-Type").unwrap_or_default(),
        "multipart/related",
    )?;
    let parts = mime::parts(boundary, &request.body)?;
    let first = parts
        .first()
        .ok_or_else(|| protocol_error("a message with no envelope"))?;
    let envelope = std::str::from_utf8(&first.body).map_err(|refused| {
        protocol_error(format!(
            "an SRMP envelope that is not UTF-8 text: {refused}"
        ))
    })?;
    let envelope = envelope::parse(envelope)?;
    let body = parts
        .iter()
        .find(|part| part.content_id() == Some(envelope.body_id.as_str()))
        .ok_or_else(|| protocol_error(format!("no attachment {:?}", envelope.body_id)))?;
    let host = request.header_value("Host").unwrap_or("localhost");
    Ok(Taken::new(
        format!("msmq://{host}/{queue}#{}", envelope.id),
        body.body.clone(),
    ))
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_secs())
}

impl Transport for MsmqTransport {
    fn name(&self) -> &'static str {
        "msmq"
    }

    fn directions(&self) -> Directions {
        Directions::BOTH
    }

    fn arrivals(&self) -> transport::Arrivals {
        transport::Arrivals::Unordered(
            "each request is its own, and a connection waiting for its answer takes no next request",
        )
    }

    /// The next message from whichever sender posts first, on the listener
    /// the first receive bound and the connections senders keep. The
    /// sender waits for its answer until the receive cycle has ended:
    /// `200` when it accepted the message, `503` when it refused it, so the
    /// sender sends it again. What is not an SRMP message is answered `400`
    /// at once.
    fn receive(&self) -> Result<Vec<Arrived>> {
        let (taken, reply) = self.inbound.next(
            || self.bind(),
            self.timeout,
            |request, _| match take(&request) {
                Ok(taken) => Heard::Waiting(Ok(taken)),
                Err(error) => {
                    let refusal = refusal(&error);
                    Heard::Answered(Err(error), refusal)
                }
            },
        )?;
        let taken = taken?;
        let reply = reply.ok_or_else(|| protocol_error("a message answered unheard"))?;
        Ok(vec![Arrived::whole(
            taken.origin_uri,
            taken.bytes,
            reply.acknowledgement(verdict),
        )])
    }

    /// POST the bytes as one message to the queue the target names.
    fn send(&self, target: &str, bytes: &[u8]) -> Result<()> {
        self.post(target, bytes, None)
    }

    /// The key goes in the SRMP envelope's message id, `uuid:<key>@<host>`,
    /// by which the receiving queue manager tells a message sent again from
    /// one it holds.
    fn send_keyed(&self, target: &str, bytes: &[u8], key: &str) -> Result<()> {
        self.post(target, bytes, Some(key))
    }
}

impl MsmqTransport {
    /// The one send: one message posted to the queue `target` names.
    fn post(&self, target: &str, bytes: &[u8], key: Option<&str>) -> Result<()> {
        let url = queue_url(target)?;
        let request = self.compose(&url, bytes, key)?;
        let endpoint = Endpoint::parse(&url)?;
        let response =
            self.connections
                .exchange(&endpoint, self.timeout, Offer::Http11, &request)?;
        if (200..300).contains(&response.status) {
            Ok(())
        } else {
            let detail = response
                .text()
                .map_or_else(|refused| refused.to_string(), str::to_string);
            Err(TransportError {
                message: format!("the queue answered {}: {detail}", response.status),
                retryable: http::status::retryable(response.status),
            })
        }
    }
}

impl MsmqTransport {
    /// Both ends on this machine: the queue's HTTP end on an ephemeral
    /// local port, the loopback timeout on both sides.
    #[must_use]
    pub fn loopback() -> Self {
        Self::new("127.0.0.1:0", "far").timing_out_after(LOOPBACK_TIMEOUT)
    }

    /// This transport's configuration in a fresh instance signing as
    /// `host` — its own message counter, as another node has.
    fn sibling(&self, host: &str) -> Self {
        let sibling = Self::new(self.bind.as_str(), host);
        match self.timeout {
            Some(timeout) => sibling.timing_out_after(timeout),
            None => sibling,
        }
    }
}

impl Loopback for MsmqTransport {
    fn ceiling(&self) -> Option<usize> {
        Some(ceiling())
    }

    fn far_end(&self) -> Result<Box<dyn FarEnd>> {
        let transport = self.sibling(&self.host);
        Ok(Box::new(Listening::new(
            move |listener: &TcpListener| transport.accept_one(listener),
            self.bind()?,
        )))
    }

    fn send_to(&self, address: &str, payload: &[u8]) -> Result<()> {
        self.sibling("near")
            .send(&format!("msmq://{address}/round-trip"), payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    #[test]
    fn msmq_declares_its_settings_and_reads_through_them() {
        use xcore::settings::Given;
        assert!(MsmqTransport::SETTINGS.problems().is_empty());
        let given = [
            ("host".to_string(), Given::Text("node-a".to_string())),
            ("timeout".to_string(), Given::Text("10s".to_string())),
        ];
        let built = MsmqTransport::open("0.0.0.0:80", Applies::Send, &given).expect("built");
        assert_eq!(built.host, "node-a");
        assert_eq!(built.timeout, Some(Duration::from_secs(10)));
        let receiving =
            MsmqTransport::open("0.0.0.0:80", Applies::Receive, &given[1..]).expect("built");
        assert_eq!(receiving.bind, "0.0.0.0:80");
        let Err(refused) = MsmqTransport::open("0.0.0.0:80", Applies::Send, &given[1..]) else {
            panic!("a Send Location signs as a host");
        };
        assert!(refused.message.contains("\"host\""), "{refused}");
    }

    #[test]
    fn every_receive_takes_from_one_kept_listener_and_one_kept_connection() {
        let queue = MsmqTransport::new("127.0.0.1:0", "node-b").timing_out_after(secs(2));
        let address = queue.inbound.bound(|| queue.bind()).expect("bound");
        let target = format!("msmq://{address}/orders");
        let sender = std::thread::spawn(move || {
            let near = MsmqTransport::new("127.0.0.1:0", "node-a").timing_out_after(secs(2));
            for round in 0..5u8 {
                near.send(&target, &[round]).expect("sent");
            }
            near.connections.opened()
        });
        for round in 0..5u8 {
            let arrived = queue.receive().expect("received").remove(0);
            assert_eq!(arrived.taken().expect("taken").bytes, [round]);
        }
        assert_eq!(
            sender.join().expect("sender"),
            1,
            "one connection for every send"
        );
        assert_eq!(queue.inbound.open(), 1);
    }

    #[test]
    fn a_message_posted_to_the_queue_arrives_as_its_attachment() {
        let far_end = MsmqTransport::new("127.0.0.1:0", "node-b").timing_out_after(secs(2));
        let (listener, address) = far_end.bind().expect("binding");
        let sender = std::thread::spawn(move || {
            let near = MsmqTransport::new("127.0.0.1:0", "node-a").timing_out_after(secs(2));
            near.send(&format!("msmq://{address}/orders"), b"\x00order\r\n\xff")?;
            near.send(&format!("DIRECT=HTTP://{address}/msmq/invoices"), b"")
        });
        let first = far_end.accept_one(&listener).expect("the order");
        let second = far_end.accept_one(&listener).expect("the invoice");
        sender.join().expect("thread").expect("sending");
        assert_eq!(first.bytes, b"\x00order\r\n\xff");
        assert!(first.origin_uri.starts_with("msmq://127.0.0.1:"));
        assert!(
            first.origin_uri.ends_with("/orders#uuid:1@node-a"),
            "{}",
            first.origin_uri
        );
        assert!(second.bytes.is_empty());
        assert!(second.origin_uri.ends_with("/invoices#uuid:2@node-a"));
    }

    #[test]
    fn the_sender_is_answered_200_when_accepted_503_when_failed_and_4xx_when_refused() {
        let queue = MsmqTransport::new("127.0.0.1:0", "node-b").timing_out_after(secs(2));
        let address = queue.inbound.bound(|| queue.bind()).expect("bound");
        let target = format!("msmq://{address}/orders");
        let sender = std::thread::spawn(move || {
            let near = MsmqTransport::new("127.0.0.1:0", "node-a").timing_out_after(secs(2));
            let failed = near.send(&target, b"first try");
            let accepted = near.send(&target, b"second try");
            let refused = near.send(&target, b"third try");
            (failed, accepted, refused)
        });
        let first = queue.receive().expect("first").remove(0);
        assert!(first.defers(), "the sender waits for the verdict");
        first.failed().expect("answered");
        let second = queue.receive().expect("second").remove(0);
        assert_eq!(second.taken().expect("taken").bytes, b"second try");
        let third = queue.receive().expect("third").remove(0);
        third
            .refused(transport::Refusal::Forbidden)
            .expect("answered");
        let (failed, accepted, refused) = sender.join().expect("thread");
        let failed = failed.expect_err("503");
        assert!(
            failed.retryable && failed.message.contains("503"),
            "{failed}"
        );
        accepted.expect("200");
        let refused = refused.expect_err("403");
        assert!(!refused.retryable, "not sent again: {refused}");
        assert!(refused.message.contains("403"), "{refused}");
    }

    #[test]
    fn what_is_not_an_srmp_message_is_answered_400_and_refused() {
        let far_end = MsmqTransport::new("127.0.0.1:0", "node-b").timing_out_after(secs(2));
        let (listener, address) = far_end.bind().expect("binding");
        let sender = std::thread::spawn(move || {
            http::HttpTransport::new("127.0.0.1:0")
                .send(&format!("http://{address}/msmq/orders"), b"<order/>")
        });
        let refused = far_end.accept_one(&listener).expect_err("not multipart");
        assert!(!refused.retryable);
        assert!(refused.message.contains("multipart/related"), "{refused}");
        let sent = sender.join().expect("thread").expect_err("answered 400");
        assert!(sent.message.contains("400"), "{sent}");
    }

    #[test]
    fn an_envelope_that_is_not_utf_8_is_refused_and_never_read_lossily() {
        let near = MsmqTransport::new("127.0.0.1:0", "node-a");
        let mut request = near
            .compose("http://node-b/msmq/orders", b"\xff", None)
            .expect("composed");
        let at = request
            .body
            .windows(8)
            .position(|window| window == b"<action>")
            .expect("an action");
        request.body.insert(at + 8, 0xfe);
        let refused = take(&request).expect_err("not UTF-8");
        assert!(!refused.retryable);
        assert!(refused.message.contains("not UTF-8"), "{refused}");
    }

    #[test]
    fn a_target_is_read_in_each_of_the_three_forms() {
        assert_eq!(
            queue_url("msmq://node-b:8080/orders").expect("url"),
            "http://node-b:8080/msmq/orders"
        );
        assert_eq!(
            queue_url("DIRECT=HTTP://node-b/msmq/orders").expect("url"),
            "http://node-b/msmq/orders"
        );
        assert_eq!(
            queue_url("http://node-b/msmq/orders").expect("url"),
            "http://node-b/msmq/orders"
        );
        assert!(queue_url("msmq://node-b").is_err(), "no queue");
        assert!(queue_url("tcp://node-b/orders").is_err(), "not MSMQ");
        assert!(
            queue_url("http://node-b/orders").is_err(),
            "not under /msmq/"
        );
    }

    #[test]
    fn a_guarded_target_is_written_as_https_in_each_of_the_three_forms() {
        for target in [
            "msmqs://node-b:8443/orders",
            "https://node-b:8443/msmq/orders",
            "DIRECT=HTTPS://node-b:8443/msmq/orders",
            "direct=https://node-b:8443/msmq/orders",
        ] {
            assert_eq!(
                queue_url(target).expect(target),
                "https://node-b:8443/msmq/orders",
                "{target}"
            );
        }
        assert!(queue_url("msmqs://node-b").is_err(), "no queue");
        let near = MsmqTransport::new("127.0.0.1:0", "node-a");
        let request = near
            .compose("https://node-b:8443/msmq/orders", b"fits", None)
            .expect("composed");
        assert_eq!(request.header_value("Host"), Some("node-b:8443"));
        assert_eq!(request.path, "/msmq/orders");
    }

    #[test]
    fn a_guarded_queue_is_carried_to_the_endpoint_rather_than_sent_in_the_clear() {
        // A listener that accepts and never speaks: a TLS build reaches it
        // and waits out a handshake, a build without TLS refuses https on
        // the open socket rather than write the message in the clear.
        let (listener, address) = socket::bind_tcp("127.0.0.1:0").expect("bind");
        let near = MsmqTransport::new("127.0.0.1:0", "node-a").timing_out_after(secs(1));
        let failure = near
            .send(&format!("msmqs://{address}/orders"), b"<order/>")
            .expect_err("no handshake");
        drop(listener);
        let refused = failure.message.contains("no tls feature");
        #[cfg(feature = "tls")]
        assert!(!refused, "{failure}");
        #[cfg(not(feature = "tls"))]
        assert!(refused, "{failure}");
    }

    #[test]
    fn a_stream_over_the_ceiling_is_refused_before_a_request_is_formed() {
        let near = MsmqTransport::new("127.0.0.1:0", "node-a");
        let over = vec![0u8; ceiling() + 1];
        let error = near
            .compose("http://node-b/msmq/orders", &over, None)
            .expect_err("over");
        assert!(!error.retryable);
        assert!(error.message.contains("4194304"), "{error}");
        let request = near
            .compose("http://node-b/msmq/orders", b"fits", None)
            .expect("fits");
        assert_eq!(request.header_value("Host"), Some("node-b"));
        assert_eq!(request.path, "/msmq/orders");
        assert_eq!(near.name(), "msmq");
        assert_eq!(near.directions(), Directions::BOTH);
        assert!(near.claims().is_none(), "the receipt is the 200");
    }

    /// The payloads an attachment must carry whole, and one at the brim.
    fn edge_payloads() -> Vec<(&'static str, Vec<u8>)> {
        vec![
            ("empty", Vec::new()),
            ("one byte", vec![0x2a]),
            ("every byte", (0..=255).collect()),
            ("nul run", vec![0; 512]),
            ("high bytes", vec![0xff; 512]),
            ("crlf storm", b"\r\n".repeat(400)),
            ("the brim", vec![b'm'; ceiling()]),
        ]
    }

    #[test]
    fn a_loopback_round_posts_one_message_and_takes_it_at_the_queue() {
        let msmq = MsmqTransport::loopback();
        let arrived = msmq.round(b"\x00order\r\n\xff").expect("round");
        assert_eq!(arrived.bytes, b"\x00order\r\n\xff");
        assert!(arrived.origin_uri.starts_with("msmq://127.0.0.1:"));
        assert!(
            arrived.origin_uri.ends_with("/round-trip#uuid:1@near"),
            "{}",
            arrived.origin_uri
        );
        assert_eq!(msmq.name(), "msmq");
        assert!(msmq.refuses(&[0, 0xff]).is_none(), "bytes are bytes");
    }

    #[test]
    fn the_loopback_returns_the_edge_payloads_whole_and_refuses_over_the_brim() {
        let msmq = MsmqTransport::loopback();
        assert_eq!(msmq.ceiling(), Some(4 * 1024 * 1024));
        for (name, payload) in edge_payloads() {
            let arrived = msmq.round(&payload).expect(name);
            assert_eq!(arrived.bytes, payload, "{name}");
        }
        let over = vec![b'm'; ceiling() + 1];
        let failure = msmq.round(&over).expect_err("over the brim");
        assert!(failure.message.starts_with("send failed:"), "{failure}");
        assert!(failure.message.contains("4194304"), "{failure}");
    }
}
