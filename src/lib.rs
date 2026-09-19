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
//! `DIRECT=HTTP://<host>/msmq/<queue>`.
//!
//! The transport is its own far end (ADR-0051): [`Loopback`] stands the
//! queue's HTTP end up on an ephemeral port and takes the one POST.

pub mod envelope;
pub mod mime;

use std::net::TcpListener;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub use envelope::Envelope;
use http::message::{self, Request, Response};
pub use mime::Part;
use transport::error::{Result, TransportError, protocol_error};
use transport::loopback::{FarEnd, LOOPBACK_TIMEOUT, Loopback};
use transport::{Arrived, Directions, Transport, socket};

/// The most one MSMQ message carries: four mebibytes.
#[must_use]
pub const fn ceiling() -> usize {
    4 * 1024 * 1024
}

/// The boundary every message this transport composes travels under.
pub const BOUNDARY: &str = "MSMQ - SOAP boundary, 12345";

pub struct MsmqTransport {
    bind: String,
    host: String,
    next: AtomicU64,
    timeout: Option<Duration>,
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
    pub fn accept_one(&self, listener: &TcpListener) -> Result<Arrived> {
        let (stream, _) = socket::accept_tcp(listener, self.timeout)?;
        let (mut reader, mut writer) = socket::split(stream)?;
        let request = message::read_request(&mut reader)?
            .ok_or_else(|| protocol_error("a connection that sent no request"))?;
        match take(&request) {
            Ok(arrived) => {
                message::write_response(&mut writer, &Response::new(200))?;
                Ok(arrived)
            }
            Err(error) => {
                let refusal = Response::new(400).body(error.message.as_bytes());
                message::write_response(&mut writer, &refusal)?;
                Err(error)
            }
        }
    }

    /// The POST that carries `bytes` to the queue at `url`.
    ///
    /// # Errors
    /// Where the Stream is over the [`ceiling`].
    pub fn compose(&self, url: &str, bytes: &[u8]) -> Result<Request> {
        if bytes.len() > ceiling() {
            return Err(TransportError::permanent(format!(
                "{} bytes is over the {} one MSMQ message carries",
                bytes.len(),
                ceiling()
            )));
        }
        let n = self.next.fetch_add(1, Ordering::Relaxed);
        let id = format!("uuid:{n}@{}", self.host);
        let body_id = format!("body{n}@{}", self.host);
        let envelope = Envelope::new(&id, url, &body_id, now());
        let parts = [
            Part {
                content_type: "text/xml".to_string(),
                content_id: format!("envelope{n}@{}", self.host),
                bytes: envelope::compose(&envelope).into_bytes(),
            },
            Part {
                content_type: "application/octet-stream".to_string(),
                content_id: body_id,
                bytes: bytes.to_vec(),
            },
        ];
        let target = http::target::HttpTarget::parse(url)?;
        Ok(Request::new("POST", target.path)
            .header("Host", target.authority)
            .header("Content-Type", &mime::content_type(BOUNDARY))
            .header("SOAPAction", "\"MSMQMessage\"")
            .header("Proxy-Accept", "NonInteractiveClient")
            .body(&mime::compose(BOUNDARY, &parts)))
    }
}

/// The Stream a request carries, with the queue and id it carries it under.
///
/// # Errors
/// Where the request is not a POST to `/msmq/<queue>`, not
/// `multipart/related`, carries no SRMP envelope, or names an attachment
/// that is not there.
pub fn take(request: &Request) -> Result<Arrived> {
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
    let boundary = mime::boundary_of(request.header_value("Content-Type").unwrap_or_default())?;
    let parts = mime::parse(&boundary, &request.body)?;
    let first = parts
        .first()
        .ok_or_else(|| protocol_error("a message with no envelope"))?;
    let envelope = envelope::parse(&String::from_utf8_lossy(&first.bytes))?;
    let body = parts
        .iter()
        .find(|part| part.content_id == envelope.body_id)
        .ok_or_else(|| protocol_error(format!("no attachment {:?}", envelope.body_id)))?;
    let host = request.header_value("Host").unwrap_or("localhost");
    Ok(Arrived::new(
        format!("msmq://{host}/{queue}#{}", envelope.id),
        body.bytes.clone(),
    ))
}

/// The queue URL a target names: `msmq://host/queue`, `http://host/msmq/queue`
/// or `DIRECT=HTTP://host/msmq/queue`, each as `http://host/msmq/queue`.
///
/// # Errors
/// Where the target names no host or no queue.
pub fn queue_url(target: &str) -> Result<String> {
    let (authority, queue) = if let Some(rest) = target.strip_prefix("msmq://") {
        rest.split_once('/').unwrap_or((rest, ""))
    } else {
        let stripped = target
            .strip_prefix("DIRECT=HTTP://")
            .or_else(|| target.strip_prefix("DIRECT=http://"))
            .or_else(|| target.strip_prefix("http://"))
            .ok_or_else(|| protocol_error(format!("{target:?} is not an MSMQ queue")))?;
        let (authority, path) = stripped.split_once('/').unwrap_or((stripped, ""));
        (authority, path.strip_prefix("msmq/").unwrap_or(""))
    };
    if authority.is_empty() || queue.is_empty() {
        return Err(protocol_error(format!(
            "{target:?} names no host and queue"
        )));
    }
    Ok(format!("http://{authority}/msmq/{queue}"))
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

    /// Bind, and take one message.
    fn receive(&self) -> Result<Vec<Arrived>> {
        let (listener, _) = self.bind()?;
        Ok(vec![self.accept_one(&listener)?])
    }

    /// POST the bytes as one message to the queue the target names.
    fn send(&self, target: &str, bytes: &[u8]) -> Result<()> {
        let url = queue_url(target)?;
        let request = self.compose(&url, bytes)?;
        let address = http::target::HttpTarget::parse(&url)?.address();
        let stream = socket::connect_tcp(&address, self.timeout)?;
        let response = message::exchange(stream, &request)?;
        if (200..300).contains(&response.status) {
            Ok(())
        } else {
            Err(TransportError::permanent(format!(
                "the queue answered {}: {}",
                response.status,
                response.text()
            )))
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

/// A bound listener waiting for its one POST.
struct Listening {
    transport: MsmqTransport,
    listener: TcpListener,
    address: String,
}

impl FarEnd for Listening {
    fn address(&self) -> &str {
        &self.address
    }

    fn take_one(self: Box<Self>) -> Result<Arrived> {
        self.transport.accept_one(&self.listener)
    }
}

impl Loopback for MsmqTransport {
    fn ceiling(&self) -> Option<usize> {
        Some(ceiling())
    }

    fn far_end(&self) -> Result<Box<dyn FarEnd>> {
        let (listener, address) = self.bind()?;
        Ok(Box::new(Listening {
            transport: self.sibling(&self.host),
            listener,
            address,
        }))
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
    fn a_stream_over_the_ceiling_is_refused_before_a_request_is_formed() {
        let near = MsmqTransport::new("127.0.0.1:0", "node-a");
        let over = vec![0u8; ceiling() + 1];
        let error = near
            .compose("http://node-b/msmq/orders", &over)
            .expect_err("over");
        assert!(!error.retryable);
        assert!(error.message.contains("4194304"), "{error}");
        let request = near
            .compose("http://node-b/msmq/orders", b"fits")
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
