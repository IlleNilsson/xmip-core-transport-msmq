//! The SRMP envelope: what MSMQ says about a message when it sends it
//! over HTTP.
//!
//! MS-MQSRM section 2.2: a SOAP 1.1 envelope whose header carries the
//! WS-Routing `path` — the action, the destination queue, the message id
//! — and the SRMP `properties`, and whose body names the attachment the
//! message body travels as. The envelope is composed and read here as the
//! flat scan the capability's `xml.rs` is (ADR-0044); a queue reads four
//! elements out of it and never needs a tree.

use codec::xml::escape;
use transport::error::{Result, protocol_error};
use transport::xml::first;

/// The SOAP envelope namespace, as SRMP fixes it.
pub const SOAP: &str = "http://schemas.xmlsoap.org/soap/envelope/";
/// The WS-Routing namespace the `path` header is in.
pub const ROUTING: &str = "http://schemas.xmlsoap.org/rp/";
/// The SRMP namespace the `properties` header is in.
pub const SRMP: &str = "http://schemas.xmlsoap.org/srmp/";
/// The MSMQ namespace the `Msmq` header is in.
pub const MSMQ: &str = "msmq.namespace.xml";
/// The one action MSMQ sends a message with.
pub const ACTION: &str = "MSMQ:default";

/// What an envelope says.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Envelope {
    /// The message id, `uuid:<n>@<host>` as MSMQ forms it.
    pub id: String,
    /// The destination queue as a URL, `http://<host>/msmq/<queue>`.
    pub to: String,
    /// The attachment the body travels as, its `Content-Id`.
    pub body_id: String,
    /// When the message was sent, seconds since 1970.
    pub sent_at: u64,
    /// When the message expires, seconds since 1970.
    pub expires_at: u64,
}

impl Envelope {
    /// The envelope for a message to `to`, its body attached as `body_id`.
    #[must_use]
    pub fn new(id: &str, to: &str, body_id: &str, sent_at: u64) -> Self {
        Self {
            id: id.to_string(),
            to: to.to_string(),
            body_id: body_id.to_string(),
            sent_at,
            expires_at: sent_at.saturating_add(u64::from(u32::MAX)),
        }
    }

    /// The queue the destination URL names: what follows `/msmq/`.
    #[must_use]
    pub fn queue(&self) -> &str {
        self.to
            .split_once("/msmq/")
            .map_or("", |(_, queue)| queue.trim_end_matches('/'))
    }
}

/// `envelope` as the XML text SRMP sends.
#[must_use]
pub fn compose(envelope: &Envelope) -> String {
    format!(
        "<?xml version=\"1.0\"?>\
         <se:Envelope xmlns:se=\"{SOAP}\">\
         <se:Header>\
         <path xmlns=\"{ROUTING}\" se:mustUnderstand=\"1\">\
         <action>{ACTION}</action><to>{}</to><id>{}</id>\
         </path>\
         <properties xmlns=\"{SRMP}\" se:mustUnderstand=\"1\">\
         <expiresAt>{}</expiresAt><sentAt>{}</sentAt>\
         </properties>\
         <Msmq xmlns=\"{MSMQ}\"><Class>0</Class><Priority>3</Priority>\
         <Journal>none</Journal><BodyType>0</BodyType></Msmq>\
         </se:Header>\
         <se:Body>\
         <Msmq xmlns=\"{MSMQ}\"><Body href=\"cid:{}\"/></Msmq>\
         </se:Body>\
         </se:Envelope>",
        escape(&envelope.to),
        escape(&envelope.id),
        envelope.expires_at,
        envelope.sent_at,
        escape(&envelope.body_id),
    )
}

/// The envelope `xml` carries, or why it is not one.
///
/// # Errors
/// Where the text is not an SRMP envelope: no `path`, no `to`, no `id`,
/// an action other than [`ACTION`], or a body that names no attachment.
pub fn parse(xml: &str) -> Result<Envelope> {
    if !xml.contains("<se:Envelope") && !xml.contains(":Envelope") {
        return Err(protocol_error("not a SOAP envelope"));
    }
    match first(xml, "action")? {
        Some(action) if action == ACTION => {}
        Some(action) => {
            return Err(protocol_error(format!(
                "an action this queue does not take: {action}"
            )));
        }
        None => return Err(protocol_error("an envelope with no path header")),
    }
    let to = first(xml, "to")?.ok_or_else(|| protocol_error("an envelope with no destination"))?;
    let id = first(xml, "id")?.ok_or_else(|| protocol_error("an envelope with no message id"))?;
    let body_id = xml
        .split("href=\"cid:")
        .nth(1)
        .and_then(|rest| rest.split('"').next())
        .map(codec::xml::unescape)
        .transpose()?
        .ok_or_else(|| protocol_error("a body that names no attachment"))?;
    Ok(Envelope {
        id,
        to,
        body_id,
        sent_at: number(xml, "sentAt")?,
        expires_at: number(xml, "expiresAt")?,
    })
}

fn number(xml: &str, name: &str) -> Result<u64> {
    Ok(first(xml, name)?
        .and_then(|text| text.parse().ok())
        .unwrap_or(0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_composed_envelope_parses_back_and_names_its_queue() {
        let envelope = Envelope::new(
            "uuid:7@node-a",
            "http://node-b/msmq/orders",
            "body@node-a",
            1_800_000_000,
        );
        let xml = compose(&envelope);
        assert!(xml.contains("<action>MSMQ:default</action>"));
        assert!(xml.contains("se:mustUnderstand=\"1\""));
        assert_eq!(parse(&xml).expect("parsing"), envelope);
        assert_eq!(envelope.queue(), "orders");
        assert_eq!(envelope.expires_at, 1_800_000_000 + u64::from(u32::MAX));
    }

    #[test]
    fn what_is_not_an_srmp_envelope_is_refused_for_the_reason_it_is_not() {
        assert!(parse("<order/>").is_err(), "no envelope");
        let other = compose(&Envelope::new("uuid:1@a", "http://b/msmq/q", "x", 0))
            .replace("MSMQ:default", "MSMQ:ack");
        let error = parse(&other).expect_err("another action");
        assert!(error.message.contains("MSMQ:ack"));
        assert!(!error.retryable);
        let no_body = compose(&Envelope::new("uuid:1@a", "http://b/msmq/q", "x", 0))
            .replace("href=\"cid:x\"", "");
        assert!(parse(&no_body).is_err(), "no attachment");
    }
}
