//! The MIME shape an SRMP message travels in: `multipart/related`, the
//! envelope first and the body attached after it.
//!
//! MS-MQSRM section 2.2.4: the HTTP body is `multipart/related` with
//! `type="text/xml"`, its first part the SOAP envelope, each further part
//! a binary attachment with a `Content-Id` the envelope refers to by
//! `cid:`. The attachment is `application/octet-stream` and travels as the
//! bytes it is, so what the queue hands on is exactly the Stream. The
//! multipart body itself is `codec::mime`'s, the estate's one; until
//! 2026-09-24 this file wrote and read its own, and took a boundary in the
//! middle of a line for a delimiter.

use codec::mime::{self, Part};
use transport::error::{Result, protocol_error};

/// The `Content-Type` an SRMP message travels under, with `boundary`.
#[must_use]
pub fn content_type(boundary: &str) -> String {
    format!("multipart/related; boundary=\"{boundary}\"; type=\"text/xml\"")
}

/// One part as SRMP writes it: its type, its content id and its length.
#[must_use]
pub fn part(content_type: &str, content_id: &str, bytes: &[u8]) -> Part {
    Part::new(bytes)
        .header("Content-Type", content_type)
        .header("Content-Id", &format!("<{content_id}>"))
        .header("Content-Length", &bytes.len().to_string())
}

/// The parts of an SRMP body under `boundary`.
///
/// # Errors
/// Where the body is not the multipart its boundary says.
pub fn parts(boundary: &str, body: &[u8]) -> Result<Vec<Part>> {
    mime::read(body, boundary).map_err(|refusal| protocol_error(refusal.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_written_body_parses_back_with_binary_attachments_whole() {
        let written = vec![
            part("text/xml", "envelope", b"<se:Envelope/>"),
            part(
                "application/octet-stream",
                "body@xmip",
                b"\r\n--x\r\n\x00\xff",
            ),
            part("application/octet-stream", "empty", b""),
        ];
        let body = mime::write("MSMQ - SOAP boundary, 12345", &written);
        let back = parts("MSMQ - SOAP boundary, 12345", &body).expect("parsing");
        assert_eq!(back, written);
        assert_eq!(back[1].content_id(), Some("body@xmip"));
        assert_eq!(back[1].header_value("content-length"), Some("9"));
    }

    #[test]
    fn the_type_written_declares_its_boundary_and_a_broken_body_is_refused() {
        let written = content_type("abc");
        assert_eq!(
            mime::boundary_of(&written, "multipart/related").expect("boundary"),
            "abc"
        );
        assert!(parts("b", b"no boundary here").is_err());
        assert!(parts("b", b"--b\r\nContent-Type: x\r\n\r\nnever closes").is_err());
    }
}
