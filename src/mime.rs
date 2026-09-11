//! The MIME shape an SRMP message travels in: `multipart/related`, the
//! envelope first and the body attached after it.
//!
//! MS-MQSRM section 2.2.4: the HTTP body is `multipart/related` with
//! `type="text/xml"`, its first part the SOAP envelope, each further part
//! a binary attachment with a `Content-Id` the envelope refers to by
//! `cid:`. The attachment is `application/octet-stream` and travels as the
//! bytes it is, so what the queue hands on is exactly the Stream.

use transport::error::{Result, protocol_error};

/// One part of a multipart body: its headers and its bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Part {
    pub content_type: String,
    pub content_id: String,
    pub bytes: Vec<u8>,
}

/// The `Content-Type` an SRMP message travels under, with `boundary`.
#[must_use]
pub fn content_type(boundary: &str) -> String {
    format!("multipart/related; boundary=\"{boundary}\"; type=\"text/xml\"")
}

/// The boundary a `Content-Type` names, or the refusal.
///
/// # Errors
/// Where the type is not `multipart/related` or names no boundary.
pub fn boundary_of(content_type: &str) -> Result<String> {
    let mut parts = content_type.split(';').map(str::trim);
    if !parts
        .next()
        .is_some_and(|kind| kind.eq_ignore_ascii_case("multipart/related"))
    {
        return Err(protocol_error(format!(
            "an SRMP message is multipart/related, not {content_type:?}"
        )));
    }
    parts
        .find_map(|parameter| {
            let (name, value) = parameter.split_once('=')?;
            name.trim()
                .eq_ignore_ascii_case("boundary")
                .then(|| value.trim().trim_matches('"').to_string())
        })
        .filter(|boundary| !boundary.is_empty())
        .ok_or_else(|| protocol_error("a multipart type naming no boundary"))
}

/// `parts` as one multipart body under `boundary`.
#[must_use]
pub fn compose(boundary: &str, parts: &[Part]) -> Vec<u8> {
    let mut out = Vec::new();
    for part in parts {
        out.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        out.extend_from_slice(
            format!(
                "Content-Type: {}\r\nContent-Id: <{}>\r\nContent-Length: {}\r\n\r\n",
                part.content_type,
                part.content_id,
                part.bytes.len()
            )
            .as_bytes(),
        );
        out.extend_from_slice(&part.bytes);
        out.extend_from_slice(b"\r\n");
    }
    out.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
    out
}

/// The parts of a multipart body under `boundary`.
///
/// # Errors
/// Where a part has no header block, or the body does not close.
pub fn parse(boundary: &str, body: &[u8]) -> Result<Vec<Part>> {
    let open = format!("--{boundary}");
    let mut parts = Vec::new();
    let mut at = find(body, 0, open.as_bytes())
        .ok_or_else(|| protocol_error("a multipart body with no first boundary"))?;
    loop {
        at += open.len();
        if body[at..].starts_with(b"--") {
            return Ok(parts);
        }
        let head_start = at + skip_eol(&body[at..]);
        let head_end = find(body, head_start, b"\r\n\r\n")
            .ok_or_else(|| protocol_error("a part with no header block"))?;
        let head = String::from_utf8_lossy(&body[head_start..head_end]).to_string();
        let content_start = head_end + 4;
        let next = find(body, content_start, open.as_bytes())
            .ok_or_else(|| protocol_error("a multipart body that does not close"))?;
        let mut content_end = next;
        if body[..content_end].ends_with(b"\r\n") {
            content_end -= 2;
        }
        parts.push(Part {
            content_type: header(&head, "Content-Type").unwrap_or_default(),
            content_id: header(&head, "Content-Id")
                .map(|id| id.trim_matches(|c| c == '<' || c == '>').to_string())
                .unwrap_or_default(),
            bytes: body[content_start..content_end].to_vec(),
        });
        at = next;
    }
}

fn find(haystack: &[u8], from: usize, needle: &[u8]) -> Option<usize> {
    haystack
        .get(from..)?
        .windows(needle.len())
        .position(|window| window == needle)
        .map(|at| at + from)
}

fn skip_eol(bytes: &[u8]) -> usize {
    if bytes.starts_with(b"\r\n") {
        2
    } else {
        usize::from(bytes.starts_with(b"\n"))
    }
}

fn header(head: &str, name: &str) -> Option<String> {
    head.lines().find_map(|line| {
        let (key, value) = line.split_once(':')?;
        key.trim()
            .eq_ignore_ascii_case(name)
            .then(|| value.trim().to_string())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_composed_body_parses_back_with_binary_attachments_whole() {
        let parts = vec![
            Part {
                content_type: "text/xml".into(),
                content_id: "envelope".into(),
                bytes: b"<se:Envelope/>".to_vec(),
            },
            Part {
                content_type: "application/octet-stream".into(),
                content_id: "body@xmip".into(),
                bytes: b"\r\n--x\r\n\x00\xff".to_vec(),
            },
            Part {
                content_type: "application/octet-stream".into(),
                content_id: "empty".into(),
                bytes: Vec::new(),
            },
        ];
        let body = compose("MSMQ_BOUNDARY", &parts);
        assert_eq!(parse("MSMQ_BOUNDARY", &body).expect("parsing"), parts);
    }

    #[test]
    fn the_boundary_is_read_from_the_type_and_a_wrong_type_is_refused() {
        assert_eq!(boundary_of(&content_type("abc")).expect("boundary"), "abc");
        assert_eq!(
            boundary_of("Multipart/Related; type=text/xml; boundary=xyz").expect("boundary"),
            "xyz"
        );
        assert!(!boundary_of("text/xml").expect_err("wrong").retryable);
        assert!(boundary_of("multipart/related").is_err());
        assert!(parse("b", b"no boundary here").is_err());
        assert!(parse("b", b"--b\r\nContent-Type: x\r\n\r\nnever closes").is_err());
    }
}
