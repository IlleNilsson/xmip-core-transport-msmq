# xmip-core-transport-msmq

MSMQ transport: one message is one Stream, carried as an SRMP envelope with its body attached over the HTTP transport MSMQ uses between machines. A technology of [xmip-core-transport](https://github.com/IlleNilsson/xmip-core-transport).

A queue's target is read by `net::Target` in [xmip-core-library-net](https://github.com/IlleNilsson/xmip-core-library-net) under the schemes this technology declares; `DIRECT=` is MSMQ's own and is taken off first. Until 2026-09-28 each form was matched by hand.

## Targets and HTTPS

A send target is `msmq://<host>/<queue>`, the queue's HTTP URL
`http://<host>/msmq/<queue>`, or MSMQ's format name
`DIRECT=HTTP://<host>/msmq/<queue>`. Each has a guarded form, `msmqs://`,
`https://` or `DIRECT=HTTPS://`, which is sent over HTTPS through the http
technology's endpoint, as as2, as4 and webdav are. TLS is the `tls` feature,
which turns on `xmip-core-transport-http`'s and so the estate's one TLS stack,
`xmip-core-library-tls` (ADR-0033). A build without it refuses an https queue
rather than write the message in the clear.

Requests go on connections kept between them (`http::endpoint::Connections`, offering HTTP/1.1): the transport holds them and hands them to every client it makes, so a call costs one exchange and not a connect, a TLS handshake and a `Connection: close`, as it did until 2026-09-27.

A Receive Location keeps its listener, bound on the first receive, and the connections senders keep open on it (`http::inbound::Inbound`): each receive takes the next request from whichever sends first, where until 2026-09-27 each receive bound a listener of its own, answered one request with `Connection: close`, and refused a request that came between two receives.

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
