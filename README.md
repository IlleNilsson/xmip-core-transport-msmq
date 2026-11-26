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

A Receive Location keeps its listener, bound as its transport is opened (`Configured::listening`; on its first receive until 2026-11-16), and the connections senders keep open on it (`http::inbound::Inbound`): each receive takes the next request from whichever sends first, where until 2026-09-27 each receive bound a listener of its own, answered one request with `Connection: close`, and refused a request that came between two receives. It binds through `http::server::listen` at the queue address's authority: `msmq://` in the clear, `msmqs://` refused as asking for TLS, which no receive riding on HTTP terminates yet (ADR-0033, amendment 2026-11-13). What is not an SRMP message is answered `400` at once, its caller turned away, and the receive goes on for the others (ADR-0018, amendment 2026-11-13).

A sender is answered after the runtime's whole receive cycle, not as its message is read: it waits on its connection (`http::inbound::Reply`) for `200` when the cycle accepted the message — as an MSMQ queue's HTTP end answers; `401`, `403` or `422` when it refused it (`http::server::refused`), a client error the sender does not send again; or `503` when Xmip could not complete the cycle, so the sender keeps the message and sends it again. A POST that is not an SRMP message is answered `400` at once. The answer is the one the request always had, written later: no added round trip. Until 2026-10-02 a message was answered `200` as it was read.

## The deduplication key

A keyed send (the key `Transport::send_from` is handed, `Outgoing::key`; built 2026-10-04) carries the Journey's identifier in the SRMP envelope's message id, `uuid:<key>@<host>` (`MsmqTransport::compose`), the same on every attempt on one Journey and from every node, `host` being the Location's configured name. A receiving queue manager and its consumer tell a message sent again from one held by that id. An unkeyed `send` is numbered by its sender, `uuid:<n>@<host>`, as before.

## The declaration

What a Location of this technology is configured with is two `const`s in its `declaration` module, `src/declaration.rs`, beside the code, which `Configured` reads (ADR-0064, amendment 2026-11-26): `declaration::SETTINGS`, `host` and `timeout`, and `SCHEMES`, `msmq` and `msmqs`; the `Configured` impl that reads the declaration is `src/configured.rs`, which held the declaration as `settings.rs` until 2026-11-26. Built with its `declaration` feature alone (`--no-default-features --features declaration`), the crate is those and nothing more: its code and every crate the code is built on — the http technology, and with it TLS — are behind the default feature, and `tls`, which is code, turns the default on with it. So the library the surfaces load carries it without the code, through the gathering crate xmip-core-declaration (open problem 48), and the language server and the desktop suggest and check an MSMQ Location's settings though no node in their process loaded the technology. `test/Declaration.Test.ps1` in the estate holds that the declaration builds nothing the transport capability does not.

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
