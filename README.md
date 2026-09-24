# xmip-core-transport-msmq

MSMQ transport: one message is one Stream, carried as an SRMP envelope with its body attached over the HTTP transport MSMQ uses between machines. A technology of [xmip-core-transport](https://github.com/IlleNilsson/xmip-core-transport).

## Targets and HTTPS

A send target is `msmq://<host>/<queue>`, the queue's HTTP URL
`http://<host>/msmq/<queue>`, or MSMQ's format name
`DIRECT=HTTP://<host>/msmq/<queue>`. Each has a guarded form, `msmqs://`,
`https://` or `DIRECT=HTTPS://`, which is sent over HTTPS through the http
technology's endpoint, as as2, as4 and webdav are. TLS is the `tls` feature,
which turns on `xmip-core-transport-http`'s and so the estate's one TLS stack,
`xmip-core-library-tls` (ADR-0033). A build without it refuses an https queue
rather than write the message in the clear.

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
