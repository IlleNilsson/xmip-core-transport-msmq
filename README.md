# xmip-core-transport-msmq

MSMQ transport: one message is one Stream, carried as an SRMP envelope with its body attached over the HTTP transport MSMQ uses between machines. A technology of [xmip-core-transport](https://github.com/IlleNilsson/xmip-core-transport).

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
