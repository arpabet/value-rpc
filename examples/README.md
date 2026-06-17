<!--
  Copyright (c) 2025-2026 Karagatan LLC.
  SPDX-License-Identifier: BUSL-1.1
-->

# Examples

Each subfolder is a self-contained, runnable `package main`. Run any with:

```sh
go run ./examples/<name>/
```

### Getting started

| Example | Shows |
|---------|-------|
| [`first`](first/) | End-to-end demo of all four call patterns over TCP. |
| [`typed`](typed/) | Statically-typed call sites via `valuerpc.Codec[T]` + the generic `CallUnary` / `AddUnary` / `GetStreamTyped` helpers. |
| [`streaming`](streaming/) | Unary, server-stream, client-stream, and chat over one connection; a 10k-value server stream stays lossless under credit-based flow control. |
| [`cancellation`](cancellation/) | Context deadline / cancellation propagation — a slow handler observes the client's deadline on its own `ctx` and abandons work early. |

### Transports

| Example | Shows |
|---------|-------|
| [`unix`](unix/) | Unix-domain-socket transport with kernel peer authentication (`valuerpc.PeerCredOf` → uid/gid/pid). |
| [`websocket`](websocket/) | WebSocket transport as an embeddable `http.Handler` — vRPC and a plain HTTP route share one port. |
| [`mtls`](mtls/) | TLS transport with mutual auth; the verified client identity is read with `valuerpc.PeerCertificates` (certs generated in-memory). |
| [`customtransport`](customtransport/) | The bring-your-own-connection seam (`NewFuncDialer` / `NewAcceptListener`) — interpose any byte-stream layer (here a byte counter). |

### Production hardening

| Example | Shows |
|---------|-------|
| [`auth`](auth/) | Handshake `Authenticator` (bearer token → principal) and session resumption bound to the authenticated principal. |
| [`observability`](observability/) | Pluggable metrics (`valuerpc.Metrics`) on client and server, plus trace-context propagation (`WithMetadata` / `WithMetadataExtractor`). |
| [`reconnect`](reconnect/) | Reconnect policy: in-flight requests fail fast with `CodeUnavailable`, and the client auto-reconnects with exponential backoff after an outage. |
