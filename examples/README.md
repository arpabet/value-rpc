<!--
  Copyright (c) 2025-2026 Karagatan LLC.
  SPDX-License-Identifier: BUSL-1.1
-->

# Examples

Each subfolder is a self-contained, runnable `package main` (`go run ./examples/<name>/`).

| Example | Shows |
|---------|-------|
| [`first`](first/) | End-to-end demo of all four call patterns over TCP. |
| [`typed`](typed/) | Statically-typed call sites via `valuerpc.Codec[T]` + the generic `CallUnary` / `AddUnary` / `GetStreamTyped` helpers. |
| [`streaming`](streaming/) | Unary, server-stream, client-stream, and chat over one connection; a 10k-value server stream stays lossless under credit-based flow control. |
| [`observability`](observability/) | Pluggable metrics (`valuerpc.Metrics`) on client and server, plus trace-context propagation (`WithMetadata` / `WithMetadataExtractor`). |
| [`mtls`](mtls/) | TLS transport with mutual auth; the verified client identity is read with `valuerpc.PeerCertificates` (certs generated in-memory). |
| [`reconnect`](reconnect/) | Reconnect policy: in-flight requests fail fast with `CodeUnavailable`, and the client auto-reconnects with exponential backoff after an outage. |

```sh
go run ./examples/typed/
go run ./examples/streaming/
go run ./examples/observability/
go run ./examples/mtls/
go run ./examples/reconnect/
```
