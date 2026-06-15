# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/), and the project aims to follow
[Semantic Versioning](https://semver.org/). Bug IDs (BUG-N) reference
[FINDINGS.md](FINDINGS.md); transport design is in [TRANSPORTS.md](TRANSPORTS.md).

## [Unreleased]

### Added

- **TLS / mutual-TLS transport** (`tls://`). `valueserver.NewTLSServer` and
  `valueclient.NewTLSClient` take a `*tls.Config` and reuse the length-prefix
  framing (a `*tls.Conn` is a `net.Conn`). Mutual TLS via `ClientAuth` +
  `ClientCAs`; the verified client certificate is exposed through
  `valuerpc.PeerCertificates` for use in a connect-authorizer — the network
  analogue of Unix peer credentials. The `tls://` scheme works through plain
  `NewClient` (system root CAs).
- **In-memory transport** (`mem://`). `valueserver.NewMemServer(name)` /
  `valueclient.NewMemClient(name)` connect a client and server in the same
  process over Go channels (a process-wide name registry); messages pass by
  reference — no sockets, no MessagePack. For deterministic tests and for
  composing a monolith that can later split onto a real socket by changing only
  the address.
- **QUIC transport** in a **separate module**, `go.arpabet.com/value-rpc/quic`
  (`valuequic.NewServer` / `valuequic.NewClient`), on
  `github.com/quic-go/quic-go`. Kept out of the core module so only programs that
  use QUIC pull in that heavyweight dependency — the core requires just `value`,
  `zap`, `golang.org/x/net`, `golang.org/x/sys`, and `coder/websocket`. QUIC
  mandates TLS (mutual TLS + `valuerpc.PeerCertificates` supported); each RPC
  request maps to its own QUIC stream (independent flow control; streams freed
  when both halves finish), plus TLS 1.3, 0-RTT, and connection migration.
  Seam-fit integration: inbound frames funnel through the existing per-connection
  read loop, so application-level slow-consumer head-of-line blocking is reduced,
  not eliminated (see TRANSPORTS.md §9). To expose the TLS state across packages,
  the `PeerCertificates` hook is now the exported `valuerpc.TLSStateConn`
  interface (`TLSConnectionState`).
- **Transport candidates research** — [TRANSPORTS.md](TRANSPORTS.md) §9 surveys
  further transports (QUIC, vsock, named pipes, stdio, in-memory, WebTransport,
  NATS, …) with a fit/effort table and recommendations.
- **Transport matrix tests** — all four interaction patterns exercised over every
  transport (TCP, Unix socket, WebSocket) in one table-driven test.
- Runnable, output-checked **godoc examples for Unix sockets and WebSocket**
  (`Example_unixSocket`, `Example_webSocket`), alongside the existing
  pattern examples.
- Unit tests for WebSocket address parsing (`splitWSPath`) and the WebSocket
  read-limit (`MaxFrameSize`) enforcement.

### Changed

- **README** and **RESEARCH.md** rewritten to document all three transports
  (intro, architecture diagram, features, dependencies, configuration) instead
  of describing the library as TCP-only.
- **Dependencies updated** to latest: `github.com/coder/websocket` v1.8.15,
  `golang.org/x/net` v0.56.0 (`value` v1.2.0, `quic-go` v0.60.0, `zap` v1.28.0
  and the rest already current).

### Fixed

- `valueserver.Server.Close()` no longer hangs when `Run()` was never called —
  the accept loop was over-counted in the shutdown `WaitGroup`, which now tracks
  only connection-handler goroutines.

## [1.2.0] — 2026-06-14

The first hardened, openly-licensed, multi-transport release. It bundles a
permissive relicense, a Go 1.25 / `value` v1.2.0 upgrade, fixes for 15
correctness/crash/DoS/lifecycle bugs, three pluggable transports (TCP, Unix
sockets, WebSocket), and the project's first test suite, benchmarks, and CI.

The MessagePack message format is unchanged — a peer on the previous TCP build
interoperates with this one. Review **Breaking changes** before upgrading.

### License

- **Relicensed from BUSL-1.1 to [Apache-2.0](LICENSE)** (OSI-approved, with a
  patent grant). Copyright remains © Karagatan LLC. Updated `LICENSE` and the
  SPDX header of every source file.

### Added

- **Pluggable transport layer.** A small seam — `valuerpc.Listener`,
  `valuerpc.Dialer`, `valuerpc.MsgConn` — decouples the RPC layer from the wire.
  New constructors `valueserver.NewServerWithListener` and
  `valueclient.NewClientWithDialer` accept any transport; `NewServer` / `NewClient`
  now parse an address scheme (a bare `host:port` is TCP).
- **Unix domain socket transport.** `unix://` scheme,
  `valueserver.NewUnixServer` / `valueclient.NewUnixClient`, stale-socket-file
  cleanup on bind (refuses to clobber a non-socket), and **peer authentication**:
  `valuerpc.PeerCred` / `valuerpc.PeerCredOf` read the kernel-reported peer
  uid/gid/pid (`SO_PEERCRED` on Linux, `LOCAL_PEERCRED` on macOS), surfaced via
  `valueserver.Server.SetConnectAuthorizer`.
- **WebSocket (MessagePack) transport** on `github.com/coder/websocket`: one
  MessagePack message per binary frame (no length prefix), ping/pong keepalive.
  `valueserver.NewWebSocketServer` (standalone) and `NewWebSocketHandler`
  (an `http.Handler` to mount on your own mux for port-sharing and `wss://` via
  your own TLS server); `valueclient.NewWebSocketClient`; `ws://`/`wss://` scheme
  support.
- `valueserver.Server.Addr() net.Addr` — the bound address (handy with `:0`).
- **Bounded frames**: `valuerpc.MaxFrameSize` (default 16 MiB; maps to the
  WebSocket read limit) caps inbound message size.
- TCP keepalive, a handshake read deadline (slowloris protection), capped
  `Accept` backoff, and a graceful `Close()` that drains in-flight connections.
- The project's **first tests** (unit, integration, transport matrix),
  **benchmarks**, runnable **godoc examples**, a **GitHub Actions CI** workflow
  (build/vet/race on a Go 1.25/1.26 matrix, benchmarks, `govulncheck`), and
  `Makefile` `vet`/`test`/`vuln` targets.
- Documentation: [FINDINGS.md](FINDINGS.md), [RESEARCH.md](RESEARCH.md),
  [TRANSPORTS.md](TRANSPORTS.md), and a complete README.

### Changed

- **Requires Go 1.25** (was 1.17).
- Upgraded `go.arpabet.com/value` v1.1.1 → v1.2.0; bumped `zap`, `golang.org/x/net`,
  `golang.org/x/crypto`, `shopspring/decimal`.
- Replaced the `github.com/smallnest/goframe` framing dependency with an internal
  bounded length-prefix codec (**same wire format**).
- Migrated `go.uber.org/atomic` → the standard library `sync/atomic` (Go 1.25
  generic atomics); `atomic.Error` became `atomic.Pointer[error]`.
- New dependencies: `github.com/coder/websocket` (WebSocket transport),
  `golang.org/x/sys` (peer credentials).

### Fixed

Highlights (full list and patches in [FINDINGS.md](FINDINGS.md)):

- **Crashes (panics):** `Chat` double-closed its response channel (BUG-7);
  shutdown/reconnect races sent on closed channels (BUG-3); `sync.Cond.Wait` was
  called without holding its lock (BUG-17).
- **Correctness:** the handshake version was read from the wrong field (BUG-1);
  `Void` functions rejected `nil` args, breaking the example (BUG-2); a phantom
  `value.Null` was delivered at end-of-stream and for void unary results (BUG-4);
  dead `== nil` guards let malformed messages be mis-routed as a handshake or
  request 0 (BUG-5).
- **Robustness / DoS:** unbounded frame length allowed an OOM (BUG-11); no
  read/idle timeout permitted slowloris and leaked goroutines (BUG-10);
  `Accept()` spun at 100% CPU on a persistent error (BUG-12); a reconnect started
  a second sender goroutine and could reorder/lose messages (BUG-8, BUG-9); the
  `canceledRequests` map leaked entries (BUG-13).
- **Lifecycle:** there was no graceful shutdown; in-flight connections were not
  drained (BUG-14). A panicking user handler can no longer crash the server.

### Security

- Apache-2.0 patent grant.
- Decoder hardened: a bounded frame size plus the `value` v1.2.0 decode limits
  prevent a tiny header from forcing a huge allocation.
- Unix-socket **peer-credential authorization** for local trust decisions.
- `wss://` (TLS) via the embedded WebSocket handler on your own TLS server.

### Breaking changes

- **License** changed from BUSL-1.1 to Apache-2.0.
- **`valuerpc.MsgConn`** dropped `Conn() net.Conn` and gained
  `SetReadDeadline(time.Time) error` and `RemoteAddr() string`.
- **`valueserver.Server`** gained `Addr()` and `SetConnectAuthorizer(...)`
  (affects external implementers only; the wire protocol is unchanged).
- Inbound messages larger than `valuerpc.MaxFrameSize` (default **16 MiB**) are
  now rejected — raise it (or set `0` to disable) if you send larger payloads.
- A client must complete the handshake within `valueserver.HandshakeTimeout`
  (default 10s).
- The `smallnest/goframe` and `go.uber.org/atomic` dependencies were removed.
- `value.Number.Equal` is now strict in the upgraded `value` lib (no cross-type
  coercion); value-rpc does not rely on it, but custom code might.

### Still open (by design)

`context.Context` propagation to handlers, server-side SLA/deadline enforcement,
TLS-over-TCP / mTLS, and a fully async per-request demux to remove slow-consumer
head-of-line blocking. See [RESEARCH.md](RESEARCH.md) §5.

## [1.1.1] — 2025-01-12

Earlier BUSL-1.1, TCP-only releases (1.0.0, 2022-06-19, and 1.1.x). Detailed
notes begin at 1.2.0; see the git history for older changes.

[Unreleased]: https://github.com/arpabet/value-rpc/compare/v1.2.0...HEAD
[1.2.0]: https://github.com/arpabet/value-rpc/releases/tag/v1.2.0
[1.1.1]: https://github.com/arpabet/value-rpc/releases/tag/v1.1.1
