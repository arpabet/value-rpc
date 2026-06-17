# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/), and the project aims to follow
[Semantic Versioning](https://semver.org/). Bug IDs (BUG-N) reference
[FINDINGS.md](FINDINGS.md); transport design is in [TRANSPORTS.md](TRANSPORTS.md).

## [Unreleased]

### Security

- **Session resumption is now bound to the authenticated principal.** The
  `valueserver.Authenticator` hook returns a `principal` identity
  (`func(conn, credential) (principal string, err error)` — **breaking**: it
  previously returned only `error`). A reconnect that presents a valid session
  token but authenticates as a *different* principal is rejected, so a leaked
  token alone cannot let one principal take over another's session (defence in
  depth atop the session-token anti-hijack). With no `Authenticator` set the
  principal is "" for all and behaviour is unchanged. Test:
  `valueserver.TestResumptionBoundToPrincipal`.

### Changed

- Error messages and protocol-error responses no longer embed the offending
  request/argument payload by default — only the function name and message type,
  which identify the call without leaking its data. Set
  `valueserver.DebugPayloadInErrors = true` to restore full payloads for local
  debugging.
- A handler that returns a coded `*valuerpc.Error` no longer has its code prefix
  duplicated in the error message once the client rebuilds the error from the wire.
- Added a suite of runnable feature examples under `examples/` (typed, streaming,
  cancellation, unix, websocket, mtls, customtransport, auth, observability,
  reconnect) with an index, and moved the original demo to `examples/first/`.

### Removed

- The deprecated `valuerpc.ThrottleIncrease` / `ThrottleDecrease` message types
  (superseded by `StreamCredit` flow control). Their wire numbers are reserved so
  `StreamCredit` and other message-type values are unchanged.
- Exported WebSocket globals `valuerpc.WSKeepAlive` (unused — the ping interval is
  the per-instance `WithKeepAlivePeriod`) and `valuerpc.WSDialTimeout` (the dial
  bound is the per-instance `valueclient.WithDialTimeout`; the handshake fallback is
  now an internal constant). Both were superseded once transport config moved to
  options.

### Changed

- The wire-protocol field names and markers (`valuerpc.MessageTypeField`, `Magic`,
  `Version`, `HandshakeRequestId`, …) are now `const` instead of `var`, so the
  protocol contract cannot be mutated at runtime.

### Tests

- Raised `valueclient` package test coverage from ~7% to ~79% (call patterns,
  credit-based flow control, options, metrics, metadata, reconnect/backoff, error
  handler, credential auth, lifecycle) and added fuzz targets
  `valuerpc.FuzzReadMessage` / `FuzzUnpack` proving the length-prefix + msgpack
  decode path never panics on adversarial input.

### Added

- **Typed-client ergonomics.** New generic helpers give statically-typed call
  sites over the schemaless wire without codegen: `valuerpc.Codec[T]` (an explicit
  per-type encode/decode), `valueclient.CallUnary` / `GetStreamTyped` /
  `PutStreamTyped`, and `valueserver.AddUnary` / `AddOutgoingStreamTyped`. A worked
  end-to-end example (`valueclient` `TestTypedService`) shows the recommended
  hand-written typed-facade pattern.
- **Reconnect policy for in-flight requests.** Previously a reconnect orphaned
  in-flight requests (they hung until their timeout). Now, by default, they are
  **failed fast** with `valueclient.ErrConnectionLost` (`CodeUnavailable`) the
  moment the connection drops and is re-established. Opt in to **replaying
  idempotent unary** calls on the new connection with
  `valueclient.WithReconnectPolicy(ReconnectPolicy{ReplayUnary: func(method string) bool { … }})`
  — matching calls are re-sent (keeping their original deadline budget) while
  streams and non-idempotent unary calls are still failed fast. Replay is
  at-least-once. The policy also drives **automatic reconnect backoff**: on a drop
  the client retries the dial with exponential backoff (`InitialBackoff` →
  `MaxBackoff`, optional `Jitter`, `MaxAttempts` with `<0` = forever), so it
  re-establishes the connection on its own after an outage instead of waiting for
  the next request; the backoff sleep wakes immediately on `Close`. Tests:
  `valueserver.TestReconnectFailFast`, `TestReconnectReplayIdempotent`,
  `TestReconnectBackoff`.

- **Metadata / trace-context propagation.** Requests now carry a string→string
  metadata map (new `md` envelope field) for distributed-trace context and
  baggage. The client injects it per request from the call's context
  (`valueclient.WithMetadata(func(ctx) valuerpc.Metadata)`); the server surfaces
  it on the handler's context (`valuerpc.MetadataFromContext(ctx)`) and can turn
  it into a propagated context via `valueserver.WithMetadataExtractor` — the
  dependency-free seam for OpenTelemetry/W3C `traceparent` (carrier helpers
  `valuerpc.EncodeMetadata`/`DecodeMetadata`, `Metadata` type alias). Test:
  `valueserver.TestMetadataPropagation`.
- **Pluggable metrics (`valuerpc.Metrics`).** A small event interface
  (`RequestBegin`/`RequestEnd(method, code, elapsed)`/`StreamValue`/`Reconnect`,
  with a `NopMetrics` default) feeds request/error counters (grouped by
  `valuerpc.Code`), an in-flight gauge (begin−end), per-call latency, reconnect
  counts, and stream throughput to Prometheus/OpenTelemetry/etc. Install on the
  client with `valueclient.WithMetrics(m)` and on the server with
  `valueserver.WithMetrics(m)`; `RequestBegin` fires when a call/stream starts and
  `RequestEnd` once at teardown (so the gauge spans a stream's whole lifetime).
  On the server, unary requests are metered synchronously and streams via
  `newServingRequest`/`deleteRequest` (the single idempotent retirement point);
  `Reconnect` is client-only. The client's `reconnects` stat is now actually
  incremented. Tests: `valueserver.TestClientMetrics`, `TestServerMetrics`.
- **Context-aware, bounded dial.** The `valuerpc.Dialer` interface is now
  `Dial(ctx context.Context)`, and the client gained `ConnectContext(ctx)`. The
  dial honours the context's deadline/cancellation; when the context carries no
  deadline a configurable dial timeout applies (`valueclient.WithDialTimeout`,
  default 30s via `DefaultDialTimeout`), so `Connect()` can no longer block on the
  OS connect timeout to an unreachable peer. Implicit connect-on-first-call uses
  the call's context. Stream dialers use `net.Dialer.DialContext` /
  `tls.Dialer.DialContext`; the bring-your-own `NewFuncDialer` connect func now
  takes a `context.Context`. Tests: `valueserver.TestConnectContextCanceled`,
  `TestConnectDialTimeout`.
- **Pluggable client logger (`*zap.Logger`).** The client no longer uses stdlib
  `log`; connection, reconnect, and protocol diagnostics now go through an
  injected `*zap.Logger` — the same structured logger glue applications already
  pass to `valueserver`. Set it with `valueclient.WithLogger(logger)`; the default
  is a no-op logger (silent), so the library never writes to stderr unbidden.
  (Already on the latest `go.uber.org/zap` v1.28.0.) Test:
  `valueserver.TestClientLogger`.
- **Machine-readable error codes.** Errors now carry a `valuerpc.Code` on the wire
  (a new `code` field): `CodeNotFound`, `CodeInvalidArgument`, `CodeResourceExhausted`,
  `CodeUnavailable`, `CodeDeadlineExceeded`, `CodeUnauthenticated`, `CodeInternal`,
  etc. Server-classified failures (unknown function → NotFound, bad args →
  InvalidArgument, caps/overrun → ResourceExhausted) and handler-supplied codes
  (`valuerpc.NewError(code, …)`) round-trip to the caller as a typed
  `*valuerpc.Error`; branch with `valuerpc.CodeOf(err)` or `errors.As` instead of
  string-matching. `valueserver.FunctionError` gained a leading `code` argument.
  Test: `valueserver.TestErrorCodes`.
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
- **Bring-your-own-connection seam.** `valuerpc.NewMsgConn` now accepts any
  `io.ReadWriteCloser` (not only `net.Conn`), so non-socket streams — pluggable /
  obfuscated transports, `ssh.Channel`, WebRTC data channels — can carry the RPC
  protocol. New adapters `NewFuncDialer`, `NewSingleConnDialer`, `NewAcceptListener`,
  and `NewSingleConnListener` (`valuerpc/transport_conn.go`) turn an externally
  established connection into a `Dialer`/`Listener` for `NewClientWithDialer` /
  `NewServerWithListener` — the integration point for out-of-tree obfuscation and
  broker/rendezvous flows. No new dependencies. Design: [TRANSPORTS.md](TRANSPORTS.md)
  §10 (censorship-resistance research) and §11 (where obfuscation lives:
  value-rpc seam vs. a standalone `obfs` module vs. servion orchestration).
- **Transport matrix tests** — all four interaction patterns exercised over every
  transport (TCP, Unix socket, WebSocket) in one table-driven test.
- Runnable, output-checked **godoc examples for Unix sockets and WebSocket**
  (`Example_unixSocket`, `Example_webSocket`), alongside the existing
  pattern examples.
- Unit tests for WebSocket address parsing (`splitWSPath`) and the WebSocket
  read-limit (`MaxFrameSize`) enforcement.

### Changed

- **Per-instance configuration via functional options.** Server and client
  behavioral tuning is now passed to the constructors as options instead of being
  read from mutable package globals at runtime, so two servers/clients in one
  process can be tuned independently and there are no config data races. The
  package globals remain the *defaults* an option overrides (backward compatible —
  existing callers and code that sets a global before constructing are unchanged).
  Server: `WithMaxConnections`, `WithMaxConcurrentRequests`,
  `WithMaxConcurrentStreams`, `WithOutgoingQueueCap`, `WithIncomingQueueCap`,
  `WithStreamMaxPending`, `WithHandshakeTimeout`, plus transport-level
  `WithKeepAlivePeriod`, `WithWriteTimeout`, `WithMaxFrameSize`. Client:
  `WithSendingCap`, `WithTimeout`, `WithStreamMaxPending`, `WithKeepAlivePeriod`,
  `WithWriteTimeout`, `WithMaxFrameSize`. All constructors gained a trailing
  `opts ...Option` (variadic, non-breaking). Tests:
  `valueserver.TestServerOptionPerInstance`, `TestServerMaxFrameSizeOption`.
- **BREAKING (transport API): `MaxFrameSize` is captured per connection.**
  `valuerpc.NewMsgConn` and the transport constructors (`NewListener`,
  `NewDialer`, `NewStreamListener`/`Dialer`, `NewUnixListener`, `NewTLSListener`/
  `Dialer`, `NewWebSocketListener`/`Handler`) gained a trailing `maxFrameSize`
  argument (and the WS ones a keepalive/ping interval). `messageConnAdapter` now
  snapshots the limit at construction instead of reading the mutable
  `valuerpc.MaxFrameSize` global on every read — removing the last config data
  race. `MaxFrameSize` semantics on a connection: `> 0` enforces, `<= 0` is
  unlimited; the bring-your-own-connection adapters still default to the package
  `MaxFrameSize`.
- **BREAKING: `context.Context` is now first-class on both ends.**
  - **Server handlers** take `ctx` as their first argument (`Function`,
    `OutgoingStream`, `IncomingStream`, `Chat`). It is cancelled on disconnect,
    server shutdown, or request cancellation, and for unary calls carries the
    client's SLA as a deadline (streams are long-lived and are bounded by
    cancellation, not the per-call SLA).
  - **Client methods** take `ctx` as their first argument too — `CallFunction`,
    `GetStream`, `PutStream`, `Chat` are now
    `CallFunction(ctx, name, args)` etc. (no separate `…Context` variants). A
    context deadline sooner than `SetTimeout` is sent as the request SLA;
    cancelling the context cancels the call (and tears down a stream) and
    returns `ctx.Err()`. Pass `context.Background()` for none.
  - Update handlers to `func(ctx context.Context, …)` and call sites to
    `cli.CallFunction(ctx, …)`. The wire protocol is unchanged. Tests:
    `valueserver.TestHandlerReceivesSLADeadline`, `TestUnaryHandlerContextCanceled`,
    `TestClientContextCancelUnary`, `TestClientContextDeadlineUnary`,
    `TestClientContextCancelStream`.
- **README** and **RESEARCH.md** rewritten to document all three transports
  (intro, architecture diagram, features, dependencies, configuration) instead
  of describing the library as TCP-only.
- **Dependencies updated** to latest: `github.com/coder/websocket` v1.8.15,
  `golang.org/x/net` v0.56.0 (`value` v1.2.0, `quic-go` v0.60.0, `zap` v1.28.0
  and the rest already current).

### Fixed

- **Data race between `Server.Run()` and `Server.Close()`** on the shutdown
  `WaitGroup`: a connection accepted just as the server was closing could call
  `wg.Add(1)` concurrently with `Close`'s `wg.Wait()` (a WaitGroup misuse the race
  detector flags). `Close` now closes the shutdown signal under a mutex that also
  guards the per-connection `Add`, so no handler is registered once shutdown has
  begun. (Surfaced under `go test -race` with `go srv.Run(); defer srv.Close()`.)
- Server handlers that return `context.DeadlineExceeded` / `context.Canceled`
  (e.g. `ctx.Err()`) now map to `CodeDeadlineExceeded` / `CodeCanceled` instead of
  `CodeInternal`.
- `valueserver.Server.Close()` no longer hangs when `Run()` was never called —
  the accept loop was over-counted in the shutdown `WaitGroup`, which now tracks
  only connection-handler goroutines.
- **Credit-based flow control (lossless high-throughput streaming).** Both
  directions now use credit windows instead of the crude sleep-based throttle: a
  receiver grants the sender an initial window (`StreamCredit` message) and
  replenishes it as it delivers values to the consumer; the sender blocks on a
  `valuerpc.CreditGate` (its own goroutine only, never the shared loop) when out
  of credit. A fast producer can no longer overrun a slow consumer — delivery is
  lossless and bounded with no head-of-line blocking; a stuck consumer simply
  stalls its own stream. The deprecated `ThrottleIncrease`/`ThrottleDecrease`
  messages are unused. Regression test: `valueserver.TestHighThroughputStreamLossless`
  (and `BenchmarkChatEcho` is lossless at scale, ~305k msg/s).
- **Stream truncation is surfaced (#13).** A peer that ignores its flow-control
  credit and overruns the buffer has the stream closed with an explicit error
  (server inbound → `ErrorResponse` to the client; client receive → `StreamError`
  on the error handler) instead of silently dropping values. Test:
  `valueserver.TestInboundOverflowSurfaced`.
- **Client `SetErrorHandler` no longer panics on `Close`.** The error handler was
  stored in an `atomic.Value` that also received `*rpcClient` during `Close`;
  storing two concrete types panics (`store of inconsistently typed value`). It is
  now an `atomic.Pointer[ErrorHandler]`, so a custom error handler is safe.
- **Head-of-line blocking (BUG-6) actually fixed.** Stream delivery now goes
  through a per-request `valuerpc.StreamPump`: the shared read/response loop
  `Push`es without blocking and a dedicated goroutine drains a bounded queue into
  the consumer, so one slow consumer can no longer stall other multiplexed
  requests on the connection. A consumer that falls more than
  `valuerpc.DefaultMaxPending` behind has that one stream failed instead of
  pinning unbounded memory. (The earlier pass only stopped the related
  send-on-closed panic; the blocking itself remained.) Regression tests:
  `valueserver.TestSlowStreamConsumerDoesNotBlockOthers`, `valuerpc.TestStreamPump_*`.
- **Unary cancellation leak.** The `canceledRequests` map (which grew unbounded
  when a cancel arrived for an already-running unary call) was replaced by
  context-based cancellation: an in-flight unary handler's context is cancelled
  on `CancelRequest`, and the tracking entry is always cleaned up when the
  handler returns.

- **Concurrent-request cap (DoS hardening).** `valueserver.MaxConcurrentRequests`
  (default 4096, per serving client; `0` disables) bounds how many request
  handlers run at once. Over the limit a request is rejected with an error
  response instead of spawning an unbounded goroutine or blocking the read loop —
  a request flood can no longer exhaust goroutines/memory. Regression test:
  `valueserver.TestMaxConcurrentRequestsRejectsFlood`.
- **Max-connections cap.** `valueserver.MaxConnections` (default `0` = unlimited)
  caps simultaneously open connections; connections over the limit are closed
  immediately.
- **Concurrent-streams cap.** `valueserver.MaxConcurrentStreams` (default `0` =
  unlimited) caps open streaming requests (get/put/chat) per serving client —
  each holds goroutines and buffers for its lifetime. A stream request over the
  limit is rejected with an error response. Regression test:
  `valueserver.TestMaxConcurrentStreamsRejectsExcess`.
- **`time.After` → `time.NewTimer` + `Stop`** in the client unary wait path
  (`SingleResp`), so a timer is no longer leaked per call when the response wins.
- **Per-message frame allocation removed.** The length-prefix framer
  (`messageConnAdapter`) now reuses a per-connection write buffer (guarded by the
  existing write lock) instead of allocating `make([]byte, 4+len)` per message;
  oversized messages (> 64 KiB) use a one-off buffer so a single huge message
  cannot pin memory. Measured: 0 allocs vs 1 alloc + 96 B per frame write.

### Security

- **Handshake authentication hook.** `valueserver.Server.SetAuthenticator` takes a
  `func(conn valuerpc.MsgConn, credential value.Value) error` that runs during the
  handshake (first connect and every reconnect) and rejects the connection on
  error. The client attaches its credential with `valueclient.Client.SetCredential`
  (sent as the `auth` handshake field). This is the transport-agnostic auth seam
  for bearer tokens / API keys / HMAC that the pre-handshake connect-authorizer
  (TLS cert, Unix peer creds) cannot reach. Regression test:
  `valueserver.TestAuthenticatorGatesHandshake`.
- **Authenticated session resumption.** The server now mints a per-session token
  (128-bit, `crypto/rand`) on the first handshake and returns it in the handshake
  response; a reconnect may resume an existing `cid` only by presenting the
  matching token (compared in constant time). Previously the client-asserted
  `cid` alone let any peer reattach to — and thereby close and hijack — another
  client's session. Adds an optional `tok` handshake field (handshake wire
  change). Regression tests: `valueserver.TestSessionResumptionRequiresToken`,
  `TestHijackAttemptLeavesVictimIntact`.

## [1.2.0] — 2026-06-14

The first hardened, multi-transport release. It bundles a Go 1.25 / `value`
v1.2.0 upgrade, fixes for 15 correctness/crash/DoS/lifecycle bugs, three
pluggable transports (TCP, Unix sockets, WebSocket), and the project's first
test suite, benchmarks, and CI.

The MessagePack message format is unchanged — a peer on the previous TCP build
interoperates with this one. Review **Breaking changes** before upgrading.

### License

- **Remains [BUSL-1.1](LICENSE)** © Karagatan LLC. (An Apache-2.0 relicense to
  match upstream `value` was explored and reverted; the project stays BUSL-1.1.)

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

- Decoder hardened: a bounded frame size plus the `value` v1.2.0 decode limits
  prevent a tiny header from forcing a huge allocation.
- Unix-socket **peer-credential authorization** for local trust decisions.
- `wss://` (TLS) via the embedded WebSocket handler on your own TLS server.

### Breaking changes

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
