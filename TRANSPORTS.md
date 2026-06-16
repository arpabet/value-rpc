# value-rpc — Additional Transports (design / research)

Date: 2026-06-14. Companion to [RESEARCH.md](RESEARCH.md) and [FINDINGS.md](FINDINGS.md).

Goal: support **Unix domain sockets** and **WebSocket (MessagePack)** alongside
the current **TCP** transport, without changing the RPC layer or the wire
encoding of messages.

## TL;DR

- The RPC layer (handshake, the four call patterns, cancellation, throttle,
  request multiplexing) and the MessagePack `value` encoding are **already
  transport-agnostic** — they operate on `MsgConn` + `value.Map`. Only the
  byte-transport and framing differ per transport.
- **Unix sockets are almost free.** They are a reliable byte stream just like
  TCP, so the existing 4-byte length-prefix framing works unchanged. The only
  change is parameterizing the network (`"tcp"` → `"unix"`) on listen/dial.
- **WebSocket needs a real transport implementation.** WebSocket is
  *message-oriented* — its own frames already delimit messages — so we must
  **not** add our length prefix on top; each WebSocket **binary** message
  carries exactly one MessagePack `value.Map`. It also rides on HTTP, so it adds
  an upgrade handshake, can share a port with other HTTP routes, and supports
  `wss://` (TLS) and browser clients.
- The refactor is small because the transport coupling today is only ~6 lines
  (see below). The plan introduces a `Listener` / `Dialer` / `MsgConn` seam.
- Recommended order: **Unix first** (a few hours), **WebSocket second** (about a
  day, one new dependency).

## 1. Where transport is coupled today

The whole coupling surface, found by grep:

| Location | Coupling |
|----------|----------|
| `valuerpc/rpc.go:43` | `MsgConn.Conn() net.Conn` leaks the raw conn |
| `valuerpc/rpc.go:46` | `NewMsgConn(conn net.Conn, …)` (stream framing) |
| `valueserver/server.go:55` | `net.Listen("tcp", address)` |
| `valueserver/server.go:132` | `valuerpc.NewMsgConn(conn, …)` on accept |
| `valueserver/server.go:158` | `conn.(*net.TCPConn)` keepalive |
| `valueserver/server.go:170,179` | `conn.Conn().SetReadDeadline(…)` (handshake) |
| `valueclient/connection.go:33-48` | `net.Dial("tcp", …)` / SOCKS5 + keepalive |

Everything else — `serving_client.go`, `serving_request.go`, `request.go`,
`client.go` dispatch — already works purely in terms of `MsgConn` and messages.
That is why this is a contained change.

## 2. Proposed abstraction

Introduce a transport seam in `valuerpc` (one small file, e.g.
`valuerpc/transport.go`). `MsgConn` loses `Conn() net.Conn` (which can't exist
for WebSocket) and gains the two operations the upper layers actually need:

```go
type MsgConn interface {
    ReadMessage() (value.Map, error)
    WriteMessage(msg value.Map) error
    SetReadDeadline(t time.Time) error // used by the handshake timeout
    RemoteAddr() string                // used for logging
    Close() error
}

// Server side: produces MsgConns.
type Listener interface {
    Accept() (MsgConn, error)
    Addr() net.Addr
    Close() error
}

// Client side: produces a MsgConn.
type Dialer interface {
    Dial() (MsgConn, error)
}
```

`messageConnAdapter` (the current length-prefix codec) keeps a `net.Conn` and
trivially implements `SetReadDeadline`/`RemoteAddr`. The server/client stop
touching `net.Conn` directly:

- `rpcServer` holds a `valuerpc.Listener`; `Run()` calls `lis.Accept()` and gets
  a ready `MsgConn` (framing + keepalive already applied inside the transport).
- `rpcConn` (client) is built from a `valuerpc.Dialer`.
- `handshake()` calls `conn.SetReadDeadline(...)` instead of
  `conn.Conn().SetReadDeadline(...)`.

This is the only "breaking" change, and `MsgConn` is realistically implemented
only inside this module, so it is safe pre-1.0.

## 3. Transport: Unix domain sockets

A Unix socket is a reliable, ordered byte stream — semantically identical to TCP
for our purposes — so **the existing length-prefix framing is reused verbatim**.
The stream listener/dialer just take a network parameter:

```go
// One implementation serves both "tcp" and "unix".
func NewStreamListener(network, address string) (Listener, error) {
    lis, err := net.Listen(network, address) // "tcp" or "unix"
    if err != nil { return nil, err }
    return &streamListener{lis: lis, framed: true}, nil
}

func (l *streamListener) Accept() (MsgConn, error) {
    c, err := l.lis.Accept()
    if err != nil { return nil, err }
    enableKeepAlive(c)               // no-op for *net.UnixConn (guarded type-assert)
    return NewMsgConn(c, DefaultTimeout), nil
}

func NewStreamDialer(network, address, socks5 string) Dialer { ... }
```

`enableKeepAlive`'s `conn.(*net.TCPConn)` assertion already fails gracefully for
Unix conns, so it needs no change.

**Gotchas to handle (Unix-specific):**

- **Stale socket file.** `net.Listen("unix", path)` fails if the path exists.
  Standard fix: best-effort `os.Remove(path)` before `Listen`, and remove it on
  `Close` (Go does this automatically for listeners it created, unless the
  process is killed). Document/handle the leftover-file case.
- **Filesystem permissions** are the access control. `os.Chmod(path, 0600)` to
  restrict to the owner; place the socket in a directory with controlled perms.
- **Bonus — peer authentication.** Unix sockets expose the peer's uid/gid via
  `SO_PEERCRED` (Linux) / `LOCAL_PEERCRED` (macOS/BSD), reachable through
  `golang.org/x/sys/unix` from the `*net.UnixConn`'s syscall conn. This enables
  real local authz ("only uid 1000 may call admin functions") that TCP cannot do
  cheaply. Worth exposing later as an optional hook on the serving client.
- **No keepalive needed** (no network path to go dead); rely on EOF/`Close`.

**Why add it:** lowest latency and highest throughput of the three (no TCP/IP
stack, no checksums, no port), and the natural choice for same-host IPC
(sidecars, agents, CLIs talking to a local daemon) with filesystem-based auth.

## 4. Transport: WebSocket (MessagePack)

WebSocket is the interesting one because it is **message-framed**, runs over
**HTTP**, and is not a `net.Conn`.

### 4.1 Framing: no length prefix

Each vRPC message is sent as **one WebSocket binary message** whose payload is
`value.Pack(msg)`. The WebSocket frame boundary *is* the message boundary, so
the 4-byte length prefix is dropped on this transport. `MaxFrameSize` maps onto
the WebSocket read limit (`SetReadLimit`).

```go
type wsMsgConn struct {
    conn     *websocket.Conn // coder/websocket
    base     context.Context
    cancel   context.CancelFunc
    writeTO  time.Duration
    readDL   atomic.Pointer[time.Time]
    writeMu  sync.Mutex
}

func (t *wsMsgConn) ReadMessage() (value.Map, error) {
    ctx := t.base
    if dl := t.readDL.Load(); dl != nil && !dl.IsZero() {
        var cancel context.CancelFunc
        ctx, cancel = context.WithDeadline(t.base, *dl)
        defer cancel()
    }
    typ, data, err := t.conn.Read(ctx)
    if err != nil { return nil, err }
    if typ != websocket.MessageBinary { return nil, errExpectedBinary }
    v, err := value.Unpack(data, true)
    ... // assert MAP
}

func (t *wsMsgConn) WriteMessage(m value.Map) error {
    payload, err := value.Pack(m); if err != nil { return err }
    ctx, cancel := context.WithTimeout(t.base, t.writeTO); defer cancel()
    t.writeMu.Lock(); defer t.writeMu.Unlock() // WS forbids concurrent writes
    return t.conn.Write(ctx, websocket.MessageBinary, payload)
}
```

`SetReadDeadline` stores a time that the next `Read` turns into a context
deadline (coder/websocket is context-native; gorilla has `SetReadDeadline`
directly).

### 4.2 Server side: HTTP upgrade, two modes

```go
// Standalone: vRPC owns the HTTP server.
func NewWebSocketServer(addr, path string, logger *zap.Logger) (Server, error)

// Embedded: mount vRPC on the user's existing mux / share a port with REST,
// health checks, metrics, etc. — a real WebSocket advantage.
func WebSocketHandler(srv Server) http.Handler
```

The handler upgrades and feeds the connection into the same accept path:

```go
func (l *wsListener) handler() http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        c, err := websocket.Accept(w, r, &websocket.AcceptOptions{
            // OriginPatterns: ... (CSRF protection for browsers)
        })
        if err != nil { return }
        c.SetReadLimit(int64(valuerpc.MaxFrameSize))
        l.incoming <- newWSMsgConn(r.Context(), c) // Accept() ranges over this
    }
}
```

### 4.3 What WebSocket buys

- **`wss://` (TLS) for free**, and HTTP-layer concerns (auth headers, cookies,
  reverse proxies, L7 load balancers, firewalls that only allow 443).
- **Port sharing**: run vRPC at `/rpc` next to REST/health on one `http.Server`.
- **Browser reach** *in principle*: a browser can speak this protocol if it
  MessagePack-encodes the same vRPC message maps. Note: only the **Go** client
  ships here; a JS/TS client would be a separate implementation. Worth a small
  `PROTOCOL.md` (message schema) if browser clients are a goal.

### 4.4 Library choice

| Library | Pros | Cons |
|---------|------|------|
| **`github.com/coder/websocket`** (rec.) | zero dependencies, context-native, modern, simple `Read`/`Write` binary API, `SetReadLimit` | deadlines via context, not `SetReadDeadline` |
| `github.com/gorilla/websocket` | battle-tested, `SetReadDeadline`/`SetWriteDeadline`, direct ping/pong | more boilerplate, callback-style control frames |
| `golang.org/x/net/websocket` | already an indirect dep | discouraged by its own authors; weak control — **avoid** |

Recommendation: **coder/websocket** (keeps the dependency footprint minimal,
which matches this library's ethos). It is the only new dependency required.

### 4.5 Keepalive

Use WebSocket **ping/pong** (both libraries support it) instead of TCP
keepalive, so dead peers are detected at the protocol layer.

## 5. What does NOT change

- The vRPC **handshake** (magic/version/clientId) is just the first message on
  the `MsgConn` — works over every transport unchanged.
- **All four patterns**, **cancellation**, **throttle/flow-control**, **request
  multiplexing**, and **client session resumption** are message-level — unchanged.
- The **MessagePack `value` encoding** of every message is identical on every
  transport. A message captured on TCP is byte-for-byte the same on Unix and
  inside a WebSocket binary frame.

Only two things vary per transport: (a) how bytes are moved, and (b) whether we
add our length prefix (stream transports) or rely on native message framing
(WebSocket).

## 6. Address scheme & backward-compatible API

Keep the current constructors working (no scheme ⇒ TCP), and add scheme parsing
plus explicit constructors:

```go
// Server (address may be a bare "host:port" → tcp, or a scheme URL)
valueserver.NewServer(":9000", logger)                  // tcp (unchanged)
valueserver.NewServer("unix:///run/vrpc.sock", logger)  // unix
valueserver.NewServer("ws://:9000/rpc", logger)         // websocket
valueserver.NewServer("wss://:9000/rpc", logger)        // websocket + TLS (needs tls.Config)
valueserver.NewServerWithListener(lis, logger)          // full control

// Client
valueclient.NewClient("host:9000", "")                  // tcp (unchanged)
valueclient.NewClient("unix:///run/vrpc.sock", "")      // unix
valueclient.NewClient("ws://host:9000/rpc", "")         // websocket
valueclient.NewClientWithDialer(d)                      // full control
```

`NewServer`/`NewClient` become thin parsers that pick a `Listener`/`Dialer`.
SOCKS5 stays a TCP-only option (already its own argument).

## 7. Security & performance comparison

| | TCP | Unix | WebSocket (ws) | WebSocket (wss) |
|---|-----|------|----------------|------------------|
| Reach | host↔host | same host only | host↔host, browsers, proxies | same + TLS |
| Latency (small msg) | baseline | **lowest** (~½ of TCP loopback) | TCP + WS frame overhead | + TLS handshake/record |
| Framing | our 4-byte prefix | our 4-byte prefix | native WS binary frame | native WS binary frame |
| AuthN | none built-in | **uid/gid via peer creds** | HTTP headers/cookies, origin | + mutual TLS |
| Transport security | none (add TLS) | filesystem perms | none | **TLS** |
| Port sharing w/ HTTP | no | n/a | **yes** | **yes** |
| New dependency | — | — | coder/websocket | coder/websocket |

## 8. Refactor plan (phased)

1. **Seam (no behavior change).** ✅ **DONE (2026-06-14).** `MsgConn` now exposes
   `SetReadDeadline` + `RemoteAddr` instead of `Conn() net.Conn`;
   `valuerpc.Listener`/`valuerpc.Dialer` were added with a generic
   `streamListener`/`streamDialer` (parameterized by network, so TCP **and**
   Unix already work); all stream-transport coupling (`net.Listen`/`net.Dial`/
   SOCKS5/keepalive) now lives only in `valuerpc/transport.go`. New public
   constructors `valueserver.NewServerWithListener` and
   `valueclient.NewClientWithDialer` expose the seam; `NewServer`/`NewClient`
   are unchanged (TCP). A Unix-socket round-trip and the new `MsgConn` methods
   are covered by tests. Pure refactor — the full `-race` suite still passes.
2. **Unix.** ✅ **DONE (2026-06-14).** Added `tcp://`/`unix://` address parsing
   (`valuerpc.ParseAddress`/`NewListener`/`NewDialer`; bare `host:port` still
   defaults to TCP), the `valueserver.NewUnixServer` / `valueclient.NewUnixClient`
   convenience constructors, and stale-socket-file cleanup on bind
   (`valuerpc.NewUnixListener`, which refuses to clobber a non-socket file).
   **Peer authentication** is implemented: `valuerpc.PeerCred` +
   `valuerpc.PeerCredOf(MsgConn)` read the kernel-reported peer uid/gid/pid
   (`SO_PEERCRED` on Linux, `LOCAL_PEERCRED` on macOS via build-tagged files,
   unsupported elsewhere), surfaced through a new
   `valueserver.Server.SetConnectAuthorizer` hook that runs before the handshake.
   Tested over real Unix sockets (round-trip, scheme + convenience constructors,
   stale-file cleanup, peer-cred uid match, authorizer rejection); Linux build
   verified by cross-compile.
3. **WebSocket.** ✅ **DONE (2026-06-14).** Added `valuerpc/transport_ws.go` on
   `github.com/coder/websocket` (one MessagePack `value.Map` per binary frame, no
   length prefix; `SetReadLimit` = `MaxFrameSize`; ping/pong keepalive via
   `WSKeepAlive`). Server: `valueserver.NewWebSocketServer(addr, path)` (standalone)
   and `valueserver.NewWebSocketHandler()` returning an `http.Handler` to mount on
   your own mux (port sharing; wss:// via your own TLS server). Client:
   `valueclient.NewWebSocketClient(url)`. The `ws://` scheme also works through
   plain `NewServer`/`NewClient`; `wss://` is supported on the client and on the
   embedded handler. All four patterns are tested over WS, plus the embedded
   handler and a `BenchmarkWebSocketUnary` (~30 µs/op vs ~25 µs TCP). Whole suite
   green under `-race`; linux+windows cross-compile verified.
4. **Tests/CI/docs.** Parameterize the integration suite by transport (run the
   same tests over tcp/unix/ws via a table), add benchmarks per transport, and
   document in the README. The existing `Example_*` tests already exercise the
   default (tcp) path.

Risk is low: phase 1 is mechanical and fully covered by the current test suite;
phases 2–3 are additive.

## 9. Candidate transports — what's missing and what could be added

Implemented today: **TCP** (+ SOCKS5), **Unix domain sockets** (+ peer creds),
**WebSocket** (+ `wss`), **TLS/mTLS**, **in-memory**, and **QUIC** (seam-fit,
per-request streams). This section surveys everything else worth considering.

### 9.0 The one requirement

vRPC multiplexes all four patterns, cancellation, and throttling over a **single**
`MsgConn`, so any transport must provide one **reliable, ordered, full-duplex**
message channel. That immediately sorts candidates into three buckets:

- **Stream transports** — a reliable ordered byte stream; reuse the existing
  4-byte length-prefix framing unchanged. Adding one is ~a file.
- **Message-framed transports** — already delimit messages (like WebSocket); one
  MessagePack map per frame, no length prefix.
- **Connectionless / broker / unreliable** — UDP, Kafka, etc.; need an adapter
  that manufactures a reliable ordered channel, or don't fit at all.

The public seam (`valuerpc.NewMsgConn`, `NewStreamListener`/`NewStreamDialer`,
`NewServerWithListener`/`NewClientWithDialer`) already lets you add any
`net.Conn`-based transport **without forking the library**.

### 9.1 Catalog

| Transport | What it adds (vs. today) | Seam fit | Effort | Library |
|-----------|--------------------------|----------|--------|---------|
| **TLS / mTLS over TCP** (`tls://`) ✅ | encryption + **certificate client auth** for TCP | stream (reuse framing) | low | stdlib `crypto/tls` |
| **In-memory** (`mem://`) ✅ | zero-network loopback for tests & monolith→services composition | native (skips pack/unpack) | low | stdlib (`chan`) |
| **QUIC** (`quic://`) ✅ | TLS 1.3 built-in, 0-RTT, **connection migration**, per-request streams (HOL reduced; see §9.2) | seam-fit done | med–high | `quic-go` |
| **AF_VSOCK** (`vsock://`) | host↔guest / **enclave** RPC (Firecracker, AWS Nitro, KVM) | stream | low | `mdlayher/vsock` |
| **Windows named pipes** (`npipe://`) | Windows local IPC | stream | low | `Microsoft/go-winio` |
| **stdio / subprocess** (`stdio://`) | **subprocess-plugin** RPC (LSP / `hashicorp/go-plugin` style) | stream (single pre-connected conn, no listener) | low | stdlib (`os.Stdin/Stdout`) |
| **SSH channel** (`ssh://`) | auth + encryption via SSH; jump-host / ops tooling | stream (`ssh.Channel` is `io.ReadWriteCloser`) | med | `x/crypto/ssh` |
| **WebTransport** (HTTP/3) | modern **browser** full-duplex; successor to WebSocket | message-framed | med | `quic-go/webtransport-go` |
| **NATS** (broker) | location independence, fan-out, no direct connectivity | adapter; model mismatch | med | `nats.go` |
| **HTTP/2** (`h2://`) | "over HTTP", like WebSocket but HTTP/2 streams | message-ish | med | `x/net/http2` |
| ~~Plain UDP~~ | — | unreliable/unordered → use QUIC | — | — |
| ~~SSE~~ | server→client only (not full-duplex) | doesn't fit | — | — |
| ~~Kafka / AMQP for RPC~~ | throughput broker, high latency | wrong tool for RPC | — | — |

### 9.2 Recommended order

1. **TLS / mTLS over TCP — ✅ DONE (2026-06-15).** `valueserver.NewTLSServer` /
   `valueclient.NewTLSClient` take a `*tls.Config`; a `*tls.Conn` is a `net.Conn`,
   so the length-prefix framing is reused unchanged (keepalive unwraps the TLS
   conn to reach the TCP socket). With `tls.Config.ClientAuth` +
   `ClientCAs` the server enforces **mutual TLS**, and the verified client
   certificate is exposed to a connect-authorizer via `valuerpc.PeerCertificates`
   — the network analogue of the Unix peer-credential check. The `tls://` scheme
   works through plain `NewClient` (system root CAs); `NewListener` rejects
   `tls://` (a server needs a certificate). Tested: server-auth round-trip, mTLS
   round-trip + client-cert authz, and mTLS rejection of an uncertified client.
2. **In-memory transport — ✅ DONE (2026-06-15).** `valueserver.NewMemServer(name)`
   / `valueclient.NewMemClient(name)` (and the `mem://name` scheme) connect a
   client and server in the same process over Go channels via a process-wide
   name registry. Messages pass **by reference** — no sockets and no MessagePack
   (safe because vRPC messages are immutable `value.Map`s) — making it the
   fastest transport and the basis for fast, deterministic tests and monolith
   composition (swap the address to a real socket later with no other call-site
   changes). Covered by the transport-matrix test (all four patterns) plus
   round-trip/scheme/duplicate-name/unregistered-dial tests.
3. **QUIC — ✅ DONE (2026-06-15), seam-fit variant, in a separate module.**
   `valuequic.NewServer` / `valuequic.NewClient` in the **`go.arpabet.com/value-rpc/quic`**
   submodule (so `github.com/quic-go/quic-go` stays out of the core module — only
   programs that use QUIC pull it in). TLS-mandatory (reuses the TLS config model;
   mutual TLS + `PeerCertificates` work over QUIC too), with TLS 1.3, 0-RTT, and
   connection migration. **Each RPC request maps to its own QUIC stream** — the
   client opens a stream per request, the server accepts one per request, with
   independent per-stream flow control and a handshake-first ordering guarantee;
   streams are freed when both halves finish (verified by a tight stream-cap
   test). It fits the existing `MsgConn` seam, so inbound frames still funnel
   through one per-connection read loop — that reduces, but does not fully
   eliminate, *application-level* slow-consumer HOL. The full fix (per-stream
   readers dispatching directly, no funnel) is the async-demux rework below.
   Covered by the transport-matrix test (all four patterns) plus
   round-trip / mTLS / stream-freeing tests.

### 9.3 Add on demand (niche but real, all low effort)

- **vsock** — RPC between a hypervisor host and guest VMs / **confidential-compute
  enclaves** (AWS Nitro Enclaves, Firecracker microVMs). `net.Conn`-compatible, so
  it drops straight into the stream transport.
- **Windows named pipes** — local IPC on Windows. (Less essential now that
  `AF_UNIX` works on Windows 10+, which the existing `unix://` transport can use.)
- **stdio** — a host process speaking vRPC to a **subprocess plugin** over its
  stdin/stdout, the model used by LSP and `hashicorp/go-plugin`. It is a single
  pre-connected `MsgConn` (no listener/accept), so it also motivates a small
  `Dialer`/`Listener` adapter for pre-established `io.ReadWriteCloser` streams.

### 9.4 Different model — evaluate before adopting

- **NATS (or another broker).** You can tunnel a vRPC connection over a pair of
  subjects (one per direction), gaining location independence, fan-out, and "no
  direct connectivity required". But it inverts the model — vRPC is
  connection/session-oriented, NATS is broker-mediated and connectionless — and
  it adds a broker dependency and a hop of latency. Worth it only if you
  specifically want broker semantics; otherwise prefer a direct transport.
- **WebTransport** pairs naturally with a QUIC addition and is the modern
  browser path, but only matters if browser clients become a goal (which also
  needs a JS implementation of the vRPC message protocol).
- **HTTP/2** buys little over the existing WebSocket transport (both are "RPC
  over an HTTP upgrade"); skip unless an HTTP/2-only environment forces it.

### 9.5 Two small enablers (independent of any single transport) — **implemented**

- ✅ A `MsgConn` constructor from an arbitrary **`io.ReadWriteCloser`** (not just
  `net.Conn`) lets users plug in `ssh.Channel`, `os` pipes, gVisor channels, and
  custom tunnels with the standard length-prefix framing. (`NewMsgConn` now takes
  `io.ReadWriteCloser`.)
- ✅ A **single-connection** `Dialer`/`Listener` adapter (wrap one already-open
  conn) covers stdio and any externally-established connection. (`NewSingleConnDialer`
  / `NewSingleConnListener`, plus the general `NewFuncDialer` / `NewAcceptListener`.)

Both landed as a few lines on top of the seam (`valuerpc/transport_conn.go`); see
§11 for the full bring-your-own-connection contract and how obfuscation attaches.

---

## 10. Censorship-resistant / obfuscated transports (research)

> **Scope and intent.** This section researches how value-rpc could keep a
> *legitimate* service reachable when an adversarial network operator tries to
> **detect and block it** — the same problem space as the Tor Project's
> Pluggable Transports, Shadowsocks, and Xray/REALITY. The user's framing —
> "popular services that some bad people would like to ban" — is exactly the
> censorship-resistance threat model: the "bad actor" is the *censor*, and the
> goal is to stop a network operator from fingerprinting and dropping traffic
> from a service that has every right to run.
>
> **This is design research, not an implementation.** No obfuscation code is
> added here. Read §10.7 (caveats) before building any of it.

### 10.0 Threat model — what a censor actually does

A modern censor (the canonical example is China's Great Firewall, but the same
toolkit is sold commercially and deployed in many networks) has an escalating
ladder of capabilities. Each rung is cheap to *us* to defeat in isolation and
expensive in combination:

| Censor capability | What it keys on | What it blocks today |
|---|---|---|
| **IP / port blocklists** | server address, well-known ports | static endpoints, known relays |
| **SNI / DNS filtering** | cleartext `server_name` in TLS ClientHello, DNS queries | TLS by destination name; DoH is now itself fingerprinted |
| **Protocol DPI (signature)** | static byte patterns / magic numbers / framing | anything with a fixed handshake header |
| **Active probing** | connects to *your* server and replays/pokes it | servers that answer probes differently than the protocol they imitate |
| **Statistical / flow fingerprinting** | packet sizes, timing, direction, burst shape, entropy | obfuscated protocols by their *behaviour*, not their bytes |
| **Fully-encrypted-traffic detection** | "looks like random bytes" heuristics (entropy, printable-ASCII ratio) | Shadowsocks, VMess, obfs4 — the "look like nothing" designs |

The decisive shift, and the thing that makes this interesting for an RPC
library specifically, is the bottom two rows. Since late 2021 the GFW has
**passively detected fully-encrypted traffic in real time** using a small set of
entropy/printable-ratio heuristics, which is what killed the first generation of
"just make it look like random noise" tools (Shadowsocks, VMess, obfs4). As of
2025 the GFW also does **QUIC SNI inspection, DoH identification, and ML-assisted
flow analysis**. The lesson the whole field has internalized:

> **"Look like nothing" lost. "Look like something extremely common" won.**

That is the strategic core of everything below.

### 10.1 What value-rpc leaks today (the signatures to hide)

Before designing cover, enumerate what an observer can fingerprint on a current
vRPC connection:

1. **The handshake magic / message shape.** Every connection opens with a
   `HandshakeRequest` `value.Map` containing stable, low-cardinality field tags
   (`t`, `rid`, `fn`, `m`, `v`). Over plaintext TCP that is a perfect DPI
   signature; even the *length* of the first few frames is distinctive.
2. **The 4-byte big-endian length prefix.** Regular, parseable framing — a
   classic flow-structure tell.
3. **MessagePack structure.** Recognizable type bytes and map/array headers.
4. **The RPC ping-pong.** Request→response call/return has a characteristic
   bidirectional, low-latency, small-frame rhythm. Streaming has a one-to-many
   burst shape. These survive encryption — TLS hides *content*, not *sizes and
   timing*.
5. **Per-operation size signatures (the user's "signatures of operations").**
   `login(user,pass)` and `getBalance()` and a 5 MB `GetStream` each have a
   recognizable request/response size profile. Even fully encrypted, an observer
   can often tell *which operation* ran from frame sizes alone. **This is the
   thing the user specifically asked to hide, and it is an RPC-library-level
   problem that no generic VPN solves for you.**
6. **TLS/QUIC handshake fingerprint (JA3/JA4).** Go's `crypto/tls` and `quic-go`
   produce a cipher/extension/curve ordering that does **not** match a mainstream
   browser, so even our `tls://` and `quic://` transports stand out from "normal
   web traffic" at the handshake.
7. **SNI and UDP.** `tls://`/`quic://` send a cleartext SNI; QUIC is UDP, which
   some networks throttle or block wholesale.

### 10.2 The field today (2025–2026) — what's actually deployed

Grounding the design in systems that currently work in the wild:

- **REALITY (Xray/XTLS).** The current best-in-class anti-probe design. Instead
  of presenting *its own* TLS identity, a REALITY server **borrows a real,
  unrelated high-reputation site's TLS handshake**, and on an unauthenticated
  active probe it **transparently forwards the prober to that real site** — so
  the censor's probe sees a genuine, expected TLS server and learns nothing. It
  uses **uTLS** to mimic a specific browser's ClientHello fingerprint and tries
  to replicate the target's packet-size/timing profile. No certificate to obtain,
  no domain to burn.
- **uTLS fingerprint mimicry.** A library that lets a Go client emit a
  ClientHello byte-identical to Chrome/Firefox/Safari (JA3/JA4 match). It is the
  substrate under REALITY and most modern tools.
- **Snowflake (Tor).** Domain-fronted signalling hands the client a swarm of
  **ephemeral, volunteer WebRTC proxies**; the data path looks like a **WebRTC
  video/voice call** (DTLS/SRTP over UDP), which is everywhere and expensive to
  block wholesale. Blocking resistance comes from the proxies being numerous and
  short-lived, so an IP blocklist can't keep up.
- **meek / domain fronting.** Tunnel through a big CDN so the visible SNI/IP is
  the CDN's, not yours. Classic fronting (mismatched SNI vs. Host) is mostly dead
  (CDNs disabled it), but **CDN co-tenancy + ECH** is its spiritual successor.
- **ECH (Encrypted Client Hello).** Encrypts the SNI so destination-by-name
  filtering fails. As of early 2025 the GFW does **not** block QUIC+ECH unless
  the *outer* SNI is already blocked, and does not reassemble a ClientHello split
  across UDP datagrams — i.e. ECH plus a friendly fronting domain currently
  works.
- **obfs4 / Shadowsocks.** The previous generation ("uniformly random bytes").
  Still useful where fully-encrypted-traffic detection isn't deployed, but the
  entropy heuristics specifically target them — a cautionary tale, not a target
  to imitate.

### 10.3 The key insight for an RPC library

value-rpc should **not** try to become a circumvention tool or invent
obfuscation crypto. Two things are true at once:

1. **Transport-shape obfuscation is a solved-by-specialists problem.** The seam
   (`Listener`/`Dialer`/`MsgConn`, §2) already lets us **wrap a `net.Conn` that
   someone else made unblockable.** That is the entire Pluggable-Transport
   contract: a PT hands you an ordinary stream; you run your protocol over it.
2. **Per-operation traffic-shape leakage is *not* solved by those tools.** A
   VPN/PT hides *that you're using vRPC*; it does nothing about *which RPC you
   just called* leaking through frame sizes/timing. This is a real, creative
   contribution — but it is **carrier-agnostic** (gRPC and HTTP leak operation
   sizes too), so it belongs in a shared, standalone `obfs` module that every
   carrier reuses, **not** in value-rpc core. See §11 for the layering decision.

So the research splits cleanly into **(A) reuse external cover** and **(B) a
traffic-shaping layer** — both of which live *outside* value-rpc and attach
through the seam described in §11.

### 10.4 Strategy A — plug existing cover into the seam (recommended first)

All of these are **out-of-tree modules** (like `value-rpc/quic`) that produce a
`MsgConn`; core stays clean. They attach through the bring-your-own-connection
seam in §11 (`NewMsgConn` over any `io.ReadWriteCloser`, plus `NewFuncDialer` /
`NewAcceptListener`), now implemented.

1. **PT-wrapping `Dialer`/`Listener`.** Accept a connection from `lyrebird`
   (obfs4), Snowflake, or Conjure (run as a side process or linked library) and
   present it as a `MsgConn`. *Lowest risk, highest leverage — you inherit a
   battle-tested, actively-maintained obfuscation layer and never touch
   obfuscation crypto yourself.* This should be the headline recommendation.
2. **uTLS fingerprint mimicry for `tls://` and `quic://`.** Swap the ClientHello
   builder so our TLS/QUIC handshake is byte-identical to a current browser
   (fixes leak #6). Server-side, present a normal-looking certificate. Small,
   self-contained, and removes the most obvious "this isn't a browser" tell.
3. **ECH + CDN co-tenancy for `wss://`/`quic://`.** Run the service behind a
   mainstream CDN and enable ECH so the SNI is encrypted (fixes leaks #7 and
   partially #4). WebSocket-over-CDN already works with today's `wss://`
   transport — this is mostly deployment guidance plus an ECH-aware dialer.
4. **REALITY-style transport (self-hosted servers).** A `reality://` module that
   borrows a real site's TLS parameters and forwards active probes to it. This is
   the strongest anti-probing option but the most complex; treat as a later,
   clearly-scoped module that leans on an existing implementation rather than a
   reimplementation.
5. **WebRTC data-channel transport (Snowflake-style).** A `webrtc://` module so a
   vRPC stream rides a DTLS/SRTP data channel and looks like a call. Heavy
   dependency; justified only for the hardest networks.

### 10.5 Strategy B — a traffic-shaping layer in the `obfs` module (the creative core)

This is the part no external PT addresses: hiding the **operation signatures**
(leak #5). It is byte-stream / `net.Conn` middleware — a shaper that wraps the
base connection *below* value-rpc's framing — so it composes with *every*
transport (TCP, TLS, QUIC, WebSocket, or a PT from Strategy A) **and** with gRPC
and HTTP. Because it is carrier-agnostic it lives in the standalone `obfs` module,
not in value-rpc (see §11); value-rpc only supplies the seam it attaches to. The
techniques, from cheapest to most aggressive:

1. **Fixed-size cells.** Stop sending a frame whose length equals the payload.
   Pad every logical message up to the next multiple of a cell size (e.g. 512 B)
   and split large ones across uniform cells, the way Tor uses 514-byte cells.
   Result: `login` and `getBalance` and the first chunk of a 5 MB stream all look
   identical on the wire. **This single change neutralizes most per-operation
   size fingerprinting.**
2. **Length-padding distributions.** Where fixed cells are too rigid (large
   streams), pad lengths to a randomized/quantized bucket so sizes carry far less
   information, à la the padding research behind modern PTs.
3. **Request/response batching + decoupling.** Coalesce or split logical vRPC
   messages so wire frames stop mapping 1:1 to RPC calls — break the readable
   call/return rhythm (leak #4).
4. **Timing jitter.** Add small randomized delays / release on a quantized clock
   so inter-frame timing stops correlating with computation time and operation
   identity. (Direct latency cost — make it tunable.)
5. **Cover / chaff traffic.** Optionally emit padding cells during idle periods,
   or run at a (capped) constant rate, so "silent vs. busy" and burst shape stop
   leaking. The extreme is constant-rate traffic (maximum hiding, maximum cost);
   the practical version is bounded, adaptive chaff.
6. **Adaptive target mimicry.** The ambitious version: shape the size/timing
   distribution to match a *chosen cover protocol* (e.g. HTTP/2 to a CDN), the
   way REALITY tries to replicate its target's packet profile — so the flow is
   statistically *typical*, not merely *uniform*.
7. **Anti-entropy framing.** Because "uniformly random bytes" is now itself a
   detectable signature (§10.0), a shaping layer that is meant to ride a
   *cleartext* channel must avoid the high-entropy tell — i.e. shape toward a
   plausible *structured* protocol, not toward noise. (When riding TLS/QUIC this
   is moot; the TLS record already supplies the entropy profile of normal web
   traffic.)

A clean API is a single `net.Conn` middleware in the `obfs` module, configured by
policy and attached through the §11 seam:

```go
// Sketch only — lives in the standalone obfs module, not value-rpc.
type ShapePolicy struct {
    CellSize  int           // 0 = off; else pad/split to fixed cells
    PadTo     func(int) int // length-bucketing function
    Jitter    time.Duration // max random delay before flush
    CoverRate int           // idle chaff cells/sec (0 = off)
}
func Wrap(base net.Conn, p ShapePolicy) net.Conn // shapes the byte stream

// Attaches with no value-rpc changes, via the §11 bring-your-own-conn seam:
dialer := valuerpc.NewFuncDialer(func() (io.ReadWriteCloser, error) {
    c, err := net.Dial("tcp", addr)
    if err != nil {
        return nil, err
    }
    return obfs.Wrap(c, policy), nil
}, writeTimeout)
```

Crucially, the shaper operates on the byte stream **below** value-rpc's framing —
fixed cells are a link-layer concept (Tor cells sit under Tor's own commands), so
the shaper needs no message awareness and value-rpc needs no shaping code. Same
composability story as the rest of §2.

### 10.6 Recommended ordering

1. **The §11 seam** (`NewMsgConn` over `io.ReadWriteCloser` + the func/single-conn
   Dialer/Listener adapters) — **done**; unlocks everything else as out-of-tree
   modules.
2. **PT-wrapping module** (Strategy A.1) — reuse obfs4/Snowflake/Conjure; never
   roll your own obfuscation crypto.
3. **uTLS mimicry + ECH/CDN guidance** (A.2, A.3) — fix the handshake and SNI
   tells using the transports already shipped.
4. **`obfs` shaping middleware** (Strategy B, starting with fixed-size cells) —
   the win on operation-signature hiding, reusable by every carrier.
5. **REALITY-style / WebRTC modules** (A.4, A.5) — only for the hardest networks,
   leaning on existing implementations.

Everything here ships as **separate modules** so the dependencies *and the
ethical/legal surface* stay isolated from core, exactly as `value-rpc/quic` does
for quic-go.

### 10.7 Caveats — read before building any of this

- **Obfuscation ≠ security.** Traffic shaping and PTs hide *that* and *what* you
  communicate from a network observer; they are **not** confidentiality or
  authentication. Always run real **TLS + mTLS** (the existing `tls://`
  transport) *underneath*. A shaping layer over plaintext is a privacy illusion.
- **It is an arms race, and the defender (censor) moves too.** Today's cover is
  tomorrow's signature — "look like random" was state-of-the-art until entropy
  detection killed it. Anything built here must be **pluggable and disposable**,
  and should track the upstream PT ecosystem rather than fork from it.
- **Reuse, don't reinvent.** The strong recommendation throughout is to **wrap
  vetted implementations** (lyrebird/obfs4, Snowflake, uTLS, an existing REALITY
  stack). Hand-rolled obfuscation crypto is how tools get *de*-anonymized.
- **Real cost.** Fixed cells, chaff, and jitter trade **bandwidth and latency**
  for unlinkability. They must be **opt-in and tunable**, never the default for
  an ordinary low-latency RPC deployment.
- **Dual-use and legality.** These techniques are legitimate and important for
  press, NGOs, and ordinary users under censorship — and they are **dual-use**.
  They must not be used to evade authorized security monitoring, for malicious
  command-and-control, or in violation of applicable law or platform terms.
  Whether running circumvention software is lawful **varies by jurisdiction**;
  that is a deployer responsibility, not something the library can decide.

### 10.8 References

- GFW SNI-based QUIC censorship & ECH behaviour (USENIX Security '25) — <https://gfw.report/publications/usenixsecurity25/en/>
- Snowflake: WebRTC-proxy circumvention (USENIX Security '24) — <https://www.usenix.org/conference/usenixsecurity24/presentation/bocovich>
- State of Tor circumvention (Snowflake/meek/obfs4) — <https://www.h25.io/dark-web/snowflake-meek-and-beyond-the-state-of-tor-censorship-circumvention/>
- GFW upgrades, Q2 2026 (fully-encrypted detection, DoH ID, QUIC SNI) — <https://sunsetbrowser.app/blog/china-gfw-update-2026-q2-en>
- REALITY protocol (XTLS/Xray-core) — <https://deepwiki.com/XTLS/Xray-core/3.2-reality-protocol>
- VLESS / REALITY / censorship bypass overview — <https://plisio.net/cybersecurity/vless-protocol>

---

## 11. Where obfuscation lives — value-rpc vs. servion (decision + seam)

value-rpc is consumed by the higher-level **servion** framework (dependency
injection / factory beans), which also drives **gRPC** and **HTTP** carriers. That
changes where the §10 work belongs, and §10 has been corrected to match.

### 11.1 The deciding axis

"Transport-layer" is not the same as "value-rpc". Traffic shaping and protocol
mimicry *feel* like transport work (they manipulate the byte stream), but they are
**carrier-agnostic** — gRPC, HTTP, and vRPC all leak operation sizes, all want the
same uTLS handshake, all benefit from the same padding. So the test is not *which
layer it touches* but:

> **Is it bound to one carrier's wire format, or generic across carriers?**

- **Generic across carriers ⇒ it must not live in value-rpc**, or gRPC/HTTP can't
  reach it without importing an RPC library they don't use.
- **Bound to value-rpc's own wire format ⇒ value-rpc.**

### 11.2 Three tiers

| Tier | Owns | Test |
|---|---|---|
| **value-rpc** | the transport **seam** (§11.3) + value-rpc's **own wire-format tells** | transport-layer **and** vRPC-specific |
| **`obfs` (standalone module)** | the obfuscation **primitives** — `net.Conn` middleware: length/timing/entropy shaping, cover traffic, uTLS mimicry, REALITY-style probe FSM, port hopping | transport-layer but **carrier-agnostic** |
| **servion** | **assembly** (DI/factory beans wiring `obfs` into each carrier) + the stateful **control plane** (moving-target rotation, rendezvous/broker, endpoint discovery) | spans carriers; needs config + lifecycle |

`obfs` is **standalone** (depends on nothing in the arpabet stack), not a servion
submodule: that keeps value-rpc and gRPC able to use it without adopting servion,
and avoids a dependency cycle (value-rpc sits *below* servion). servion *consumes*
`obfs`; it does not own it.

Note the dependency direction differs from `value-rpc/quic`: the QUIC module
*imports* value-rpc (it is a transport plugin), whereas `obfs` is orthogonal
middleware that never imports value-rpc. Both stay out of core; only QUIC is a
value-rpc-namespaced submodule.

### 11.3 The seam value-rpc provides (implemented)

value-rpc's whole job here is to be a clean, dependency-free **mounting point**.
That is now in place — no obfuscation code, no new dependencies, just the seam:

- **`NewMsgConn(conn io.ReadWriteCloser, timeout)`** — frames *any* byte stream
  (not only a `net.Conn`): an obfuscated PT connection, an `ssh.Channel`, a WebRTC
  data channel, an `io.Pipe`. Optional `net.Conn` deadline/address methods are
  used when present; otherwise `SetReadDeadline` is a no-op and `RemoteAddr` is
  empty. (`rpc.go`)
- **`NewFuncDialer(connect func() (io.ReadWriteCloser, error), timeout)`** — the
  general client seam: `connect` establishes (and on reconnect re-establishes) the
  obfuscated stream; value-rpc frames it. (`transport_conn.go`)
- **`NewSingleConnDialer(conn, timeout)`** — for a single connection handed over
  out of band (broker/rendezvous); a reconnect returns `ErrConnConsumed`.
- **`NewAcceptListener(accept, addr, stop, timeout)`** — the general server seam:
  `accept` yields connections produced out of band (a broker, an obfuscation layer
  wrapping a base listener); value-rpc frames each one.
- **`NewSingleConnListener(conn, addr, timeout)`** — serve over one externally
  established connection.
- These feed the existing **`valueserver.NewServerWithListener`** /
  **`valueclient.NewClientWithDialer`**, which servion already calls.

Canonical wiring (no value-rpc change needed for any obfuscator):

```go
dialer := valuerpc.NewFuncDialer(func() (io.ReadWriteCloser, error) {
    base, err := net.Dial("tcp", addr) // any base transport
    if err != nil {
        return nil, err
    }
    return obfs.Client(base, policy), nil // out-of-tree obfuscator returns a net.Conn
}, writeTimeout)
cli := valueclient.NewClientWithDialer(dialer)
```

### 11.4 Deferred: value-rpc's own wire-format tells

The one thing genuinely value-rpc's to own (per §10.1) is its own fingerprint: the
handshake magic (`"vRPC"`), the low-cardinality field tags, the MessagePack type
bytes. Hardening these (randomized/keyed framing, dropping the static magic) is
**deferred and low-priority**: it only matters for *plaintext* operation, and any
censored deployment must run under TLS + an `obfs` shaping layer anyway, which
encrypts and pads those bytes out of view. Documented here as a known item; no
code until a concrete plaintext-obfuscation need appears.
