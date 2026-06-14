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

## 9. Future transports (out of scope, but the seam enables them)

- **TLS over TCP**: a `tls.Listener`/`tls.Dial` wrapper around the stream
  transport (mutual-TLS auth) — no framing change.
- **QUIC / HTTP-3**: stream-per-request multiplexing at the transport layer
  (e.g. `quic-go`); larger change because vRPC currently multiplexes itself over
  one stream, but interesting for head-of-line-blocking-free streaming.
- **In-process / `net.Pipe`**: a zero-copy loopback transport for tests and
  same-process composition (the framing tests already use `net.Pipe`).
