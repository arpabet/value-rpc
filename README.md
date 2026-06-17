# value-rpc

[![Value-RPC CI](https://github.com/arpabet/value-rpc/actions/workflows/build.yaml/badge.svg)](https://github.com/arpabet/value-rpc/actions/workflows/build.yaml)

**vRPC** is a small, schemaless RPC framework for Go. It offers the same four
interaction patterns as gRPC — unary, server‑streaming, client‑streaming, and
bidirectional — but **without an IDL or code generation**: handlers take and
return [`value`](https://github.com/arpabet/value) values (a deterministic
MessagePack model) and argument types are checked at runtime. It runs over
**TCP, Unix domain sockets, or WebSocket** behind one API.

```
transport:  TCP  ·  Unix socket  ·  WebSocket        (optional SOCKS5 / wss TLS)
  └─ framing: [4-byte BE length][payload]  (TCP/Unix)  |  one binary frame  (WebSocket)
       └─ value.Pack / value.Unpack  (MessagePack)
            └─ message = value.Map { t, rid, fn, args, res, val, err, ... }
                 └─ unary · server-stream · client-stream · chat
```

## Features

- **No codegen, no `.proto`.** Register Go functions, call them by name.
- **Four call patterns** multiplexed over a single connection (keyed by request id).
- **Pluggable transports**: TCP, Unix domain sockets, and WebSocket (MessagePack) — one API, pick by address scheme.
- **Runtime type checking** via `TypeDef` / `Verify` (`Arg`, `List`, `Map`, `Void`, `Any`).
- **Machine-readable error codes**: failures carry a `valuerpc.Code` (NotFound,
  InvalidArgument, ResourceExhausted, Unavailable, Internal, …). Handlers return
  `valuerpc.NewError(code, …)`; callers branch with `valuerpc.CodeOf(err)` or
  `errors.As` instead of string-matching.
- **Cancellation**, **timeouts**, and **credit‑based flow control** (bidirectional):
  a sender only sends what the receiver has granted credit for, so a fast producer
  can't overrun a slow consumer — delivery is **lossless**, buffering is **bounded**,
  and the shared connection loop never blocks.
- **`context.Context` in every handler**, cancelled on disconnect, shutdown,
  client cancel, or the client's SLA deadline — for deadline/cancellation
  propagation to downstream work.
- **No head‑of‑line blocking**: each stream is delivered through a non‑blocking
  per‑request pump, so one slow consumer can't stall other multiplexed requests.
- **Structured logging** via `*zap.Logger` on both server and client (the client
  takes it with `valueclient.WithLogger(logger)`; silent by default).
- **Pluggable metrics** (`valuerpc.Metrics` via `valueclient.WithMetrics`):
  request/error counters (by code), in-flight gauge, latency, reconnects, and
  stream throughput — wire it to Prometheus/OpenTelemetry. No-op by default.
- **Metadata / trace-context propagation**: the client injects per-request
  metadata from the call's context (`valueclient.WithMetadata`) into the envelope;
  the server surfaces it on the handler's context (`valuerpc.MetadataFromContext`)
  and can enrich that context via `valueserver.WithMetadataExtractor` — the
  dependency-free seam for OpenTelemetry / W3C `traceparent`.
- **Context-aware, bounded dial**: `cli.ConnectContext(ctx)` cancels/bounds the
  dial; without a context deadline a default `WithDialTimeout` applies, so connect
  never hangs on an unreachable peer.
- **Authenticated session resumption**: the server issues a per-session token at
  handshake; reconnecting with the matching token resumes the session, so a peer
  can't take it over by guessing the client id.
- **SOCKS5** client support; **Unix peer authentication** (`SO_PEERCRED`) via a connect‑authorizer hook.
- **Bounded frames** (`MaxFrameSize`), **keepalive**, **handshake deadline**,
  and **graceful shutdown** out of the box.
- Core dependencies: `value`, `zap`, `golang.org/x/net`, `golang.org/x/sys`, `coder/websocket`. QUIC lives in a **separate module** (`go.arpabet.com/value-rpc/quic`) so `quic-go` is pulled in only if you use it.

## Install

```sh
go get go.arpabet.com/value-rpc@latest
```

Requires **Go 1.25+**.

## Quick start

### Server

```go
package main

import (
    "context"

    "go.arpabet.com/value"
    "go.arpabet.com/value-rpc/valuerpc"
    "go.arpabet.com/value-rpc/valueserver"
    "go.uber.org/zap"
)

func main() {
    logger, _ := zap.NewProduction()
    srv, err := valueserver.NewServer(":9000", logger)
    if err != nil {
        panic(err)
    }
    defer srv.Close()

    // args: [name] (one string)   result: string
    // ctx is cancelled on disconnect, shutdown, client cancel, or SLA deadline.
    srv.AddFunction("greet",
        valuerpc.List(valuerpc.String), valuerpc.String,
        func(ctx context.Context, args value.Value) (value.Value, error) {
            name := args.(value.List).GetStringAt(0).String()
            return value.Utf8("Hello, " + name + "!"), nil
        })

    if err := srv.Run(); err != nil { // blocks
        panic(err)
    }
}
```

### Client

```go
cli := valueclient.NewClient("localhost:9000", "" /* or a socks5 addr */)
if err := cli.Connect(); err != nil {
    panic(err)
}
defer cli.Close()
cli.SetTimeout(2000) // default unary deadline, milliseconds

// Every call takes a context; pass context.Background() for none. A context
// deadline sooner than SetTimeout becomes the request SLA, and cancelling the
// context cancels the call (and, for streams, tears the stream down).
res, err := cli.CallFunction(context.Background(), "greet", value.Tuple(value.Utf8("world")))
if err != nil {
    panic(err)
}
fmt.Println(res.(value.String).String()) // Hello, world!
```

The example above uses TCP; the identical code works over a Unix socket or
WebSocket by changing only the address — see [Transports](#transports). Runnable,
output‑checked examples for every pattern and transport are in
[`valueserver/example_test.go`](valueserver/example_test.go) (they also render on
pkg.go.dev), and a full end‑to‑end demo is in [`example/sample.go`](example/sample.go)
(`make run`).

## The four patterns

| Pattern | gRPC analogue | Server registration | Server handler |
|---------|---------------|----------------------|----------------|
| Unary | unary | `AddFunction` | `func(ctx, args) (Value, error)` |
| Server stream | server‑streaming | `AddOutgoingStream` | `func(ctx, args) (<-chan Value, error)` |
| Client stream | client‑streaming | `AddIncomingStream` | `func(ctx, args, <-chan Value) error` |
| Chat | bidirectional | `AddChat` | `func(ctx, args, <-chan Value) (<-chan Value, error)` |

Client side: `CallFunction`, `GetStream`, `PutStream`, `Chat`.

```go
// Server streaming: close the channel to end the stream.
srv.AddOutgoingStream("count", valuerpc.List(valuerpc.Number),
    func(ctx context.Context, args value.Value) (<-chan value.Value, error) {
        n := args.(value.List).GetNumberAt(0).Long()
        out := make(chan value.Value)
        go func() {
            defer close(out)
            for i := int64(1); i <= n; i++ {
                out <- value.Long(i)
            }
        }()
        return out, nil
    })

readC, reqID, _ := cli.GetStream("count", value.Tuple(value.Long(3)), 64)
for v := range readC {
    fmt.Println(v.(value.Number).Long())
}
_ = reqID // pass to cli.CancelRequest(reqID) to cancel early
```

For chat you may close the send channel and keep receiving — the server's output
stream stays open until it finishes.

## Type definitions

Arguments and results are validated against a `valuerpc.TypeDef`:

```go
valuerpc.Void                                   // no args / no result
valuerpc.Any                                    // anything, including nil
valuerpc.String / .Number / .Bool               // a single required value
valuerpc.StringOpt / .NumberOpt / .BoolOpt      // optional
valuerpc.List(valuerpc.String, valuerpc.Number) // positional args [string, number]
valuerpc.Map(valuerpc.Param("name", value.STRING, true)) // named params
```

## Configuration

Most behavioral knobs are **per-instance functional options** passed to the
constructors; the package-level variables below are the *defaults* an option
overrides (so two servers/clients in one process can differ, and nothing is read
from a mutable global at runtime):

```go
srv, _ := valueserver.NewServer(":9000", logger,
    valueserver.WithMaxConnections(1000),
    valueserver.WithMaxConcurrentRequests(256),
    valueserver.WithMaxConcurrentStreams(64),
    valueserver.WithHandshakeTimeout(5*time.Second))

cli := valueclient.NewClient("host:9000", "",
    valueclient.WithTimeout(2000),
    valueclient.WithSendingCap(2048))
```

Server options: `WithMaxConnections`, `WithMaxConcurrentRequests`,
`WithMaxConcurrentStreams`, `WithOutgoingQueueCap`, `WithIncomingQueueCap`,
`WithStreamMaxPending`, `WithHandshakeTimeout`, `WithKeepAlivePeriod`,
`WithWriteTimeout`, `WithMaxFrameSize`. Client options: `WithSendingCap`,
`WithTimeout`, `WithStreamMaxPending`, `WithKeepAlivePeriod`, `WithWriteTimeout`,
`WithMaxFrameSize`. (The transport-level options — keepalive, write timeout, max
frame size — apply when a convenience constructor builds the listener/dialer;
when you bring your own via `NewServerWithListener`/`NewClientWithDialer`, set
them on the listener/dialer.)

The package-level variables below are the defaults for the options above;
`MaxFrameSize` is now captured per connection at construction (never read from
the global at runtime):

| Variable | Default | Purpose |
|----------|---------|---------|
| `valuerpc.MaxFrameSize` | 16 MiB | Max inbound message size; `0` disables (maps to the WebSocket read limit) |
| `valueserver.HandshakeTimeout` | 10s | Deadline for a client to complete the handshake |
| `valueserver.KeepAlivePeriod` / `valueclient.KeepAlivePeriod` | 15s | TCP keepalive (ignored on Unix sockets) |
| `valuerpc.WSKeepAlive` | 15s | WebSocket ping interval (`0` disables) |
| `valuerpc.WSDialTimeout` | 30s | WebSocket opening-handshake timeout |
| `valueserver.OutgoingQueueCap` | 4096 | Per‑client server send buffer |
| `valueserver.IncomingQueueCap` | 4096 | Per‑request inbound stream buffer |
| `valueserver.MaxConcurrentRequests` | 4096 | Max concurrent request handlers per connection; over the limit requests are rejected with an error (`0` disables) |
| `valueserver.MaxConnections` | 0 | Max simultaneously open connections; over the limit new connections are closed (`0` disables) |
| `valueserver.MaxConcurrentStreams` | 0 | Max open streams (get/put/chat) per connection; over the limit stream requests are rejected with an error (`0` disables) |
| `valuerpc.DefaultMaxPending` | 4096 | Per‑stream pending‑queue bound before a slow consumer is failed |
| `valueclient.DefaultTimeoutMls` | 1000 | Default client call timeout (ms) |

Per‑client: `cli.SetTimeout(ms)`, `cli.SetErrorHandler(...)`, `cli.SetMonitor(...)`,
`cli.CancelRequest(id)`, `cli.Stats()`.

For transport security and authentication: use the **`tls://`** transport or
**`wss://`** for encryption; **Unix-socket peer credentials** (below) for local
authz; a **connect-authorizer** (`SetConnectAuthorizer`) for pre-handshake checks
on the connection (TLS cert, peer creds); or a **handshake authenticator**
(`SetAuthenticator`) to validate a client credential carried in the handshake
(bearer token, API key) — the client attaches it with `cli.SetCredential(...)`:

```go
srv.SetAuthenticator(func(conn valuerpc.MsgConn, cred value.Value) error {
    if cred == nil || cred.Kind() != value.STRING || cred.(value.String).String() != token {
        return errors.New("unauthorized")
    }
    return nil
})
// client:
cli.SetCredential(value.Utf8(token))
```

## Transports

The RPC layer is decoupled from the wire transport behind a small seam
(`valuerpc.Listener` / `valuerpc.Dialer` / `valuerpc.MsgConn`). `NewServer` /
`NewClient` accept a scheme in the address; a bare `host:port` is **TCP**:

| Transport | Address | Convenience constructor |
|-----------|---------|--------------------------|
| TCP | `host:port` or `tcp://host:port` | `NewServer` / `NewClient` |
| Unix socket | `unix:///path.sock` | `NewUnixServer` / `NewUnixClient` |
| WebSocket | `ws://host:port/path` (client also `wss://`) | `NewWebSocketServer` / `NewWebSocketClient` |
| TLS / mTLS | `tls://host:port` (needs a `*tls.Config`) | `NewTLSServer` / `NewTLSClient` |
| In-memory | `mem://name` (same process) | `NewMemServer` / `NewMemClient` |
| QUIC | (separate module, needs a `*tls.Config`) | `valuequic.NewServer` / `valuequic.NewClient` |

```go
valueserver.NewServer("unix:///run/vrpc.sock", logger) // or NewUnixServer(path, logger)
valueserver.NewServer("ws://:9000/rpc", logger)        // or NewWebSocketServer(":9000", "/rpc", logger)
valueserver.NewTLSServer(":9000", tlsConf, logger)     // tls.Config with a server cert (+ ClientAuth for mTLS)
valueserver.NewServer("mem://billing", logger)         // or NewMemServer("billing", logger)
valueclient.NewClient("ws://host:9000/rpc", "")        // or NewWebSocketClient(url)
valueclient.NewTLSClient("host:9000", clientConf)      // or NewClient("tls://host:9000", "") for public CAs
valueclient.NewMemClient("billing")                    // or NewClient("mem://billing", "")

// QUIC is in its own module: import go.arpabet.com/value-rpc/quic
valuequic.NewServer(":9000", tlsConf, logger)          // each request = its own QUIC stream
valuequic.NewClient("host:9000", clientConf)
```

For full control — including obfuscation and custom tunnels — build a transport
yourself and pass it to `NewServerWithListener` / `NewClientWithDialer`; see
[Custom transports](#custom-transports-bring-your-own-connection) below and
[TRANSPORTS.md](TRANSPORTS.md) for the design.

### Comparison

All transports speak the identical MessagePack message protocol and support all
four call patterns; they differ only in reach, security, framing, and
dependencies:

| Transport | Reach | Encryption | Client auth | Wire framing | Extra dependency | Best for |
|-----------|-------|------------|-------------|--------------|------------------|----------|
| **TCP** | host ↔ host | — (add TLS) | — (SOCKS5 proxy only) | 4-byte length prefix | none | general networked RPC |
| **Unix socket** | same host | local-only | **OS uid/gid/pid** (peer creds) | 4-byte length prefix | none | local IPC: sidecars, agents, CLI ↔ daemon |
| **WebSocket** | host ↔ host, through proxies/firewalls, browsers | `wss://` (TLS) | HTTP headers/cookies; mTLS on your TLS server | one binary frame / message | `coder/websocket` | sharing a port with HTTP, traversing 443 |
| **TLS / mTLS** | host ↔ host | **TLS 1.3** | **X.509 cert** (mutual TLS) | 4-byte length prefix | none (stdlib) | encrypted, certificate-authenticated TCP |
| **in-memory** | same process | n/a | n/a | none (by reference) | none | tests; a monolith you can later split |
| **QUIC** | host ↔ host | **TLS 1.3** (mandatory) | **X.509 cert** (mutual TLS) | one QUIC **stream per request** | `quic-go` (separate module) | mobility/roaming, per-request streams |

Notes:
- **Multiplexing:** every transport except QUIC multiplexes all concurrent
  requests over a single connection (keyed by request id); QUIC additionally
  gives each request its own stream with independent flow control.
- **Plain TCP and Unix sockets are unencrypted** — use TLS or QUIC over the
  network, and filesystem permissions (plus peer creds) for Unix sockets.
- **Dependency footprint:** the core module needs none of the heavy ones; only
  WebSocket pulls `coder/websocket` (zero-dep) and only QUIC pulls `quic-go`
  (and it's a separate module, so non-QUIC users never compile it).

WebSocket can also share a port with your other HTTP routes (and serve `wss://`
from your own TLS server) by mounting the vRPC handler on your `http.ServeMux`:

```go
srv, handler, _ := valueserver.NewWebSocketHandler(logger)
srv.AddFunction(...)
go srv.Run()
mux.Handle("/rpc", handler) // alongside /healthz, REST, metrics, …
```

### TLS and mutual TLS

Plain TCP is unencrypted and unauthenticated — anyone on the network path can
read the traffic, and the server has no idea who connected. TLS fixes the first
half; **mutual TLS (mTLS)** fixes both.

| | What it proves | Encrypted? | Who is authenticated |
|---|---|---|---|
| Plain TCP | nothing | ❌ | nobody |
| **TLS** | client checks the **server's** cert (like HTTPS) | ✅ | the server only |
| **mTLS** | client checks the server **and** server checks the **client's** cert | ✅ | **both sides** |

`valueserver.NewTLSServer` / `valueclient.NewTLSClient` each take a standard
`*tls.Config`.

**TLS (server authentication).** The client confirms it's talking to the real
server and the link is encrypted; the server still doesn't know who the client is.

```go
import ("crypto/tls"; "crypto/x509"; "os")

// Server presents its certificate.
cert, _ := tls.LoadX509KeyPair("server.crt", "server.key")
srv, _ := valueserver.NewTLSServer(":9000",
    &tls.Config{Certificates: []tls.Certificate{cert}}, logger)

// Client trusts the CA that signed it (omit RootCAs for a public/Let's Encrypt CA).
caPEM, _ := os.ReadFile("ca.crt")
roots := x509.NewCertPool()
roots.AppendCertsFromPEM(caPEM)
cli := valueclient.NewTLSClient("server.example:9000", &tls.Config{RootCAs: roots})
```

**mTLS (both sides authenticated).** The server *requires* a client certificate
signed by a CA it trusts, so an unknown client is rejected during the TLS
handshake — before any RPC runs. You then authorize by the client's certified
identity:

```go
// Server: present a cert AND require + verify a client cert.
serverCert, _ := tls.LoadX509KeyPair("server.crt", "server.key")
clientCAs := x509.NewCertPool()
clientCAs.AppendCertsFromPEM(caPEM)

srv, _ := valueserver.NewTLSServer(":9000", &tls.Config{
    Certificates: []tls.Certificate{serverCert},
    ClientCAs:    clientCAs,
    ClientAuth:   tls.RequireAndVerifyClientCert, // <- the "mutual" part
}, logger)

// Allow/deny by the verified client identity (the analogue of Unix peer creds).
srv.SetConnectAuthorizer(func(conn valuerpc.MsgConn) error {
    certs, ok := valuerpc.PeerCertificates(conn)
    if !ok || certs[0].Subject.CommonName != "billing-service" {
        return fmt.Errorf("client not allowed")
    }
    return nil // accept
})

// Client: present its own certificate and trust the server's CA.
clientCert, _ := tls.LoadX509KeyPair("client.crt", "client.key")
cli := valueclient.NewTLSClient("server.example:9000", &tls.Config{
    RootCAs:      roots,
    Certificates: []tls.Certificate{clientCert},
})
```

**Why prefer mTLS for service-to-service RPC?**

- **Both ends are proven.** The server knows *exactly* which service connected
  (its certificate identity), not just "someone who reached the port".
- **No shared secrets to leak.** No API keys or passwords to embed, rotate, or
  accidentally commit — identity is a certificate signed by your CA.
- **Network position isn't enough.** An attacker who can reach the port still
  can't open a connection without a valid client cert; it's refused at the
  handshake, before a single request is processed.
- **Zero-trust by default.** Every connection is mutually authenticated and
  encrypted, with no implicit trust from "being on the internal network".
- **Identity drives authorization.** `valuerpc.PeerCertificates` hands the
  verified cert to your `SetConnectAuthorizer`, so per-client allow/deny is a few
  lines (mTLS is the network analogue of the Unix peer-credential check below).

> The `tls://` scheme via plain `NewClient("tls://host:443", "")` verifies against
> the system root CAs — handy for publicly-trusted servers. For local testing
> only, `&tls.Config{InsecureSkipVerify: true}` skips verification — never use it
> in production. Generate dev certs with `mkcert` or `openssl`.

### Unix peer authentication

On a Unix socket the connecting process can be authorized by its
kernel-reported credentials (uid/gid/pid), which the peer cannot forge:

```go
srv.SetConnectAuthorizer(func(conn valuerpc.MsgConn) error {
    cred, ok := valuerpc.PeerCredOf(conn)
    if !ok || cred.UID != uint32(os.Getuid()) {
        return fmt.Errorf("peer uid %d not allowed", cred.UID)
    }
    return nil // accept the connection
})
```

### In-memory composition

`mem://` connects a client and server **in the same process** over Go channels —
no sockets and no serialization (messages pass by reference). It's ideal for fast,
deterministic tests, and for building a monolith as in-process services that you
can later split onto a real transport by changing **only the address**:

```go
// today: everything in one binary
srv, _ := valueserver.NewMemServer("billing", logger)
cli := valueclient.NewMemClient("billing")

// later: billing moves to its own host — call sites are unchanged
// srv, _ := valueserver.NewTLSServer(":9000", tlsConf, logger)
// cli := valueclient.NewTLSClient("billing.internal:9000", clientConf)
```

### QUIC (per-request streams)

QUIC lives in its **own module** so its heavyweight dependency
(`github.com/quic-go/quic-go`) only enters builds that use it:

```sh
go get go.arpabet.com/value-rpc/quic
```

```go
import valuequic "go.arpabet.com/value-rpc/quic"
```

QUIC runs over UDP and **mandates TLS**, so `valuequic.NewServer` /
`valuequic.NewClient` take a `*tls.Config` exactly like the TLS transport (mutual
TLS via `ClientAuth` + `ClientCAs`, with `valuerpc.PeerCertificates` in the
connect-authorizer). On top of that it adds **TLS 1.3, 0-RTT, and connection
migration** (a connection survives the client's IP changing), and it maps **each
RPC request to its own QUIC stream** — so requests have independent flow control
and ordering on the wire, and a cancelled request can drop its stream without
disturbing others.

```go
import (
    "crypto/tls"
    "go.arpabet.com/value"
    valuequic "go.arpabet.com/value-rpc/quic"
    "go.arpabet.com/value-rpc/valuerpc"
)

// Server — QUIC requires a certificate (add ClientAuth + ClientCAs for mTLS).
cert, _ := tls.LoadX509KeyPair("server.crt", "server.key")
srv, err := valuequic.NewServer(":9000",
    &tls.Config{Certificates: []tls.Certificate{cert}}, logger)
if err != nil {
    panic(err)
}
defer srv.Close()
srv.AddFunction("greet", valuerpc.List(valuerpc.String), valuerpc.String,
    func(ctx context.Context, args value.Value) (value.Value, error) {
        return value.Utf8("Hello, " + args.(value.List).GetStringAt(0).String() + "!"), nil
    })
go srv.Run()

// Client — trust the server's CA (omit RootCAs for a publicly-trusted cert).
cli := valuequic.NewClient("host:9000", &tls.Config{RootCAs: roots})
if err := cli.Connect(); err != nil {
    panic(err)
}
defer cli.Close()
cli.SetTimeout(5000)

res, _ := cli.CallFunction("greet", value.Tuple(value.Utf8("quic")))
fmt.Println(res.(value.String).String()) // Hello, quic!
```

For **mutual TLS** over QUIC, give the server `ClientAuth: tls.RequireAndVerifyClientCert`
+ `ClientCAs`, give the client a `Certificates` entry, and read the verified
identity with `valuerpc.PeerCertificates` in `srv.SetConnectAuthorizer` — exactly
as in the [TLS section](#tls-and-mutual-tls). Streaming works the same too: e.g.
`cli.GetStream(...)` server-streams over its own QUIC stream.

> **Scope note.** This is the "seam-fit" integration: requests are per-stream on
> the wire, but inbound frames still funnel through one read loop per connection,
> so *application-level* slow-consumer head-of-line blocking is reduced, not
> eliminated. Fully eliminating it needs the async per-request demux in
> [RESEARCH.md](RESEARCH.md) §5.

### Custom transports (bring your own connection)

The transport seam takes **any byte stream**, not just the built-in schemes.
`valuerpc.NewMsgConn` frames an arbitrary `io.ReadWriteCloser` — a tunnel, an
`ssh.Channel`, a WebRTC data channel, or a connection produced by an external
obfuscation / pluggable-transport layer — and four adapters turn it into a
`Dialer`/`Listener` for `NewClientWithDialer` / `NewServerWithListener`:

| Adapter | Use |
|---------|-----|
| `NewFuncDialer(connect, timeout)` | client dials (and reconnects) via your `connect func() (io.ReadWriteCloser, error)` |
| `NewSingleConnDialer(conn, timeout)` | client runs over one connection handed over out of band (broker/rendezvous) |
| `NewAcceptListener(accept, addr, stop, timeout)` | server accepts connections produced out of band (a broker, or a wrapper around a base listener) |
| `NewSingleConnListener(conn, addr, timeout)` | server runs over a single externally established connection |

This is the integration point for **obfuscation / censorship-resistant
transports**, which stay out of value-rpc as separate modules and simply hand it a
shaped `net.Conn`:

```go
// Wrap any base transport with an out-of-tree obfuscator, then frame with value-rpc.
dialer := valuerpc.NewFuncDialer(func() (io.ReadWriteCloser, error) {
    base, err := net.Dial("tcp", addr) // any base transport
    if err != nil {
        return nil, err
    }
    return obfs.Wrap(base, policy), nil // e.g. go.arpabet.com/obfs (traffic shaping)
}, valueclient.DefaultTimeout)
cli := valueclient.NewClientWithDialer(dialer)
```

value-rpc itself carries **no obfuscation code or dependencies** — it only
provides this seam. The threat model, technique survey, and the layering decision
(value-rpc seam vs. a standalone `obfs` module vs. higher-level orchestration) are
in [TRANSPORTS.md](TRANSPORTS.md) §10–§11.

## Performance

Microbenchmarks on an Apple‑silicon laptop, Go 1.25 (`make` / `go test -bench=.`).
Numbers are indicative; run `go test -bench=. ./...` on your hardware.

| Benchmark | Result |
|-----------|--------|
| Pack+Unpack a small request | ~0.6 µs, 53 allocs |
| Frame codec write+read (in‑memory) | ~1.8 µs |
| Unary call, loopback, serial (TCP) | ~25 µs (~40k calls/s) |
| Unary call, loopback, parallel (TCP) | ~5 µs (~190k calls/s) |
| Unary call, loopback, serial (WebSocket) | ~30 µs (~22% over TCP) |
| Server stream, per value | ~2 µs (~500k values/s) |

Streaming uses **credit‑based flow control**: the receiver grants the sender an
initial window (`valuerpc.DefaultMaxPending`, tunable per side) and replenishes
credit as it delivers values to the consumer; the sender blocks (only its own
per‑stream goroutine, never the shared loop) when it runs out of credit. So a
fast producer can't overrun a slow consumer — delivery stays **lossless** with
**bounded** buffering and no head‑of‑line blocking; a slow or stuck consumer
simply stalls its own stream. Only a peer that *ignores* its credit and overruns
the buffer has that one stream failed — with an explicit error to that peer —
rather than pinning unbounded memory or silently dropping values. Size the window
(`WithStreamMaxPending`) for your bandwidth‑delay product.

## Project status

See [CHANGELOG.md](CHANGELOG.md) for release notes (the current release, **v1.2.0**,
adds the three transports plus a major hardening pass).

Pre‑1.0 in maturity. The library was recently hardened — see [FINDINGS.md](FINDINGS.md) for
the bugs that were found and fixed (crash, correctness, DoS, and lifecycle
issues) and [RESEARCH.md](RESEARCH.md) for how it compares to gRPC / WebSocket /
msgpack‑rpc and a high‑load/concurrency analysis. Slow‑consumer head‑of‑line
blocking has been resolved with a per‑request `StreamPump`, and streaming uses
**credit‑based flow control** (lossless, bounded, non‑HOL); session resumption is
authenticated with a server‑issued token; handlers now receive a
`context.Context` (cancelled on disconnect/shutdown/cancel and carrying the
client's SLA deadline) — and the client API takes a context on every call for
deadline/cancellation propagation; and `MaxConcurrentRequests` / `MaxConnections`
/ `MaxConcurrentStreams` caps bound handler goroutines, connections, and open
streams under a flood; and a handshake `Authenticator` hook (plus the
connect-authorizer) gates connections by client credential. Known larger items
still open: binding the client id to a verified principal for resumption, and
forced cancellation of handlers that ignore their context.

## License

[BUSL‑1.1](LICENSE) © Karagatan LLC.
