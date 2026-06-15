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
- **Cancellation**, **timeouts**, and a **throttle**-based flow‑control mechanism.
- **Client session resumption** by client id across reconnects.
- **SOCKS5** client support; **Unix peer authentication** (`SO_PEERCRED`) via a connect‑authorizer hook.
- **Bounded frames** (`MaxFrameSize`), **keepalive**, **handshake deadline**,
  and **graceful shutdown** out of the box.
- Dependencies: `value`, `zap`, `golang.org/x/net`, `golang.org/x/sys`, `coder/websocket`, and `quic-go` (QUIC only).

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
    srv.AddFunction("greet",
        valuerpc.List(valuerpc.String), valuerpc.String,
        func(args value.Value) (value.Value, error) {
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
cli.SetTimeout(2000) // unary deadline, milliseconds

res, err := cli.CallFunction("greet", value.Tuple(value.Utf8("world")))
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
| Unary | unary | `AddFunction` | `func(args) (Value, error)` |
| Server stream | server‑streaming | `AddOutgoingStream` | `func(args) (<-chan Value, error)` |
| Client stream | client‑streaming | `AddIncomingStream` | `func(args, <-chan Value) error` |
| Chat | bidirectional | `AddChat` | `func(args, <-chan Value) (<-chan Value, error)` |

Client side: `CallFunction`, `GetStream`, `PutStream`, `Chat`.

```go
// Server streaming: close the channel to end the stream.
srv.AddOutgoingStream("count", valuerpc.List(valuerpc.Number),
    func(args value.Value) (<-chan value.Value, error) {
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

Package‑level knobs (set before constructing servers/clients):

| Variable | Default | Purpose |
|----------|---------|---------|
| `valuerpc.MaxFrameSize` | 16 MiB | Max inbound message size; `0` disables (maps to the WebSocket read limit) |
| `valueserver.HandshakeTimeout` | 10s | Deadline for a client to complete the handshake |
| `valueserver.KeepAlivePeriod` / `valueclient.KeepAlivePeriod` | 15s | TCP keepalive (ignored on Unix sockets) |
| `valuerpc.WSKeepAlive` | 15s | WebSocket ping interval (`0` disables) |
| `valuerpc.WSDialTimeout` | 30s | WebSocket opening-handshake timeout |
| `valueserver.OutgoingQueueCap` | 4096 | Per‑client server send buffer |
| `valueserver.IncomingQueueCap` | 4096 | Per‑request inbound stream buffer |
| `valueclient.DefaultTimeoutMls` | 1000 | Default client call timeout (ms) |

Per‑client: `cli.SetTimeout(ms)`, `cli.SetErrorHandler(...)`, `cli.SetMonitor(...)`,
`cli.CancelRequest(id)`, `cli.Stats()`.

For transport security and authentication: use **`wss://`** (TLS) via the embedded
WebSocket handler on your own TLS `http.Server`; use **Unix-socket peer
credentials** (below) for local authz; or wrap a TCP connection with `crypto/tls`.
There is otherwise no built‑in TLS or auth.

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
| QUIC | `quic://host:port` (needs a `*tls.Config`) | `NewQUICServer` / `NewQUICClient` |

```go
valueserver.NewServer("unix:///run/vrpc.sock", logger) // or NewUnixServer(path, logger)
valueserver.NewServer("ws://:9000/rpc", logger)        // or NewWebSocketServer(":9000", "/rpc", logger)
valueserver.NewTLSServer(":9000", tlsConf, logger)     // tls.Config with a server cert (+ ClientAuth for mTLS)
valueserver.NewQUICServer(":9000", tlsConf, logger)    // QUIC (TLS-mandatory); each request = its own stream
valueserver.NewServer("mem://billing", logger)         // or NewMemServer("billing", logger)
valueclient.NewClient("ws://host:9000/rpc", "")        // or NewWebSocketClient(url)
valueclient.NewTLSClient("host:9000", clientConf)      // or NewClient("tls://host:9000", "") for public CAs
valueclient.NewQUICClient("host:9000", clientConf)     // or NewClient("quic://host:9000", "")
valueclient.NewMemClient("billing")                    // or NewClient("mem://billing", "")
```

TCP and Unix sockets share the 4-byte length-prefix framing; WebSocket carries
one MessagePack message per **binary** frame (no length prefix); the in-memory
transport passes messages **by reference** (no sockets, no serialization —
same-process only); **QUIC** maps each request to its own stream (TLS 1.3 +
connection migration; see the note below). For full control, build a transport
yourself and pass it to `NewServerWithListener` / `NewClientWithDialer`. See
[TRANSPORTS.md](TRANSPORTS.md) for the design.

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

QUIC runs over UDP and **mandates TLS**, so `NewQUICServer` / `NewQUICClient`
take a `*tls.Config` exactly like the TLS transport (mutual TLS via `ClientAuth`
+ `ClientCAs`, with `valuerpc.PeerCertificates` in the connect-authorizer). On
top of that it adds **TLS 1.3, 0-RTT, and connection migration** (a connection
survives the client's IP changing), and it maps **each RPC request to its own
QUIC stream** — so requests have independent flow control and ordering on the
wire, and a cancelled request can drop its stream without disturbing others.

```go
srv, _ := valueserver.NewQUICServer(":9000", &tls.Config{
    Certificates: []tls.Certificate{cert}, // + ClientAuth/ClientCAs for mTLS
}, logger)
cli := valueclient.NewQUICClient("host:9000", &tls.Config{RootCAs: roots})
```

> **Scope note.** This is the "seam-fit" integration: requests are per-stream on
> the wire, but inbound frames still funnel through one read loop per connection,
> so *application-level* slow-consumer head-of-line blocking is reduced, not
> eliminated. Fully eliminating it needs the async per-request demux in
> [RESEARCH.md](RESEARCH.md) §5. QUIC also pulls in `github.com/quic-go/quic-go`
> (a larger dependency than the other transports).

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

Streaming/chat throughput is bounded by the millisecond‑granularity throttle
once a consumer falls behind; size the receive buffers for your workload.

## Project status

See [CHANGELOG.md](CHANGELOG.md) for release notes (the current release, **v1.2.0**,
adds the three transports plus a major hardening pass).

Pre‑1.0 in maturity. The library was recently hardened — see [FINDINGS.md](FINDINGS.md) for
the bugs that were found and fixed (crash, correctness, DoS, and lifecycle
issues) and [RESEARCH.md](RESEARCH.md) for how it compares to gRPC / WebSocket /
msgpack‑rpc and a high‑load/concurrency analysis. Known larger items still open:
`context.Context` propagation, server‑side SLA enforcement, TLS/auth, and a fully
async per‑request demux to remove slow‑consumer head‑of‑line blocking.

## License

[Apache‑2.0](LICENSE) © Karagatan LLC.
