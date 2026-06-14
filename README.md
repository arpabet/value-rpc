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
- Small dependency set: `value`, `zap`, `golang.org/x/net`, `golang.org/x/sys`, `coder/websocket`.

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

```go
valueserver.NewServer("unix:///run/vrpc.sock", logger) // or NewUnixServer(path, logger)
valueserver.NewServer("ws://:9000/rpc", logger)        // or NewWebSocketServer(":9000", "/rpc", logger)
valueclient.NewClient("ws://host:9000/rpc", "")        // or NewWebSocketClient(url)
```

TCP and Unix sockets share the 4-byte length-prefix framing; WebSocket carries
one MessagePack message per **binary** frame (no length prefix). For full control,
build a transport yourself and pass it to `NewServerWithListener` /
`NewClientWithDialer`. See [TRANSPORTS.md](TRANSPORTS.md) for the design.

WebSocket can also share a port with your other HTTP routes (and serve `wss://`
from your own TLS server) by mounting the vRPC handler on your `http.ServeMux`:

```go
srv, handler, _ := valueserver.NewWebSocketHandler(logger)
srv.AddFunction(...)
go srv.Run()
mux.Handle("/rpc", handler) // alongside /healthz, REST, metrics, …
```

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

Pre‑1.0. The library was recently hardened — see [FINDINGS.md](FINDINGS.md) for
the bugs that were found and fixed (crash, correctness, DoS, and lifecycle
issues) and [RESEARCH.md](RESEARCH.md) for how it compares to gRPC / WebSocket /
msgpack‑rpc and a high‑load/concurrency analysis. Known larger items still open:
`context.Context` propagation, server‑side SLA enforcement, TLS/auth, and a fully
async per‑request demux to remove slow‑consumer head‑of‑line blocking.

## License

[Apache‑2.0](LICENSE) © Karagatan LLC.
