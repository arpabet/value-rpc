# value-rpc

[![Value-RPC CI](https://github.com/arpabet/value-rpc/actions/workflows/build.yaml/badge.svg)](https://github.com/arpabet/value-rpc/actions/workflows/build.yaml)

**vRPC** is a small, schemaless RPC framework for Go that runs over a raw TCP
connection. It offers the same four interaction patterns as gRPC — unary,
server‑streaming, client‑streaming, and bidirectional — but **without an IDL or
code generation**: handlers take and return [`value`](https://github.com/arpabet/value)
values (a deterministic MessagePack model) and argument types are checked at
runtime.

```
net.Conn (TCP, optional SOCKS5)
  └─ length-prefixed frames  [4-byte BE length][payload]
       └─ value.Pack / value.Unpack  (MessagePack)
            └─ message = value.Map { t, rid, fn, args, res, val, err, ... }
                 └─ unary · server-stream · client-stream · chat
```

## Features

- **No codegen, no `.proto`.** Register Go functions, call them by name.
- **Four call patterns** multiplexed over a single connection (keyed by request id).
- **Runtime type checking** via `TypeDef` / `Verify` (`Arg`, `List`, `Map`, `Void`, `Any`).
- **Cancellation**, **timeouts**, and a **throttle**-based flow‑control mechanism.
- **Client session resumption** by client id across reconnects.
- **SOCKS5** client support.
- **Bounded frames** (`MaxFrameSize`), **TCP keepalive**, **handshake deadline**,
  and **graceful shutdown** out of the box.
- Tiny dependency set: `value`, `zap`, `golang.org/x/net`.

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

Runnable, output‑checked versions of all patterns are in
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
| `valuerpc.MaxFrameSize` | 16 MiB | Max inbound message size; `0` disables the check |
| `valueserver.HandshakeTimeout` | 10s | Deadline for a client to complete the handshake |
| `valueserver.KeepAlivePeriod` | 15s | TCP keepalive on accepted connections |
| `valueclient.KeepAlivePeriod` | 15s | TCP keepalive on dialed connections |
| `valueserver.OutgoingQueueCap` | 4096 | Per‑client server send buffer |
| `valueserver.IncomingQueueCap` | 4096 | Per‑request inbound stream buffer |
| `valueclient.DefaultTimeoutMls` | 1000 | Default client call timeout (ms) |

Per‑client: `cli.SetTimeout(ms)`, `cli.SetErrorHandler(...)`, `cli.SetMonitor(...)`,
`cli.CancelRequest(id)`, `cli.Stats()`.

There is no built‑in TLS or authentication; wrap the connection (e.g. with
`crypto/tls`) or run behind a mesh if you need them.

## Transports

The RPC layer is decoupled from the wire transport behind a small seam
(`valuerpc.Listener` / `valuerpc.Dialer` / `valuerpc.MsgConn`). `NewServer` /
`NewClient` use **TCP**; for anything else, build a transport and pass it to the
explicit constructors:

```go
// Unix domain socket (the generic stream transport, network "unix")
lis, _ := valuerpc.NewStreamListener("unix", "/run/vrpc.sock",
    valueserver.KeepAlivePeriod, valueserver.DefaultTimeout)
srv, _ := valueserver.NewServerWithListener(lis, logger)

cli := valueclient.NewClientWithDialer(
    valuerpc.NewStreamDialer("unix", "/run/vrpc.sock", "",
        valueclient.KeepAlivePeriod, valueclient.DefaultTimeout))
```

TCP and Unix sockets share the length-prefix framing and work today. Ergonomic
`unix://` / `ws://` address parsing and a **WebSocket (MessagePack)** transport
are planned — see [TRANSPORTS.md](TRANSPORTS.md) for the design and roadmap.

## Performance

Microbenchmarks on an Apple‑silicon laptop, Go 1.25 (`make` / `go test -bench=.`).
Numbers are indicative; run `go test -bench=. ./...` on your hardware.

| Benchmark | Result |
|-----------|--------|
| Pack+Unpack a small request | ~0.6 µs, 53 allocs |
| Frame codec write+read (in‑memory) | ~1.8 µs |
| Unary call, loopback, serial | ~25 µs (~40k calls/s) |
| Unary call, loopback, parallel | ~5 µs (~190k calls/s) |
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
