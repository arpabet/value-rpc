# value-rpc — Research & Architecture Notes

Date: 2026-06-13. Companion to [FINDINGS.md](FINDINGS.md) (bugs) and the test
suite added under `valuerpc/`, `valueclient/`, `valueserver/`.

## 1. What this library is

`value-rpc` ("vRPC") is a **small, schemaless RPC framework** with pluggable
transports — **TCP, Unix domain sockets, and WebSocket** (see
[TRANSPORTS.md](TRANSPORTS.md)). It is the transport/RPC layer built on top of
the [`value`](https://github.com/arpabet/value) library, which provides a
deterministic MessagePack value model (`value.Map`, `value.List`,
`value.Number`, `value.String`, …).

The stack, bottom to top:

```
transport: TCP · Unix socket · WebSocket   (optional SOCKS5 / wss TLS)
  └─ framing: 4-byte length prefix (TCP/Unix) | one binary frame (WebSocket)
       └─ value.Pack / value.Unpack  (MessagePack, canonical)
            └─ message = value.Map with well-known fields (t, rid, fn, args, …)
                 └─ four interaction patterns
```

Message envelope (`valuerpc/protocol.go`): every message is a MessagePack map
with short keys — `t` (message type), `rid` (request id), `fn` (function name),
`args`, `res`, `err`, `val` (stream payload), `sla` (timeout), `cid` (client id),
`m`/`v` (magic/version for the handshake).

**Four interaction patterns** (the same quartet gRPC offers):

| value-rpc | gRPC equivalent | Server signature |
|-----------|-----------------|------------------|
| `CallFunction` | unary | `func(args) (Value, error)` |
| `GetStream` | server-streaming | `func(args) (<-chan Value, error)` |
| `PutStream` | client-streaming | `func(args, <-chan Value) error` |
| `Chat` | bidirectional | `func(args, <-chan Value) (<-chan Value, error)` |

**Distinguishing features**

- **No code generation, no IDL.** Handlers take and return `value.Value`. Types
  are validated at runtime via `TypeDef`/`Verify` (`Arg`, `List`, `Map`, `Void`,
  `Any`). This is the central trade-off: less ceremony, no compile-time safety.
- **Multiplexing over one connection**, keyed by `rid`, so many concurrent
  calls and streams share a socket.
- **Session identity / resumption**: the client picks a random `cid`; on the
  first handshake the server issues a per-session token and re-attaches to the
  same `servingClient` on reconnect only when the client presents the matching
  token (its in-flight request state survives a dropped socket). The token gates
  resumption so a peer cannot hijack a session by reusing another client's `cid`.
- **Built-in flow control**: `ThrottleIncrease`/`ThrottleDecrease` messages let a
  consumer slow a producer (a sleep-based brake today).
- **Cancellation**: `CancelRequest`.
- **Determinism inherited from `value`**: identical messages serialize to
  identical bytes — useful for hashing, signing, or content-addressing payloads.

**Maturity:** pre-1.0, single author, BUSL-1.1 licensed, and (until this pass)
**zero tests**. Several correctness and crash bugs exist (see FINDINGS.md).
Treat it as a promising foundation, not production-ready as-is.

## 2. Where it sits vs. competitors

Think of it as **MessagePack-RPC with streaming**, or **a hand-rolled, lighter
drpc**.

| Option | Wire | Schema/IDL | Streaming | Notes |
|--------|------|-----------|-----------|-------|
| **value-rpc** | TCP + msgpack | none (runtime `Verify`) | 4 modes | tiny, no codegen, dynamic |
| gRPC | HTTP/2 + protobuf | `.proto`, codegen | 4 modes | the standard; heavy, great tooling |
| ConnectRPC | HTTP/1.1+2 + protobuf | `.proto` | 4 modes | browser-friendly, gRPC-compatible |
| Twirp | HTTP/1.1 + protobuf/JSON | `.proto` | unary only | dead simple, no streaming |
| storj/**drpc** | TCP + protobuf | `.proto` | 4 modes | closest in spirit: lightweight gRPC alt |
| **msgpack-rpc** | TCP + msgpack | none | none (req/resp + notify) | closest in *encoding*; no streaming |
| JSON-RPC 2.0 | any + JSON | none | none | ubiquitous, no streaming, verbose |
| Cap'n Proto RPC | TCP + capnp | `.capnp` | promise pipelining | zero-copy, powerful, complex |
| Apache Thrift | TCP + binary | `.thrift` | limited | mature, multi-language |
| net/rpc (stdlib) | TCP + gob/json | Go interfaces | none | Go-only, frozen, no streaming |
| WebSocket + custom | TCP/HTTP + your bytes | none | full duplex | browser reach; you build everything |
| NATS / req-reply | TCP + your bytes | none | via subjects | broker in the middle; great fan-out |

**When value-rpc is the *right* class of tool**

- You want gRPC-style streaming **without** `protoc`, plugins, or generated code.
- Payloads are naturally dynamic/heterogeneous (config blobs, scripting,
  document-ish data) and a rigid schema would fight you.
- Both ends are Go and you control them.
- You value a tiny, readable dependency you can fork and own.

**When to reach for something else**

- You need polyglot clients, mature observability, deadlines/retries/LB, and
  ecosystem (auth, tracing) → **gRPC** or **ConnectRPC**.
- You need browsers as first-class clients → **ConnectRPC** or **WebSocket**.
- You need compile-time type safety across a large API surface → anything with
  an IDL.
- You need pub/sub or work queues, not point-to-point RPC → **NATS**/**Kafka**.

## 3. Using it in backend services

A realistic service shape:

```go
logger, _ := zap.NewProduction()
srv, err := valueserver.NewServer(":9000", logger)
if err != nil { log.Fatal(err) }

// Register the API. Prefer typed args; see BUG-2 about Void+nil.
srv.AddFunction("user.get",
    valuerpc.List(valuerpc.Number),          // args: [userID]
    valuerpc.Any,                             // result
    func(args value.Value) (value.Value, error) {
        id := args.(value.List).GetNumberAt(0).Long()
        u, err := store.User(ctx, id)
        if err != nil { return nil, err }     // surfaces to client as ErrorResponse
        return value.EmptyMap(true).
            Put("id", value.Long(u.ID)).
            Put("name", value.Utf8(u.Name)), nil
    })

srv.AddOutgoingStream("events.tail",          // server-streaming
    valuerpc.List(valuerpc.String),
    func(args value.Value) (<-chan value.Value, error) {
        topic := args.(value.List).GetStringAt(0).String()
        return tailTopic(topic), nil           // close the chan to end the stream
    })

go srv.Run()
defer srv.Close()
```

Client:

```go
cli := valueclient.NewClient("backend:9000", "" /* or socks5 addr */)
if err := cli.Connect(); err != nil { return err }
defer cli.Close()
cli.SetTimeout(2000) // ms, unary deadline

res, err := cli.CallFunction("user.get", value.Tuple(value.Long(42)))

evs, reqID, err := cli.GetStream("events.tail", value.Tuple(value.Utf8("orders")), 256)
for ev := range evs { /* ... */ }            // remember to skip the phantom Null (BUG-4)
_ = reqID                                     // use with cli.CancelRequest
```

**Practical guidance for service authors (today's code)**

- **Register with concrete `TypeDef`s or `Any`, avoid `Void` with `nil` args**
  until BUG-2 is fixed (or pass `value.EmptyList(true)`).
- **Filter `value.Null`** when ranging stream channels (BUG-4).
- **Run one `valueclient.Client` per remote and share it** — it multiplexes and
  is goroutine-safe for `CallFunction`. But beware BUG-6: a slow stream consumer
  can stall *all* calls on that client; give heavy streams their own client.
- **Wrap handlers defensively.** A handler panic in a streamer goroutine is not
  recovered and can crash the server (BUG-3). Add your own `recover` and never
  block forever inside a handler.
- **Put a deadline/timeout in front of every handler yourself** — the server
  does not enforce the client's `sla` (BUG-10).
- **Terminate TLS and authenticate at a layer you add** (e.g. wrap with
  `tls.Server`/`tls.Client`, or run behind a mesh). There is no built-in
  transport TLS or peer authz beyond the connect-authorizer hook. Session
  *resumption* is now gated by a server-issued token (a reused `cid` alone can
  no longer hijack a session), but the initial `cid`/identity is still
  client-asserted — add real peer authentication (mTLS, Unix peer creds) if you
  need to bind sessions to a verified principal.
- **Bound message sizes upstream** (proxy/LB) until BUG-11 is fixed.

## 4. As a simpler alternative to gRPC / WebSocket

**Versus gRPC.** value-rpc gives you the same four call shapes with a fraction
of the toolchain: no `.proto`, no `protoc`, no generated stubs, no HTTP/2 stack.
For internal Go-to-Go services where you'd otherwise fight protobuf for
dynamic/loosely-typed payloads, it is genuinely simpler. What you give up:
cross-language clients, deadline propagation, interceptors/middleware,
load-balancing and name resolution, mature metrics/tracing, and — most
importantly — **compile-time contracts**. With value-rpc the "schema" is the
`Verify` definitions plus a gentleman's agreement about field names; a typo in
`"user.get"` is a runtime error, not a build error.

**Versus raw WebSocket.** A custom WebSocket protocol gives you browser reach
and full-duplex, but you hand-roll framing, multiplexing, request/response
correlation, cancellation, and backpressure every time. value-rpc already has
request multiplexing (`rid`), the four patterns, cancellation, and a throttle
mechanism. So for **server-to-server** duplex messaging it removes a lot of
boilerplate. It is *not* a WebSocket replacement where the requirement is "talk
to a browser" — there is no HTTP upgrade and no JS client.

**Sweet spot:** internal Go microservices and agents that need streaming and a
dynamic payload model, where operational simplicity and a hackable codebase
beat ecosystem and polyglot reach. A reasonable plan is to **wrap it behind your
own typed client interface** per service, so callers get Go types and the
dynamic `value.Map` plumbing stays internal — and so you can swap the transport
later without touching call sites.

## 5. High-load & multi-threading analysis

### Concurrency model

- **Server:** one `accept` loop; one goroutine per connection running the read
  loop; one `sender()` goroutine per `servingClient` draining a 4096-deep
  `outgoingQueue`; new request types spawn a handler goroutine (`go
  serveFunctionRequest`); outgoing streams spawn an `outgoingStreamer`. State
  lives in `sync.Map`s (`clientMap`, `functionMap`, `requestMap`,
  `canceledRequests`) and `go.uber.org/atomic` fields.
- **Client:** one `requestLoop` (drains `reqCh`), one `responseLoop` (reads and
  dispatches by `rid`); per-request `rpcRequestCtx` with a buffered `resultCh`.

This is a sensible shared-socket multiplexed design — *the shape is right*. But
several details break under real concurrency and load (full detail in
FINDINGS.md):

### What breaks under load

1. **Head-of-line blocking (BUG-6).** The single most important scalability
   defect. Both the server's per-connection read loop and the client's single
   response loop perform **blocking** channel sends into per-request buffers
   (`inC <- val`, `resultCh <- res`). One slow consumer that lets its buffer
   fill freezes the entire connection — every other multiplexed request, plus
   control messages (cancel, throttle), stops being processed. Under load with
   any slow stream, the connection deadlocks. The throttle machinery exists but
   doesn't prevent this because the producer-side enqueue is unconditional.

2. **Crashes are races, not edge cases.** `Chat` reliably double-closes its
   channel (BUG-7); shutdown/`reconnect` races send on closed channels (BUG-3);
   a nil-conn race panics in `Cond.Wait` (BUG-17). All three are *more* likely
   as concurrency rises, and none are recovered → whole-process crash. For a
   server multiplexing many clients, one bad client interaction can take
   everyone down.

3. **Goroutine / memory growth.** Each reconnect leaks/duplicates a `sender()`
   (BUG-8); `canceledRequests` never frees entries (BUG-13); idle connections
   are never timed out (BUG-10). Over a long-lived, churny deployment these are
   unbounded.

4. **No backpressure to the socket.** `OutgoingQueueCap = 4096` per client; once
   full, `send()` blocks the producer (a handler or streamer goroutine), which
   is unbounded in count. There is no global limit on concurrent requests,
   in-flight bytes, or handler goroutines → a burst can exhaust memory.

5. **DoS surface.** Unbounded frame length (BUG-11), no read timeout / slowloris
   protection (BUG-10), no max connections, no max concurrent streams. A single
   adversarial or buggy peer can spin CPU (BUG-12), pin memory, or hold
   resources forever.

6. **Allocation profile.** Each message is a fresh `value.Map` built with
   chained immutable `Put`s, then MessagePack-encoded. The measured cost (Apple
   M-class, loopback, see below) is ~0.6 µs and 53 allocs just to pack+unpack a
   small request, and ~3.2 KB / 132 allocs per unary round-trip. That's fine for
   thousands of req/s but will GC-pressure a six-figure-RPS hot path. There's
   room to pool buffers and reuse maps.

### Measured throughput (added benchmarks, loopback, Apple Silicon, Go 1.26)

```
BenchmarkPackUnpackFunctionRequest   ~621 ns/op    992 B/op    53 allocs/op
BenchmarkUnaryCallLoopback           ~22.8 µs/op  3213 B/op   132 allocs/op   (~44k calls/s, serial)
BenchmarkUnaryCallParallel           ~4.9 µs/op   3268 B/op   133 allocs/op   (~205k calls/s, parallel)
```

So the *encoding* is cheap; the per-call overhead is dominated by the
round-trip and allocations. Parallelism scales ~4–5× on this machine, which
says the multiplexing path is not heavily lock-bound for unary calls — the
problems are correctness/robustness, not raw unary speed.

### If you build services on it, fix these first (priority order)

1. **BUG-7, BUG-3, BUG-17** — stop the panics (close-once semantics; don't
   close channels to signal shutdown; lock around `Cond.Wait`). Non-negotiable
   for a server that stays up.
2. **BUG-6** — make the read/response loops never block on a slow consumer
   (bounded enqueue + real backpressure, or per-request pump goroutines). This
   is what lets it survive load.
3. **BUG-10/11/12** — add read/idle deadlines, a max frame size, connection &
   concurrency caps, and `Accept` backoff. This is what lets it survive hostile
   or noisy peers.
4. **BUG-1/2/4/5** — the `nil`/`Null` correctness cluster; cheap to fix and
   removes a lot of foot-guns.
5. **BUG-8/13/14** — leak fixes and graceful shutdown for long-running
   deployments.

### Bigger-picture suggestions

- **Add `context.Context`** to handler signatures and to the client API for
  deadline/cancellation propagation (today cancellation is best-effort and only
  client-driven).
- **Add TLS + auth hooks** at the `MsgConn` boundary.
- **Consider replacing `go.uber.org/atomic`** with Go 1.25 generic
  `sync/atomic` types, and the per-message immutable-map building with a small
  object pool, once correctness is settled.
- **Generate a thin typed client** from a tiny manifest (or hand-write one per
  service) so application code never touches `value.Map` directly — you keep the
  no-IDL simplicity but regain ergonomics and a refactor-safe surface.
