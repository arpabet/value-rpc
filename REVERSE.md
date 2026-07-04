# Review: bidirectional invocation (client→server and server→client)

> **Status (2026-07-04): all findings R1–R8 fixed** in the working tree, each
> with a regression test. Summary:
> - **R1** — `WithSessionRetention` + a per-session reap timer
>   (`servingClient.scheduleReap`); disconnected sessions are reclaimed (sender
>   goroutine stopped, clientId slot freed). Default 2 min; `-1` disables.
>   Tests: `TestSessionReapedAfterDisconnect`, `TestReconnectCancelsReap`,
>   `TestReapingDisabled`; `valueserver.SessionCount` diagnostics hook.
> - **R2** — `servingClient.replaceConn` fails reverse in-flight
>   (`failReverseInFlight`, CodeUnavailable); `valueclient.Reconnect` tears down
>   its reverse `servingMap`. Test: `TestReplaceConnFailsReverseInFlight`.
> - **R3** — `Peer.PutStreamWait` returns a completion channel; ctx-lifetime
>   caveat documented on `Peer.PutStream`/`PeerFromContext`. Test:
>   `TestReversePutStreamWaitUsesRequestCtx`.
> - **R4** — inbound request-id sign validated both directions (client requires
>   rid<0 for server-initiated; server requires rid>0 for new requests). Tests:
>   `TestServeNewRequestRejectsNonPositiveId`, `TestServeInboundRejectsNonNegativeId`.
> - **R5** — `servingClient.CallFunction` sends CancelRequest on ctx cancel; the
>   client registers a per-reverse-call cancel (`reverseCancels`) and routes
>   CancelRequest to it. Test: `TestReverseUnaryCancelPropagates`.
> - **R6** — `WithMaxConcurrentInbound` (default 1024) bounds reverse-call
>   handlers on the client. Test: `TestReverseCallConcurrencyBound`.
> - **R7** — `valueclient.Close()` sweeps `requestCtxMap` (fails in-flight with
>   `ErrClientClosed`) and `syncConn.close()` makes `getConn` return nil so a
>   blocked `streamOut` unblocks. Test: `TestClientCloseUnblocksInFlight`.
> - **R8** — the client records CodeCanceled on a peer CancelRequest (the server
>   already did via `setCode`), so a cancelled terminal state is distinguishable
>   from a clean end.
>
> The original findings follow, for the record.

Date: 2026-07-04. Scope: the code that lets **either peer invoke functions and
open streams on the other** — `valueclient/client.go` (`serveInboundCall`,
`serveInboundStream`), `valueclient/serving.go` (`clientServingRequest`),
`valueserver/serving_client.go` (`CallFunction`, `completePendingCall`,
`processRequest`), `valueserver/serving_stream.go` (`clientStream`,
`openStream`, reverse Get/Put/Chat), `valueserver/peer.go`
(`PeerFromContext`), plus the id-space and lifecycle rules in
`valuerpc/protocol.go` / `peer.go`.

Evidence base: code reading, the existing test suites, and a new
**cross-language conformance run** — the TypeScript client
(`value-rpc-js/packages/vrpc`) exercised every pattern in both directions
(unary, get/put/chat; reverse unary, reverse get, reverse put; auth;
cancellation; reconnect + hash-chain resumption) against this repo's real
server over WebSocket/msgpack. All pass, one harness bug found en route
(R3 below — it bit in practice).

## What is solid

- **Id-space design.** Handshake 0, client-initiated positive, server-initiated
  negative (`Dialect.HandshakeRequestId` doc, `nextServerRequestId`). Honest
  peers cannot collide, and both dispatchers route reverse traffic before
  own-request traffic consistently.
- **Registration-before-send, everywhere.** Server: `pendingStreams.Store`
  before the open frame (`openStream`), initial inbound credit only after
  `requestMap.Store` (`newServingRequest`). Client: `servingMap.Store` on the
  response loop before any reply can race (`serveInboundStream`). No
  lost-first-frame races.
- **Flow control symmetry.** Credit-before-Ready for put/chat on both sides;
  the `recvDelivered = -1` StreamReady-ack offset is mirrored in
  `rpcRequestCtx` and `clientStream`; overflow (peer exceeding credit) is
  detected and surfaced as `CodeResourceExhausted` + teardown on both sides.
- **Chat half-close semantics** are symmetric: peer's StreamEnd closes the
  inbound channel only; the responder's output ending is the terminal event.
- **Resumption security** (anchor/pre-image chain, principal binding,
  idempotent retry) is well-tested (`session_test.go` incl. hijack tests).
- Server handler panics are recovered; the response loops are never blocked by
  slow consumers (StreamPump).

## Findings, most severe first

### R1 — Sessions are never reaped: goroutine + memory growth, permanent cid squatting  (severity: high for internet-facing deployments)

`rpcServer.clientMap` gains a `servingClient` per clientId
(`createOrUpdateServingClient`) and **nothing ever deletes an entry** except
server `Close()` (verified: no `clientMap.Delete` in the package, no
TTL/reaper). Each `servingClient` owns a `sender()` goroutine and a
`outgoingQueue` channel (cap `OutgoingQueueCap` = 4096) that live until server
shutdown, even after its connection dies and the client never returns.

Consequences:

1. **Unbounded growth**: every client that ever connects — including every
   browser tab, which is exactly the WEB.md direction — permanently costs a
   goroutine + queue + map entry. `WithMaxConnections` caps live sockets, not
   dead sessions, so a connect/disconnect loop is a cheap resource-exhaustion
   attack.
2. **Permanent cid squatting**: whoever first presents a clientId owns that
   slot forever. A later connect with the same cid but a non-chaining token is
   rejected (`authorizeResume`), which is correct for hijack protection — but
   with no expiry, an attacker who can learn or race a victim's cid can
   pre-claim it and permanently lock the victim out (the victim's client keeps
   reconnect-failing; with resumption enabled it never rotates its cid).

Recommendation: a disconnect-started retention timer per session (canceled on
resume) with a `WithSessionRetention(d)` option; optionally a per-principal
session cap. Stopping `sender()` and freeing the queue on reap also fixes the
goroutine growth. A `sessions` gauge in `Metrics` would make the growth
observable.

### R2 — Reverse in-flight requests neither fail fast nor resync across a reconnect  (severity: high for reverse-stream users)

The **client-initiated** direction has an explicit reconnect policy: on drop,
in-flight requests are failed with `CodeUnavailable` (or replayed if
idempotent) — `rpcClient.Reconnect` sweeps `requestCtxMap` with a cutoff.

The **server-initiated** direction has no policy at all:

- `valueclient.Reconnect` never touches `servingMap`, so client-side responder
  state survives the drop.
- `valueserver.replaceConn` only swaps the connection; `pendingCalls` and
  `pendingStreams` survive too.

Because the session resumes but the frames in flight during the gap are simply
gone (client `reply()` silently drops when disconnected; queued writes on the
dead socket are lost), a reverse stream that straddles a reconnect loses
values **silently** and its credit accounting desyncs — sent-but-lost
StreamValues consumed `CreditGate` credit that the receiver will never
replenish, so the stream eventually **stalls forever** rather than erroring. A
reverse unary whose request or response fell in the gap waits until its ctx
deadline.

Recommendation: mirror the fail-fast policy — on `replaceConn`, fail
`pendingCalls`/`pendingStreams` with `CodeUnavailable`; on client reconnect,
tear down `servingMap` (the server will re-open). Alternatively, document that
reverse streams do not survive resumption — but today they half-survive, which
is the worst of both.

### R3 — `peer.PutStream`/`GetStream` from a request ctx dies silently when the handler returns  (severity: medium; bit in practice)

For a unary call the server cancels the request context the moment the handler
returns (`doServeFunctionRequest`: `defer cancel()`), and `watchStream` tears
down any reverse stream bound to that ctx (`cancelClientStream`). `PutStream`
returns after StreamReady and streams **in the background with no completion
signal**, so this pattern:

```go
srv.AddFunction("push", vrpc.Any, vrpc.Any, func(ctx context.Context, args value.Value) (value.Value, error) {
    peer, _ := valueserver.PeerFromContext(ctx)
    go feed(putCh)
    peer.PutStream(ctx, "ingest", args, putCh) // returns at StreamReady
    return value.Utf8("ok"), nil               // ← ctx canceled here; stream killed
})
```

delivers **zero values, with no error surfaced anywhere** (the streamer's
`Acquire` returns false and exits silently). The TS conformance harness hit
exactly this; `context.WithoutCancel(ctx)` is the workaround.

Recommendation: (a) call it out in the `PeerFromContext` and `Peer.PutStream`
docs (the current `PeerFromContext` comment even encourages caching the peer
for later use — which is safe, but only with a non-request ctx); (b) give
`PutStream` a completion signal (e.g. an additional `<-chan error`, or a
`PutStreamWait` variant) so callers *can* keep the ctx alive until the stream
drains.

### R4 — Neither side validates the request-id sign of inbound new requests  (severity: medium, robustness)

The id spaces are sign-keyed, but only by convention:

- Client: `serveInboundCall` / `serveInboundStream` accept a server-initiated
  request with **any** rid. A buggy/hostile server sending rid > 0 registers a
  `servingMap` entry that shadows the client's own request with that id —
  `servingMap` is routed first — hijacking/blackholing its frames.
- Server: `serveNewRequest` accepts a new request with a **negative** rid,
  which then collides with the server's own `pendingCalls`/`pendingStreams`
  space; `processRequest` routes `findPendingStream` first, so subsequent
  frames for it are silently mis-routed or swallowed.

Honest peers never do this; a malicious peer only corrupts its own connection.
But one wrong-sign frame currently causes silent cross-talk instead of a clean
protocol error, and a second implementation (the TS client, or any
PROTOCOL.md-based port) is exactly the kind of peer that could get this wrong.
One guard per side (`rid < 0` required inbound at the client, `rid > 0`
required for new requests at the server, else `ErrorResponse{InvalidArgument}`)
plus normative PROTOCOL.md wording closes it.

### R5 — Server→client unary cancellation does not propagate  (severity: medium)

Client→server unary cancel is fully plumbed: `CancelRequest` →
`requestCancels` → handler ctx canceled. The reverse direction is not:
`servingClient.CallFunction` on `ctx.Done()` just returns `ctx.Err()` — **no
CancelRequest frame is sent**, and the client would have nowhere to route one
anyway: `serveInboundCall` spawns the handler goroutine with a ctx derived
from `baseCtx` and keeps no per-request cancel registry. The client handler
runs to completion and its reply is dropped (`completePendingCall`: unknown id
ignored).

Fine for cheap handlers; wasteful/incorrect for expensive or side-effecting
ones. Recommendation: send `CancelRequest` from `CallFunction` on ctx
cancellation, and mirror `requestCancels` on the client for inbound unary
calls (the reverse-stream path already handles CancelRequest via
`servingMap`).

### R6 — No inbound concurrency bound on the client  (severity: medium; acknowledged TODO)

`client.go:617` — `TODO(reverse-rpc): bound inbound-call concurrency (cf.
server MaxConcurrentRequests)`. The server caps handler goroutines
(`MaxConcurrentRequests` = 4096) and live streams (`MaxConcurrentStreams`);
the client spawns an unbounded goroutine per inbound call **and** per inbound
stream. In the peer-symmetric model the client is often the weaker device (a
browser tab, per WEB.md). Same fix as the server: a small default cap +
`CodeResourceExhausted` rejection.

### R7 — `valueclient.Close()` strands in-flight consumers  (severity: low-medium)

`Close()` tears down `servingMap` (reverse streams) and cancels `baseCtx`, but
never sweeps `requestCtxMap` (verified: the only `Range` over it is in
`Reconnect`). Consequences:

- a goroutine ranging over an open get-stream/chat channel blocks **forever**
  (`resultCh` is never closed);
- an in-flight unary only fails when its own timer fires — as
  `ErrTimeoutError`, not "client closed";
- a `streamOut` (or `reply`) that reaches `syncConn.getConn()` while the conn
  is nil blocks forever on the cond var — `reset()` stores nil without any
  wake-up, and nothing signals shutdown to waiters.

Recommendation: in `Close()`, sweep `requestCtxMap` with
`failRequest(rc, ErrClientClosed)` (the code already exists for the reconnect
path), and make `getConn` shutdown-aware (return nil after `closed` and guard
the call sites).

### R8 — A peer's cancel is indistinguishable from a normal end of stream  (severity: low)

Both responders handle an incoming `CancelRequest` by closing the request
without recording an error (`processResponse` CancelRequest → `Close()`;
`clientServingRequest.serveRunning` likewise). The consumer's channel closes
exactly as it would on a clean `StreamEnd`, so application code cannot tell
"the peer finished" from "the peer gave up". A recorded `CodeCanceled` before
close would make the terminal state honest on both sides.

## Test-coverage notes

`reverse_call_test.go` (3), `reverse_stream_test.go` (3),
`reverse_metadata_test.go` (1) cover the happy paths and reverse-unary
concurrency; `session_test.go` covers resumption security thoroughly. Not
covered today — and each maps to a finding above: reverse traffic **across a
reconnect** (R2), session accumulation/reaping (R1), `Close()` with active
streams (R7), wrong-sign rids (R4), reverse-unary cancellation (R5).

## Cross-language conformance (new, in `value-rpc-js`)

`value-rpc-js/packages/vrpc/test/conformance.test.ts` boots
`value-rpc-js/conformance/server` (a real `valueserver` binary from this
checkout) and drives 18 scenarios over WebSocket/msgpack, including all three
reverse patterns and a server-side kill → reconnect → hash-chain resume →
reverse-call-after-resume sequence. This is the §6.8 anti-drift harness from
WEB.md, and it doubles as a regression net for the findings above.
