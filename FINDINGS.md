# value-rpc — Bug & Issue Findings

Date: 2026-06-13. Reviewed at commit `470b9c2`, after upgrading to
`go.arpabet.com/value v1.2.0` and Go 1.25.

Severity legend: **C** critical (panic / crash / corruption), **H** high
(correctness or scalability), **M** medium, **L** low / style.

> **STATUS (2026-06-13): all of BUG-1 … BUG-15 are FIXED in the working tree.**
> The tests below were converted from characterization tests (which asserted the
> buggy behaviour) into regression tests that assert the corrected behaviour.
> `go build`, `go vet`, `go test -race ./...`, and `govulncheck ./...` pass, and
> the example program runs end-to-end through all four patterns. A GitHub Actions
> workflow (`.github/workflows/build.yaml`) runs build/vet/race-test on a
> Go 1.25/1.26 matrix plus govulncheck.

| ID | Sev | One-liner | Status | Regression test |
|----|-----|-----------|--------|-----------------|
| BUG-1 | H | Handshake version check reads the wrong field | fixed | `valuerpc.TestValidMagicAndVersion_RejectsNewerVersion` |
| BUG-2 | H | `Void` function + `nil` args is always rejected | fixed | `valueserver.TestVoidArgsAccepted` |
| BUG-3 | C | Server `send()` on closed `outgoingQueue` → panic | fixed | covered by stress + example |
| BUG-4 | H | Phantom `value.Null` at end of stream / void result | fixed | `valueserver.TestServerStreaming`, `TestChatBidirectional` |
| BUG-5 | H | `GetNumber/GetString == nil` guards dead; malformed msgs mis-routed | fixed | `valuerpc.TestValidMagicAndVersion_MissingVersion` |
| BUG-6 | H | Head-of-line blocking / send-on-closed in read/response loops | fixed (StreamPump) | `valueserver.TestSlowStreamConsumerDoesNotBlockOthers`, `valuerpc.TestStreamPump_*` |
| BUG-7 | C | `Chat` double-closes the response channel → panic | fixed | `valueclient.TestRequestCtx_ChatClosesOnce_*` |
| BUG-8 | H | Reconnect starts a second `sender()` goroutine | fixed | — |
| BUG-9 | M | Sender re-enqueues on write error → possible deadlock | fixed | — |
| BUG-10 | M | No server read/idle timeout; slowloris | fixed (keepalive + handshake deadline) | — |
| BUG-11 | M | Unbounded frame length → OOM DoS | fixed (`MaxFrameSize`) | — |
| BUG-12 | M | `Accept()` error path is a busy-loop (no backoff) | fixed | — |
| BUG-13 | M | `canceledRequests` leak (wrong key type) | fixed | — |
| BUG-14 | L | No graceful drain; `wg` never `Wait()`ed; conns untracked | fixed | — |
| BUG-15 | L | License consistency (stays BUSL-1.1); `go.uber.org/atomic` in maintenance | fixed | — |

## How each fix was applied

- **BUG-1 / BUG-5** — `valuerpc/protocol.go` now reads `VersionField`, and new
  `GetNumberField` / `GetStringField` helpers distinguish "absent" from "zero".
  All dead `== nil` guards across the client and server were replaced with these
  helpers or `!= value.Null` checks.
- **BUG-2** — `valuerpc/verify.go`: `Void`/`VerifyArgs`/`VerifyParams` accept
  `value.Null` (the on-wire form of nil) and honour optional params.
- **BUG-3 / BUG-9** — `valueserver/serving_client.go` and
  `valueclient/connection.go` no longer `close()` the queue/`reqCh` to signal
  shutdown; they use a `done` channel and `select`-based sends. The server
  sender no longer re-enqueues or dies on a write error.
- **BUG-4** — the client skips `value.Null` on `StreamValue`/`StreamEnd` and
  converts an absent unary result to Go `nil` (`valueclient/client.go`).
- **BUG-6** — *Initial pass* made `notifyResult` (client) and `offer` (server)
  deliver via `select { case ch <- v: case <-done: }` with a `recover`, which
  stopped the send-on-closed panic and the block-forever-after-close, but it
  did **not** remove the actual head-of-line blocking: a live-but-slow stream
  consumer still froze the single read/response loop (and every other
  multiplexed request) once its buffer filled. The original
  `TestConcurrentUnaryCalls` never exercised a slow stream, so it passed
  regardless.
  *Real fix* — `valuerpc.StreamPump` (`valuerpc/pump.go`) sits between the
  shared loop and each server→client / inbound stream channel. The loop now
  `Push`es (never blocks); a per-request pump goroutine drains a bounded
  internal queue into the consumer channel at the consumer's pace, so
  backpressure is isolated to the one slow request. The pump owns closing the
  out channel (`Close` drains then closes; `Stop` abandons and closes), so
  there is still no send-on-closed or double-close. A consumer that exceeds the
  pending bound (`DefaultMaxPending`) is failed (stream closed; client cancels
  once) rather than pinning unbounded memory. Used for client `getStream`/`chat`
  (`valueclient/request.go`) and server incoming-stream/`chat`
  (`valueserver/serving_request.go`); unary/put-stream keep the direct path.
  Covered by `valueserver.TestSlowStreamConsumerDoesNotBlockOthers` and
  `valuerpc.TestStreamPump_*`.
- **BUG-7** — `valueclient/request.go` rewritten so `resultCh` is closed exactly
  once (`sync.Once`), on the get-side terminal for unary/get/chat and the
  put-side terminal for put-stream. Chat half-close is now correct on the server
  too (`serving_request.go`): the client ending input closes only the inbound
  channel; teardown waits for the server's output to finish.
- **BUG-8** — exactly one long-lived sender per serving client; `replaceConn`
  only swaps `activeConn`.
- **BUG-10 / BUG-11 / BUG-12 / BUG-14** — `valuerpc/rpc.go` replaces goframe with
  a bounded length-prefix codec (`MaxFrameSize`, default 16 MiB; the goframe
  dependency was dropped). `valueserver/server.go` adds TCP keepalive, a
  handshake read deadline, capped `Accept` backoff, live-connection tracking,
  and a graceful `Close()` that drains via `wg.Wait()`. A `recover` in
  `serveFunctionRequest` keeps a panicking user handler from crashing the server.
- **BUG-13** — `canceledRequests.Delete` now uses the `int64` key.
- **BUG-15** — the project **remains BUSL-1.1**: `LICENSE` and all `.go` SPDX
  headers are BUSL-1.1 and internally consistent (an earlier draft explored an
  Apache-2.0 relicense to match upstream `value`, but that change was reverted).
  Separately, migrated off the maintenance-mode `go.uber.org/atomic` to stdlib
  `sync/atomic` (Go 1.25 generic types): `atomic.Error` (no stdlib equivalent)
  became `atomic.Pointer[error]`; `.Inc()/.Dec()` → `.Add(±1)`; `.CAS()` →
  `.CompareAndSwap()`. The dependency was dropped from `go.mod`.

---

## Root-cause theme: `nil` vs `value.Null`

Most of the H-class correctness bugs share one root cause. The `value`
accessors never return a Go `nil`:

- `Map.GetNumber(k)` returns `value.Zero` when the key is absent or non-numeric.
- `Map.GetString(k)` returns `value.EmptyString`.
- `Map.Get(k)` returns the `value.Null` *sentinel* (a non-nil interface).

So `if x := m.GetNumber(k); x == nil { ... }` is **dead code**, and
`m.Get(k) != nil` is **always true**. The correct idioms are
`m.Get(k) != value.Null` (presence) and `m.Get(k).Kind()` (type). The codebase
uses the correct idiom in `serving_request.go` but the broken one almost
everywhere else.

---

## Critical

### BUG-7 (C) — `Chat` double-closes the response channel

`valueclient/request.go`. A chat request opens both halves
(`state = getStreamFlag + putStreamFlag`). On shutdown the server's `StreamEnd`
drives `TryGetClose()` (response loop) while the drained `putCh` drives
`TryPutClose()` (`streamOut` goroutine). Each method calls `close(t.resultCh)`
unconditionally when it clears *its own* flag, so the second one panics with
`close of closed channel` — and it runs on a goroutine with no `recover`, so it
takes the **process** down.

Fix — close exactly once, when the *last* half closes:

```go
func (t *rpcRequestCtx) TryGetClose() bool {
	for {
		st := t.state.Load()
		if st&getStreamFlag == 0 {
			return true
		}
		if t.state.CAS(st, st-getStreamFlag) {
			if st-getStreamFlag == 0 { // no halves left
				close(t.resultCh)
			}
			return true
		}
	}
}
```

…and the symmetric change in `TryPutClose`, plus make `Close()` consistent
(only close when transitioning to 0). A `sync.Once` around the actual
`close(resultCh)` is a simpler, equally correct alternative.

### BUG-3 (C) — server sends on a closed channel

`valueserver/serving_client.go`. `Close()` does `close(t.outgoingQueue)`, but
`send()` (`t.outgoingQueue <- resp`) is called from many goroutines:
`outgoingStreamer`, `serveFunctionRequest`, etc. Any in-flight producer after
`Close()` panics with `send on closed channel`. Those goroutines are not under
the `recover` in `handleConnection`, so this crashes the process.

The mirror image exists on the client: `rpcConn.Close()` closes `reqCh`, while
`SendRequest()` (`t.reqCh <- req`) runs from many callers; a `Reconnect()`
racing with a send panics.

Fix — don't close the channel to signal shutdown while producers exist. Use a
`done chan struct{}` + `select { case q <- v: case <-done: }`, or an atomic
"closed" flag checked under a lock, or hand ownership of close to a single
coordinator that first quiesces producers.

### BUG-17 (C) — `sync.Cond.Wait()` without holding the lock

`valueclient/sync_conn.go`:

```go
func (t *syncConn) getConn() *rpcConn {
	conn := t.conn.Load().(connHolder)
	if conn.value == nil {
		t.active.Wait()        // BUG: called without t.connecting held
		return t.getConn()
	}
	return conn.value
}
```

`Cond.Wait` requires the associated `Locker` to be held; otherwise it calls
`Unlock` on an unlocked mutex and panics (`sync: unlock of unlocked mutex`).
This fires when `getConn` observes a nil conn during a race with `reset()`.

Fix — hold the lock and loop on the predicate:

```go
t.connecting.Lock()
for t.conn.Load().(connHolder).value == nil {
	t.active.Wait()
}
c := t.conn.Load().(connHolder).value
t.connecting.Unlock()
return c
```

---

## High

### BUG-1 (H) — handshake version check reads the wrong field

`valuerpc/protocol.go`:

```go
version := req.GetNumber(MagicField)   // should be VersionField
if version == nil || version.Double() > Version { return false }
```

It reads `"m"` (the magic string) as the version. `GetNumber("m")` parses
`"vRPC"` → `NaN`; `NaN > 1.0` is false, and `version == nil` is never true, so
**the version gate is a no-op** — a client claiming version 999 is accepted.
Fix: read `VersionField` and compare correctly (and validate it is actually a
`NUMBER`).

### BUG-2 (H) — `Void` function called with `nil` args is rejected

The client serializes `nil` args into the `args` field; it crosses the wire as
`value.Null`; the server runs `Verify(Null, Void)` which returns **false** —
`Verify`'s `Void` branch only accepts Go `nil` or an *empty* LIST/MAP, not
`Null`. Net effect: the shipped `example/sample.go` fails at its first `Void`
call (`getName`). Reproduced in `TestVoidArgsRejected`.

Fix (any one):
- In `Verify`, treat `nil` **or** `value.Null` **or** empty collection as valid for `Void`;
- have `constructRequest` omit the args field when `args == nil`, and treat an
  absent field as empty on the server.

### BUG-4 (H) — phantom `Null` at the end of every server stream

`valueclient/client.go`, `StreamEnd` handler:

```go
streamEndValue := resp.Get(valuerpc.ValueField)
if streamEndValue != nil {                  // always true: Get returns Null, not nil
	requestCtx.notifyResult(streamEndValue) // pushes a spurious value.Null
}
```

Every `GetStream`/`Chat` consumer receives a trailing `value.Null` it did not
ask for. Reproduced in `TestServerStreaming` (logs "1 phantom Null"). The
`StreamValue` branch has the same shape. Fix: compare against `value.Null` and
skip it.

### BUG-5 (H) — dead nil-guards; malformed messages mis-routed

See the root-cause theme. Concretely, a message with **no** message-type field
is decoded as `MessageType(0)` = `HandshakeRequest`, and a missing request-id
becomes `0`, instead of being rejected. The guards `if mt == nil`,
`if reqId == nil`, `if cid == nil`, `if name == nil` throughout
`serving_client.go` / `server.go` / `client.go` never trigger. Fix: validate
with `req.Get(field) != value.Null` (and a `Kind()` check) before reading.

### BUG-6 (H) — head-of-line blocking from blocking channel sends

- Server: `servingRequest.incomingStreamValue` does `t.inC <- val` **inside the
  connection read loop** (`serveRunningRequest` runs synchronously in
  `handleConnection`). If the application consumer is slow and `inC` (cap 4096)
  fills, the entire connection stalls — no other multiplexed request, cancel,
  or throttle message is processed. Effectively a per-connection deadlock under
  backpressure.
- Client: `notifyResult` does `t.resultCh <- res` inside the single
  `responseLoop`; one slow stream consumer blocks dispatch for **all** requests
  on that client.

Fix: bounded, non-blocking enqueue that converts overflow into explicit
backpressure (the `ThrottleIncrease` machinery is already half-built for this),
or hand each request its own pump goroutine, or `select` with the throttle/done
channels.

### BUG-8 (H) — reconnect leaks/duplicates the `sender()` goroutine

`servingClient.replaceConn` does `go t.sender()` but never stops the previous
one. Two senders then drain one `outgoingQueue`, so messages are split across
goroutines and can be written out of order; every reconnect adds another
sender. Fix: tie exactly one sender to the connection lifecycle and stop the
old one on replace.

---

## Medium

- **BUG-9** — On write error `sender()` does `t.send(resp)` (blocking enqueue)
  then `break`. With no running sender and a full queue this blocks forever, and
  it reorders the failed message. Prefer dropping or a dedicated resend buffer
  tied to reconnect.
- **BUG-10** — The server never sets a read/idle deadline, so an idle or
  slowloris client holds a goroutine + fd indefinitely. The `timeout` is used
  only for writes, and the client-supplied `sla`/`TimeoutField` is never
  enforced server-side (a slow handler is never cancelled). Add idle read
  deadlines and per-request `context` deadlines.
- **BUG-11** — `goframe` uses a 4-byte length prefix with no maximum, so a
  header claiming ~4 GiB triggers a giant allocation (DoS). `value` v1.2.0 added
  decode limits, but the *frame* read happens first. Bound the frame length.
- **BUG-12** — `Run()` retries `Accept()` immediately on any non-shutdown error;
  on a persistent error (e.g. `EMFILE`) this is a 100%-CPU busy loop. Add
  capped exponential backoff (cf. `net/http.Server.Serve`).
- **BUG-13** — `servingRequest.closeRequest` calls
  `cli.canceledRequests.Delete(t.requestId)` with a `value.Number` key, but
  entries are stored under `reqId.Long()` (an `int64`); the delete never matches
  → unbounded map growth. Also, a `CancelRequest` for an in-flight *unary* call
  is a no-op (the cancel set is only consulted once, before the handler runs).

## Low / housekeeping

- **BUG-14** — `rpcServer.wg` is `Add`ed but never `Wait`ed; `Close()` does not
  drain in-flight handlers, and `outgoingStreamer`/handler goroutines are
  spawned with bare `go` and untracked. There is no graceful shutdown.
- **BUG-15** — Licensing: source headers and `LICENSE` are BUSL-1.1 and
  consistent. Upstream `value` is Apache-2.0; value-rpc deliberately **stays on
  BUSL-1.1**. Separately, `go.uber.org/atomic` is in maintenance mode — Go
  1.25's generic `sync/atomic` types (`atomic.Int64`, `atomic.Bool`,
  `atomic.Pointer[T]`) replace it.
- `Run()`'s trailing unreachable `return nil` was removed during the upgrade so
  `go vet ./...` is clean.

---

## What the upgrade changed (and why it's safe)

- `go.mod`: `go 1.17 → 1.25.0`; `value v1.1.1 → v1.2.0`; `decimal 1.3.1 → 1.4.0`;
  `x/crypto 0.6 → 0.53`; `x/net 0.7 → 0.55`; `zap 1.24 → 1.28`;
  `atomic 1.10 → 1.11`; `multierr 1.9 → 1.11`.
- The `value` **wire format is unchanged** in v1.2.0, so peers stay compatible.
- The one behavior change to watch is `value.Number.Equal` (now strict, no
  cross-type coercion). value-rpc does not rely on `Number.Equal` in any hot
  path (it uses `Get*` + `Kind()` + `!= value.Null`), so the upgrade is safe.
  `go build`, `go vet`, and `go test -race ./...` all pass.
