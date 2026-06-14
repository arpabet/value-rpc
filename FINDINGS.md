# value-rpc — Bug & Issue Findings

Date: 2026-06-13. Reviewed at commit `470b9c2`, after upgrading to
`go.arpabet.com/value v1.2.0` and Go 1.25.

Severity legend: **C** critical (panic / crash / corruption), **H** high
(correctness or scalability), **M** medium, **L** low / style.

Four of these are reproduced by automated tests (`go test ./...`); they are
written as *characterization* tests that pass today and `t.Skip` if the bug is
later fixed.

| ID | Sev | One-liner | Proven by test |
|----|-----|-----------|----------------|
| BUG-1 | H | Handshake version check reads the wrong field | `valuerpc.TestValidMagicAndVersion_VersionCheckBroken` |
| BUG-2 | H | `Void` function + `nil` args is always rejected | `valueserver.TestVoidArgsRejected` |
| BUG-3 | C | Server `send()` on closed `outgoingQueue` → panic | — |
| BUG-4 | H | Phantom `value.Null` delivered at end of every server stream | `valueserver.TestServerStreaming` |
| BUG-5 | H | `GetNumber/GetString == nil` guards are dead code; malformed msgs mis-routed | — |
| BUG-6 | H | Head-of-line blocking: blocking channel sends in the read/response loops | — |
| BUG-7 | C | `Chat` double-closes the response channel → panic | `valueclient.TestRequestCtx_GetThenPutClose_DoubleClose` |
| BUG-8 | H | Reconnect starts a second `sender()` goroutine | — |
| BUG-9 | M | Sender re-enqueues on write error → possible deadlock | — |
| BUG-10 | M | No server read/idle timeout; client `sla` never enforced | — |
| BUG-11 | M | Unbounded frame length → OOM DoS | — |
| BUG-12 | M | `Accept()` error path is a busy-loop (no backoff) | — |
| BUG-13 | M | `canceledRequests` leak (wrong key type) + ineffective unary cancel | — |
| BUG-14 | L | No graceful drain; `wg` is never `Wait()`ed; streamers untracked | — |
| BUG-15 | L | License header mismatch (BUSL-1.1 vs upstream Apache-2.0); `go.uber.org/atomic` is in maintenance | — |

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
- **BUG-15** — Licensing: source headers and `LICENSE` are BUSL-1.1, but the
  upstream `value` is now Apache-2.0 (relicensed in v1.2.0). Decide whether
  value-rpc should also relicense. Separately, `go.uber.org/atomic` is in
  maintenance mode — Go 1.25's generic `sync/atomic` types
  (`atomic.Int64`, `atomic.Bool`, `atomic.Pointer[T]`) can replace it.
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
