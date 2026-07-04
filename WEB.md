# value-rpc — Web & Browser Strategy: JSON codec + TypeScript client (design / research)

Date: 2026-07-04. Companion to [TRANSPORTS.md](TRANSPORTS.md) (§4.3 raised browser
reach and asked for a `PROTOCOL.md`; §9.4 deferred WebTransport until "browser
clients become a goal") and [RESEARCH.md](RESEARCH.md). Browser clients are now
the goal: this doc covers (1) adding a **JSON wire codec** next to MessagePack,
(2) how a client **selects the codec** (msgpack stays the default), and (3) the
**JS/TS client stack** for a Vue+Vite / Nuxt+Vite front end.

## TL;DR — recommendations

1. **Add JSON as a second *wire codec* on the WebSocket transport only.**
   TCP/Unix/TLS/QUIC stay msgpack-only (Go↔Go, msgpack is strictly better
   there). The codec is a per-connection property of the WS transport, not a
   per-message or process-wide setting.
2. **Negotiate via WebSocket subprotocol** (`Sec-WebSocket-Protocol`):
   client offers `vrpc.json` or `vrpc.msgpack`; the server answers with its
   pick. **No subprotocol offered → msgpack** — every existing client (which
   dials with no subprotocol) keeps working unchanged. JSON rides in **text**
   frames, msgpack in **binary** frames.
3. **The encode half of the JSON mapping already exists** in the `value`
   library (`Value.MarshalJSON` / `PrintJSON` / `Jsonify`, with the `"base64,"`
   convention for raw bytes). The missing piece is the decode half — a
   `value.ParseJson([]byte) (Value, error)` — plus two small upstream fixes
   (±Inf emits invalid JSON; parse limits parity). Contribute both to `value`,
   not to value-rpc, so the conventions stay in one place.
4. **Two browser blockers are independent of the codec** and should land first:
   expose `AcceptOptions.OriginPatterns` (the WS handler currently passes `nil`
   options, so cross-origin browsers are rejected by coder/websocket's default
   CSRF check) and write **PROTOCOL.md** — the normative envelope spec a
   foreign-language client is built against (TRANSPORTS.md §4.3 already called
   for it).
5. **Build a small TS monorepo** (`value-rpc-js`): `@arpabet/vrpc` (zero-dep
   core: JSON codec, all four patterns, credit-based flow control mapped onto
   async iterators, reconnect + hash-chain resumption via WebCrypto, reverse
   calls), `@arpabet/vrpc-vue` (composables), `@arpabet/vrpc-nuxt` (module).
   msgpack in the browser stays available as an **optional** codec behind a
   subpath export backed by `@msgpack/msgpack`.
6. **Defaults:** Go client/server default to msgpack everywhere (unchanged);
   the TS client defaults to **JSON** — devtools-readable frames, native
   `JSON.parse`, zero dependencies — with a one-line switch to msgpack when a
   payload needs exact int64 or is binary-heavy.

Effort, roughly: Go side (codec + negotiation + origin option + PROTOCOL.md)
≈ 3–5 days; TS core ≈ 1–2 weeks to solid; Vue + Nuxt + examples ≈ 1 week.

## 1. Goal & constraints

Target stack: **Vue 3 + Vite**, or **Nuxt 4 + Vite**, talking to a Go
value-rpc server — ideally the *same* server instance that serves backend
peers over TCP/QUIC, with the WS handler mounted next to the site
(`valueserver.NewWebSocketHandler` → `mux.Handle("/rpc", h)`).

Constraints that shape the design:

- **Wire compatibility is non-negotiable.** Existing Go peers must keep
  connecting with zero changes; msgpack remains the default codec.
- **One server, both codecs.** A JSON browser and a msgpack Go service must
  coexist on the same listener, and a handler must not know or care which
  codec its caller used (codecs translate at the transport boundary; handlers
  see `value.Value` either way).
- **The envelope is already codec-neutral.** Every message is a
  `value.Map` with dialect-named fields (`valuerpc/protocol.go`); nothing in
  the RPC layer touches msgpack bytes. Only the two `MsgConn.ReadMessage` /
  `WriteMessage` seams serialize.
- **Browsers are hostile-network clients.** Origin checks, `wss://`, auth
  without custom headers, and the DoS knobs (frame size, connection and
  request caps) all matter once the port faces the public internet.

## 2. Where msgpack is wired today (inventory)

| Site | What it does | JSON impact |
|------|--------------|-------------|
| `valuerpc/transport_ws.go:73–111` (`wsMsgConn.ReadMessage`/`WriteMessage`) | `value.Unpack` / `value.Pack`, binary frames only | **the seam** — becomes codec-driven |
| `valuerpc/transport_ws.go:215` (`wsListener.Handler`) | `websocket.Accept(w, r, nil)` | add `Subprotocols` + `OriginPatterns` (+ optional compression) |
| `valuerpc/transport_ws.go:287` (`wsDialer.Dial`) | `websocket.Dial(ctx, url, nil)` | add `Subprotocols` when the Go client opts into JSON |
| `valuerpc/rpc.go` (`messageConnAdapter`) | length-prefix + msgpack for TCP/Unix/TLS | out of scope — stays msgpack |
| `quic/transport.go` | per-stream msgpack | out of scope — stays msgpack |
| `valuerpc/transport_mem.go` | no serialization (by reference) | unaffected |

Everything above the transport (`protocol.go`, `peer.go`, pumps, credit,
serving loops) operates on `value.Map` and needs **no change**.

## 3. The JSON codec

### 3.1 The mapping — mostly already decided by `value`

`value` (v1.3.1) implements `json.Marshaler`/`PrintJSON` on every kind, so the
**encode direction and its conventions already exist** and vRPC should not
invent competing ones:

| value kind | JSON encoding (existing) | Decode rule (new `ParseJson`) | Caveat |
|---|---|---|---|
| `NULL` | `null` | `null` → Null | — |
| `BOOL` | `true`/`false` | → Bool | — |
| `NUMBER/LONG` | number (`42`) | integer syntax → Long | **> ±2^53 loses precision in JS** (§6.3); int64 overflow on parse → BigInt |
| `NUMBER/DOUBLE` | number; **NaN → `null`** | decimal/exponent syntax → Double | **±Inf currently emits `+Inf` — invalid JSON; upstream fix needed** (`number.go:356`) |
| `NUMBER/BIGINT` | string `"0x<hex>"` / `"-0x<hex>"` | stays a UTF8 string | **asymmetric**: does not round-trip to BIGINT (§3.2) |
| `NUMBER/DECIMAL` | string `"0x<hex>[#exp]"` | stays a UTF8 string | same asymmetry |
| `STRING/UTF8` | string | string → `ParseString` → UTF8 | a legit string starting with `"base64,"` is mis-read as bytes — documented hazard of the existing convention |
| `STRING/RAW` | `"base64,<RawStdEncoding>"` | `ParseString` reverses it → RAW | round-trips ✓ (JS side: `Uint8Array` ↔ `"base64,…"`) |
| `LIST` | array | array → List | — |
| `MAP` | object (keys sorted — deterministic ✓) | object → sorted Map; duplicate key → last wins (define it) | — |
| `UNKNOWN` (msgpack ext) | `"data:application/x-msgpack-ext;base64,…"` | recognize prefix → Unknown | round-trips ✓ |

### 3.2 What's missing (all upstream in `value`, by preference)

1. **`value.ParseJson(data []byte) (Value, error)`** — the decode half.
   Implementation: `encoding/json.Decoder` with `UseNumber()`, walking tokens
   into `value` nodes with the table above. Must enforce the **same decode
   limits** as the msgpack path (`limits.go`: `MaxParseDepth`,
   `MaxParseCollectionLen`, `MaxParseByteLen`) — a hostile browser is exactly
   the peer those limits exist for. `MaxFrameSize` already bounds input size
   at the transport.
2. **Fix ±Inf**: `strconv.FormatFloat(math.Inf(1), 'f', -1, 64)` produces
   `+Inf`, which is not JSON. Emit `null` (matching the NaN choice) — or
   strings `"Infinity"`/`"-Infinity"`; either way decide once, in `value`.
3. **Document the BIGINT/DECIMAL asymmetry**: they encode as `0x…` strings and
   come back as strings. Auto-reviving any `"0x…"` string into BIGINT would
   corrupt legitimate user strings, so don't. Policy for web-facing APIs:
   model big/precise numbers as strings at the app level (or use the msgpack
   codec, where they round-trip natively).

If upstreaming to `value` is not convenient immediately, the same code can live
in `valuerpc` as `jsoncodec.go` and move later — but the conventions are
`value`'s, and its `marshal_test.go`/fuzz harnesses are the natural home.

### 3.3 The `WireCodec` seam

```go
// valuerpc/wirecodec.go
type WireCodec interface {
    // Name is the codec identity and the subprotocol suffix: "msgpack", "json".
    Name() string
    Encode(msg value.Map) ([]byte, error)
    Decode(data []byte) (value.Map, error) // asserts top-level MAP, applies limits
    // TextFrames selects the WebSocket frame type: text (JSON) vs binary (msgpack).
    TextFrames() bool
}

var (
    MsgpackWireCodec WireCodec = msgpackCodec{} // value.Pack / value.Unpack — today's behavior
    JsonWireCodec    WireCodec = jsonCodec{}    // value.ParseJson / PrintJSON
)
```

`wsMsgConn` gains a `codec WireCodec` field (snapshotted at construction, like
`writeTO`/`keepAlive`); `ReadMessage` requires the negotiated frame type and
rejects the other with an **actionable** error (see §3.4), `WriteMessage` uses
`codec.Encode` + the codec's frame type. The msgpack path compiles to exactly
today's behavior, so the change is mechanical; `transport_ws_internal_test.go`
and the fuzz harness extend naturally.

JSON encode note: `value.Jsonify` returns a `string` (one extra copy). Fine at
browser scale; if it ever shows up in profiles, add a `[]byte`-returning
variant upstream.

### 3.4 Negotiation: WebSocket subprotocol

The subprotocol header is the purpose-built, browser-reachable negotiation
mechanism (`new WebSocket(url, ["vrpc.json"])` — the *only* header a browser
can influence). Prior art: graphql-ws (`graphql-transport-ws`), MQTT-over-WS
(`mqtt`), Kubernetes channel protocols.

Server (`wsListener.Handler`):

```go
c, err := websocket.Accept(w, r, &websocket.AcceptOptions{
    Subprotocols:    []string{"vrpc.msgpack", "vrpc.json"}, // server's preference order
    OriginPatterns:  l.originPatterns,                       // §4
    CompressionMode: l.compression,                          // §4, optional
})
codec := valuerpc.MsgpackWireCodec
if c.Subprotocol() == "vrpc.json" { codec = valuerpc.JsonWireCodec }
```

- Client offers nothing (every existing Go client, `websocket.Dial(ctx, url,
  nil)`) → `Subprotocol()` is `""` → **msgpack**. Full backward compatibility,
  both directions, no version bump.
- Client offers both in preference order → server picks the first it supports.
- RFC 6455 says a server should fail the connection if the client offers only
  protocols it doesn't recognize; coder/websocket returns 400 for that, which
  is correct here.

Go client opt-in (mostly for tests, parity, and non-Go-server interop):

```go
cli := valueclient.NewWebSocketClient(url, valueclient.WithWireCodec(valuerpc.JsonWireCodec))
```

→ dials with `DialOptions{Subprotocols: []string{"vrpc.json"}}`; if the server
echoes no subprotocol (pre-JSON server), fail the connect with a clear error
rather than sending text frames a binary-only server will choke on.

**Rejected alternatives**

- *Query parameter* (`?codec=json`): works, but non-standard, pollutes URLs
  and logs, and offers no server acknowledgment — the client can't tell
  whether the server actually supports JSON until something fails.
- *First-frame sniffing* (text frame ⇒ JSON): zero-config and tempting — the
  frame type alone is a reliable discriminator — but the client gets no
  up-front signal of server support, a mixed-frame connection becomes
  representable, and implicit magic is harder to reason about in middleboxes
  and tests. Instead, keep the *diagnostic*: when a msgpack connection
  receives a text frame, the error should say `"JSON codec not negotiated;
  offer subprotocol vrpc.json"` — that turns the most likely hand-rolled-client
  mistake into a self-explaining failure.
- *In-handshake field*: too late — the handshake itself must already be
  decoded, a chicken-and-egg.

Dialect note: the subprotocol strings are a cleartext fingerprint on `ws://`
(encrypted under `wss://`). If the Dialect-based DPI-shedding story matters for
a deployment, the two names belong in `Dialect` alongside the magic; default
them to `vrpc.msgpack`/`vrpc.json`.

### 3.5 API surface (Go) — proposed additions

| Layer | Addition | Default |
|---|---|---|
| `valuerpc` | `WireCodec`, `MsgpackWireCodec`, `JsonWireCodec`; `value.ParseJson` (upstream) | msgpack |
| `valuerpc` | `NewWebSocketListener`/`NewWebSocketHandler` grow a small options struct (they are already positional-arg-heavy: writeTO, maxFrame, keepAlive → add codecs, origins, compression) | both codecs enabled |
| `valueserver` | `WithWebSocketOrigins(patterns ...string)`, `WithWebSocketCompression()`, `WithWireCodecs(...)` (to *restrict*, e.g. JSON-only public endpoint) | both |
| `valueclient` | `WithWireCodec(c)` on WS clients | msgpack |

Server handlers, `TypeDef`/`Verify`, interceptors, metrics, resilience: **no
changes** — a JSON caller's `{"args":[42]}` arrives as the same
`value.List{value.Long(42)}` a msgpack caller produces. The
`valueserver/matrix_test.go` pattern should gain a codec axis: every pattern ×
{msgpack, json} × (JSON client ↔ msgpack client through one server).

### 3.6 Interop fine points (verified against the code)

- **Handshake version**: `NewHandshakeRequest` writes `v` as `Double(1.0)`;
  JSON renders it `1`, which parses back as `Long(1)`. The check
  (`protocol.go:171`) only calls `version.Double()` — Long(1) satisfies it.
  A JS client sending `"v":1` is valid. (Same on the msgpack path:
  `@msgpack/msgpack` encodes `1.0` as int 1.)
- **Request ids**: client-initiated ids are positive, server-initiated
  (reverse calls) negative, handshake 0. A JS `number` counter is safe for ~2^53
  requests per connection — no issue.
- **`cid`**: the Go client uses `rand.Int63()` (`valueclient/client.go:142`),
  which exceeds JS's 2^53 safe-integer range. Irrelevant for interop — the
  *client* chooses its cid — but the TS client must generate its own within
  `Number.MAX_SAFE_INTEGER` (53 random bits), not copy the Go range.
- **Timeouts**: `sla` is milliseconds; the server turns it into the handler's
  context deadline. TS client: `timeoutMs` option → `sla` + local timer +
  `CancelRequest` on expiry (mirrors `SingleResp`'s `onTimeout`).
- **Metadata**: `md` is a string→string map on request envelopes
  (`WithMetadata` / `MetadataFromContext`) — the seam for `traceparent` from
  the browser.

## 4. Browser-readiness gaps that have nothing to do with the codec

These bite *before* the first JSON byte and should land first.

1. **Origin (CSRF) policy.** `websocket.Accept(w, r, nil)` rejects
   cross-origin browser requests by default (Origin must match Host — the
   safe default). Same-origin deployments (`https://example.com` page,
   `wss://example.com/rpc`) work **today**; an api-subdomain split
   (`app.example.com` → `api.example.com`) is rejected until
   `OriginPatterns` is exposed (§3.5). Keep the safe default; make it
   configurable.
2. **Auth for browsers.** A browser cannot set custom headers on a WebSocket —
   the usual `Authorization: Bearer` pattern does not exist. vRPC is already
   well-shaped for this: the handshake's in-band `auth` field
   (`cli.SetCredential` ↔ `srv.SetAuthenticator`) carries the credential
   inside the (encrypted, post-upgrade) stream, works cross-origin, and binds
   the principal to the session — including resumption. The TS client should
   take `auth: credential | () => credential | Promise<credential>` and put it
   in the handshake. Cookies also flow on the upgrade automatically
   (same-origin) and can back a cookie→principal `Authenticator`; prefer the
   explicit `auth` field as the documented default (no CSRF ambiguity).
3. **Deployment shape.** Recommended: mount `NewWebSocketHandler` on the
   site's existing HTTPS server at `/rpc` (wss termination, one port, one
   cert, LB/CDN-friendly — the WebSocket row in the README's transport table).
   The standalone `NewWebSocketServer` stays for dedicated endpoints.
4. **Compression (optional).** `AcceptOptions.CompressionMode`
   (permessage-deflate) makes JSON frames roughly as compact as msgpack for
   typical payloads. Off by default (matches coder/websocket); expose the
   knob.
5. **Public-exposure hardening.** The knobs exist — `MaxFrameSize` (already
   the WS read limit), `WithMaxConnections`, `WithMaxConcurrentRequests`,
   `WithMaxConcurrentStreams`, `WithHandshakeTimeout`, keepalive pings (the
   server pings; browsers pong automatically, so dead tabs are reaped) — but
   a "facing the internet" README section should say: **set all of them**,
   plus the JSON parse limits of §3.2. Consider a lower default `MaxFrameSize`
   guidance for public WS endpoints (16 MiB is generous for a browser API).

## 5. The wire contract → PROTOCOL.md

The TS client is a second implementation of the protocol; it needs a normative
spec, not code-reading. TRANSPORTS.md §4.3 already proposed `PROTOCOL.md`.
Contents (all verified against the current code):

- **Envelope**: one map per WS frame; default dialect field names (`t`, `rid`,
  `m`, `v`, `fn`, `args`, `res`, `err`, `code`, `val`, `cr`, `sla`, `cid`,
  `tok`, `auth`, `md`); message types 0–14 with 12/13 reserved; codes 0–9.
- **Id spaces**: handshake 0; client-initiated positive; server-initiated
  negative (a client must route negative-rid requests to *its* registered
  handlers, or answer `ErrorResponse{code:NotFound}`).
- **Choreography** per pattern, including flow control (below).
- **Resumption**: SHA-256 reverse hash chain, hex links, anchor on first
  connect, one pre-image per reconnect, server resync window, idempotent-retry
  semantics (`hashchain.go`'s doc comment is 90% of the text).
- **JSON codec**: the §3.1 mapping table, the `"base64,"` and
  `"data:application/x-msgpack-ext;"` string conventions, NaN/Inf policy,
  number-precision policy, duplicate-key policy, subprotocol names.
- **Golden frames**: fixture files with concrete bytes/text for each message in
  both codecs — the determinism of `value` (canonical msgpack, sorted JSON
  keys) makes byte-exact fixtures possible; both the Go and TS test suites
  consume the same fixtures. This is the anti-drift mechanism for a second
  implementation.

A complete unary + server-stream exchange in the JSON codec, for flavor
(→ client-to-server, ← server-to-client; text frames):

```
→ {"cid":712398211,"m":"vRPC","rid":0,"t":0,"tok":"<anchor-hex>","auth":"<bearer>"}
← {"m":"vRPC","rid":0,"t":1,"v":1}
→ {"args":["world"],"fn":"greet","rid":1,"sla":5000,"t":2}
← {"res":"Hello, world!","rid":1,"t":3}
→ {"args":[3],"fn":"count","rid":2,"sla":5000,"t":4}
← {"rid":2,"t":8}                        StreamReady — ack, then client grants window
→ {"cr":4096,"rid":2,"t":14}             initial credit (client→server grant)
← {"rid":2,"t":9,"val":1}
← {"rid":2,"t":9,"val":2}
← {"rid":2,"t":9,"val":3}
← {"rid":2,"t":10}                       StreamEnd
→ {"rid":2,"t":11}                       (or CancelRequest at any point)
← {"code":5,"err":"function not found","rid":3,"t":7}   error shape
```

Flow-control rules a foreign client must honor (from
`valueclient/request.go`, `valueclient/serving.go`):

- **Consuming** (get/chat): after StreamReady, grant the initial window
  (`cr` = receive capacity); re-grant in batches (~half window) as values are
  *consumed by the application* — not as they arrive. The StreamReady delivery
  itself doesn't count toward replenishment.
- **Producing** (put/chat): send at most the credit granted by peer
  `StreamCredit` messages; block (async-await) when out of credit. Overrunning
  credit gets the stream killed with `CodeResourceExhausted`.

## 6. The TypeScript client stack

### 6.1 Packaging

A separate repo (`arpabet/value-rpc-js`), pnpm monorepo, three packages:

| Package | Contents | Deps |
|---|---|---|
| `@arpabet/vrpc` | protocol core, JSON codec, reconnect, streams, typed surface | **zero** runtime deps; optional `@arpabet/vrpc/msgpack` subpath → peer dep `@msgpack/msgpack` (stays out of the bundle unless imported) |
| `@arpabet/vrpc-vue` | Vue 3 plugin + composables | peer: vue |
| `@arpabet/vrpc-nuxt` | Nuxt module wrapping the Vue package | @nuxt/kit |

Runs in browser, Node ≥ 22 (native `WebSocket`), Bun/Deno — the same core
serves Nuxt SSR/Nitro and Node scripts. Build: tsup/unbuild, ESM-first, `.d.ts`
maps. License: note that the Go repo is BUSL-1.1 while `value` is Apache-2.0;
a client SDK wants a permissive license for adoption (Apache-2.0, matching
`value`) — decide deliberately.

### 6.2 Core API sketch

```ts
import { createClient, VrpcError, Code } from "@arpabet/vrpc";

const client = createClient({
  url: "wss://example.com/rpc",
  codec: "json",                          // default; "msgpack" needs @arpabet/vrpc/msgpack
  auth: () => session.token,              // → handshake `auth` field
  timeoutMs: 5000,                        // default unary SLA
  reconnect: { backoff: "exp-jitter", resume: true },  // hash-chain resumption
  metadata: () => ({ traceparent: currentTrace() }),   // → `md` on every request
});

await client.connect();

// unary — AbortSignal maps to CancelRequest
const user = await client.call("user.get", [42], { signal: ctrl.signal });

// server stream — async iterator; `break` / `return` sends CancelRequest;
// credit is granted as the loop pulls ⇒ real pull-based backpressure in the browser
for await (const ev of client.getStream("events.tail", ["orders"])) {
  render(ev);
}

// client stream / chat
await client.putStream("logs.upload", lines());            // any (async) iterable
const chat = client.chat("support.chat", [ticketId]);
chat.send("hello"); chat.end();
for await (const msg of chat.incoming) { /* … */ }

// peer symmetry: the SERVER can call the BROWSER (live updates without a
// hand-rolled notification layer)
client.addFunction("notify", async (args) => { toast(String(args)); });

client.status;                    // "connecting" | "open" | "resuming" | "closed"
client.on("status", updateBadge);
```

Error model: `VrpcError { code: Code, message }` mirroring `valuerpc.Code`
0–9, so `err.code === Code.Unavailable` branches like `valuerpc.CodeOf`.

Typed calls **without codegen** — the TS analogue of `valuerpc.Codec[T]` /
`CallUnary`, keeping the framework's no-IDL ethos (types instead of schemas):

```ts
interface Api {
  "user.get":    { args: [id: number]; result: User };
  "events.tail": { args: [topic: string]; stream: OrderEvent };
}
const api = client.typed<Api>();
const u = await api.call("user.get", [42]);         // ← User, args checked
for await (const e of api.getStream("events.tail", ["orders"])) { /* OrderEvent */ }
```

Optional runtime validation (the client-side mirror of `TypeDef`/`Verify`):
accept [Standard Schema](https://standardschema.dev) validators (zod/valibot)
per method — opt-in, tree-shaken away when unused.

### 6.3 Value mapping in TS

| Wire | TS type | Notes |
|---|---|---|
| null | `null` | |
| bool | `boolean` | |
| long | `number` | **throws if a *sent* integer exceeds ±2^53**; received longs beyond that already lost precision in `JSON.parse` — see policy below |
| double | `number` | NaN arrives as `null` (server-side encode policy) |
| bigint/decimal | `string` (`"0x…"`) | app-level; or msgpack codec (`useBigInt64` → `bigint`) |
| utf8 string | `string` | |
| raw bytes | `Uint8Array` | JSON codec transparently encodes as `"base64,…"` (matches `value.ParseString` server-side); msgpack codec uses native bin |
| list | `Array` | |
| map | plain object | codec writes keys sorted (canonical, matches golden fixtures) |

**Number policy (must be documented, it's the one real JSON limitation):**
JSON + `JSON.parse` is IEEE-double land. Rule for web-facing APIs: keep
integers within ±2^53, or model them as strings/BIGINT app-level, or flip that
client to msgpack (`useBigInt64: true` gives exact `bigint`). The envelope
itself is safe (rid counter, sla, code, cr are small; cid is client-chosen).
A lossless-JSON parse mode (e.g. `lossless-json`) is possible later behind the
codec interface without API changes — don't pay its DX cost by default.

### 6.4 Reconnect & resumption

Mirrors the Go client's semantics (`WithReconnectPolicy` + `hashchain.go`):

- On drop: fail in-flight calls fast with `VrpcError(Code.Unavailable)`;
  optionally auto-replay calls marked idempotent.
- Backoff: exponential + jitter; `navigator.onLine` / `visibilitychange` as
  reconnect hints (bfcache/tab-sleep kills sockets — a browser-specific
  reality the Go client never faces).
- Resumption: reverse hash chain via WebCrypto (`crypto.subtle.digest`,
  async). Precompute a browser-sized chain (e.g. 1 024 links ≈ 1 024 awaited
  SHA-256s, milliseconds, ideally in `requestIdleCallback`) — the chain length
  is client-chosen so this needs no server change. Send the anchor in `tok` on
  first connect, one pre-image per reconnect, exactly per PROTOCOL.md. Chain
  state is per-tab in memory; a fresh tab is a fresh session by design.
- Keepalive: rely on server pings (browsers pong automatically); the client
  additionally watches `close`/`error` and half-open detection via a
  lightweight app-level echo only if practice shows it's needed.

### 6.5 Vue package

```ts
// main.ts
app.use(createVrpcPlugin({ url: import.meta.env.VITE_VRPC_URL }));

// component
const { data, pending, error, refresh } =
  useVrpcCall<User>("user.get", () => [route.params.id]);   // reactive args → auto re-call

const { latest, values, status, cancel } =
  useVrpcStream<OrderEvent>("events.tail", ["orders"]);      // auto-cancel on unmount

const { status: conn } = useVrpcConnection();                // for a status badge
```

Composables stay thin (subscribe on mount, cancel on scope dispose, refs out).
For caching/dedup/SWR, don't reinvent: `client.call` slots directly into
`@tanstack/vue-query`'s `queryFn`; document the recipe instead of shipping a
cache.

### 6.6 Nuxt module

`defineNuxtModule` (configKey `vrpc`): registers the plugin, auto-imports the
composables, reads `runtimeConfig.public.vrpc.url`.

**SSR policy — keep it boring:** the plugin is **client-only** by default.
`useVrpcCall` behaves like `useAsyncData(..., { server: false })`: SSR renders
the pending state, the browser connects and fills. A WebSocket-per-SSR-render
is the anti-pattern to avoid. For genuinely SSR-critical data, phase 2 offers
a **Nitro plugin** holding one shared `@arpabet/vrpc` client (Node 22 native
WebSocket) that server routes / `useAsyncData` can query during render — same
core package, one persistent connection per Nitro instance.

### 6.7 Dev DX (Vue + Vite without Nuxt)

Same-origin in dev via the Vite proxy — no origin patterns, no CORS, no mixed
content:

```ts
// vite.config.ts
export default defineConfig({
  server: { proxy: { "/rpc": { target: "http://localhost:9000", ws: true } } },
});
```

The Nuxt module ships the same via `nitro.devProxy`. Example apps to include
in the monorepo: `examples/vue-vite` (status badge, unary call, live stream,
server→browser notify) and `examples/nuxt` (module config + SSR-safe usage).

### 6.8 Testing strategy (the anti-drift plan)

1. **Golden frames** (from PROTOCOL.md fixtures): TS codec unit tests assert
   byte/text-exact encoding — possible because `value` is deterministic and
   the TS codec writes sorted keys.
2. **Conformance suite**: CI boots the real Go server (tiny `go run` harness
   exposing all four patterns + auth + resumption faults) and runs the TS
   client against it over both codecs — the same matrix
   `valueserver/matrix_test.go` runs Go-to-Go.
3. **Fault injection**: dropped connections mid-stream (resumption), credit
   starvation, oversized frames, wrong-frame-type, malformed JSON — mirroring
   the hardening tests the Go side already has.

## 7. Alternatives considered

| Option | Verdict |
|---|---|
| **REST/JSON gateway in front of vRPC** (or ConnectRPC for the web tier) | The pragmatic fallback if a second protocol implementation is unwanted — but it forfeits streaming, cancellation, peer-symmetric push, and duplicates every API definition. The user goal is vRPC end-to-end. |
| **msgpack-only browser client** (skip JSON entirely) | Viable — `@msgpack/msgpack` is solid — and it keeps one codec. Rejected as the *only* path: opaque devtools frames hurt day-to-day development, and a zero-dep JSON client is the better default for web DX. JSON's cost (one decoder in `value`, one seam in `wsMsgConn`) is small and also buys `curl`-ability and non-JS polyglot clients. |
| **tRPC / graphql-ws** | Not alternatives (different servers), but the DX prior art: tRPC for the typed no-codegen client surface (§6.2), graphql-ws for subprotocol negotiation + reconnect semantics. |
| **WebTransport** | Still the "later" answer (TRANSPORTS.md §9.4): pairs with the QUIC module, needs the same TS protocol core anyway. The TS client's transport is a tiny interface (`send/onmessage/close`) so WebTransport slots in without touching the protocol layer. |
| **socket.io / SSE** | Wrong layer (socket.io is its own protocol; SSE is half-duplex). |

## 8. Risks & open questions

- **Second implementation drift** — the main structural risk. Mitigation:
  PROTOCOL.md as the normative spec + shared golden fixtures + CI conformance
  against the real Go server (§6.8). Treat protocol changes as spec changes
  first.
- **int64 precision in JSON** (§6.3) — an API-design rule, not a bug; must be
  loudly documented. msgpack codec is the escape hatch.
- **`"base64,"` prefix collision** — inherited from `value`'s convention: a
  user string that genuinely starts with `base64,` becomes bytes server-side.
  Document; msgpack unaffected.
- **BIGINT/DECIMAL don't round-trip through JSON** (§3.2) — strings on the JS
  side by policy.
- **±Inf upstream bug** — `value` emits invalid JSON for infinities today;
  fix before shipping the codec (a browser `JSON.parse` will throw on such a
  frame).
- **Public exposure** — a browser endpoint is an internet-facing endpoint:
  the §4.5 caps + JSON parse limits are part of "done", not hardening
  follow-up. FINDINGS.md's DoS class applies doubly here.
- **Browser tab lifecycle** — sockets die on bfcache/sleep; reconnect+resume
  (§6.4) is the answer, but UX around "resuming" state needs the status
  surface in the Vue package.
- **Open**: should the TS client ship request replay-on-reconnect for
  idempotent calls in v1 (Go client has it) or defer? Lean: defer; browsers
  re-fetch naturally on UI events.
- **Open**: BUSL vs Apache for `value-rpc-js` (§6.1).

## 9. Phased roadmap

| Phase | Deliverables | Size |
|---|---|---|
| **0 — enablers** | `OriginPatterns` (+compression) options through listener/handler/serveroptions; PROTOCOL.md with golden fixtures | ~1 day |
| **1 — JSON codec (Go)** | `value.ParseJson` + Inf fix + limits (upstream); `WireCodec` seam in `wsMsgConn`; subprotocol negotiation; `WithWireCodec` client option; matrix/fuzz/golden tests | ~2–4 days |
| **2 — TS core** | `@arpabet/vrpc`: JSON codec, handshake+auth, unary, errors, streams+credit, cancel, reconnect+hash-chain resume, reverse calls; conformance CI vs Go server | ~1–2 weeks |
| **3 — framework layer** | `@arpabet/vrpc-vue` composables; `@arpabet/vrpc-nuxt` module (client-only SSR policy); msgpack optional codec; vue-vite + nuxt examples; docs | ~1 week |
| **4 — later** | Nitro shared-client SSR plugin; lossless-number mode; WebTransport transport for the TS core | on demand |

Phases 0–1 are pure Go and independently shippable (a minor release: "WebSocket
transport now also speaks JSON, negotiated by subprotocol; msgpack default,
fully backward compatible"). Phase 2 unlocks the actual goal; phase 3 makes it
pleasant in the user's stack.
