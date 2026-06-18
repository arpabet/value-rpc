<!--
  Copyright (c) 2025-2026 Karagatan LLC.
  SPDX-License-Identifier: BUSL-1.1
-->

# Release checklist

vRPC ships as three modules from this repo:

- `go.arpabet.com/value-rpc` — core (no `quic-go` dependency).
- `go.arpabet.com/value-rpc/quic` — the QUIC transport (`quic/`), a **separate
  module** so `quic-go` only enters builds that use it.
- `go.arpabet.com/value-rpc/resilience` — service-governance interceptors
  (`resilience/`: retry, circuit breaker, timeout, rate limit, bulkhead,
  fallback), a **separate module** so plain clients carry no governance deps.

Each submodule's `go.mod` carries a local override so it builds against the
working tree during development:

```
replace go.arpabet.com/value-rpc => ../
```

This `replace` **must not** be present in a published tag — a consumer that pulls
`…/value-rpc/quic@vX` (or `…/resilience@vX`) would otherwise fail (there is no
`../`). Releasing is therefore an ordered process: tag core first, then each
submodule with the replace dropped.

## 1. Pre-release verification (both modules)

From the repo root:

```sh
gofmt -l .                                   # must print nothing
go build ./... && go vet ./...
go test -race ./...
go test -run='^$' -bench=. -benchtime=20x ./...
go run golang.org/x/vuln/cmd/govulncheck@latest ./...
# fuzz smoke (codec robustness)
go test ./valuerpc/ -run='^$' -fuzz=FuzzReadMessage -fuzztime=20s
go test ./valuerpc/ -run='^$' -fuzz=FuzzUnpack      -fuzztime=20s
```

In each submodule (still with the `replace`, so it exercises the working tree):

```sh
for m in quic resilience; do (cd "$m" && go build ./... && go vet ./... && go test -race ./...) || break; done
```

Update `CHANGELOG.md` (move `[Unreleased]` to the new version + date) and confirm
any breaking changes are called out (e.g. the `Authenticator` now returns a
principal; transport constructors and `Dialer.Dial` take a `context`/`maxFrameSize`).

## 2. Tag the core module

```sh
git tag value-rpc/vX.Y.Z   # or vX.Y.Z, matching the module path convention in use
git push origin <tag>
```

## 3. Release each submodule (drop the replace)

A submodule can only be tagged once core is tagged and its `require` points at that
tag instead of the local path. Repeat for `quic` and `resilience`:

```sh
cd <submodule>            # quic, then resilience
# remove the dev override and pin the just-released core
go mod edit -dropreplace=go.arpabet.com/value-rpc
go mod edit -require=go.arpabet.com/value-rpc@vX.Y.Z
go mod tidy
go build ./... && go test ./...        # builds against the published core, no ../
git add go.mod go.sum
git commit -m "<submodule>: release vA.B.C against value-rpc vX.Y.Z"
git tag value-rpc/<submodule>/vA.B.C
git push origin <tag>
```

## 4. Restore the dev override

After tagging, re-add the `replace` in each submodule so day-to-day development
keeps building against the working tree:

```sh
for m in quic resilience; do
  (cd "$m" && go mod edit -replace=go.arpabet.com/value-rpc=../)
done
git add quic/go.mod resilience/go.mod
git commit -m "submodules: restore local replace for development"
```

## Downstream

`servion/vrpc` (and any other consumer) bumps its `require go.arpabet.com/value-rpc`
to the new tag; if it used a local `replace`, drop it. The QUIC submodule must NOT
be bundled into core (`git show <core-tag>:go.mod` must contain no `quic-go`).
