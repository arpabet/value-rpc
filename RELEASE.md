<!--
  Copyright (c) 2025-2026 Karagatan LLC.
  SPDX-License-Identifier: BUSL-1.1
-->

# Release checklist

vRPC ships as two modules from this repo:

- `go.arpabet.com/value-rpc` — core (no `quic-go` dependency).
- `go.arpabet.com/value-rpc/quic` — the QUIC transport (`quic/`), a **separate
  module** so `quic-go` only enters builds that use it.

During development `quic/go.mod` carries a local override so the submodule builds
against the working tree:

```
replace go.arpabet.com/value-rpc => ../
```

This `replace` **must not** be present in a published tag — a consumer that pulls
`…/value-rpc/quic@vX` would otherwise fail (there is no `../`). Releasing the two
modules is therefore a two-step, ordered process.

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

In `quic/` (still with the `replace`, so it exercises the working tree):

```sh
cd quic && go build ./... && go vet ./... && go test -race ./... && cd ..
```

Update `CHANGELOG.md` (move `[Unreleased]` to the new version + date) and confirm
any breaking changes are called out (e.g. the `Authenticator` now returns a
principal; transport constructors and `Dialer.Dial` take a `context`/`maxFrameSize`).

## 2. Tag the core module

```sh
git tag value-rpc/vX.Y.Z   # or vX.Y.Z, matching the module path convention in use
git push origin <tag>
```

## 3. Release the quic submodule (drop the replace)

The submodule can only be tagged once core is tagged and its `require` points at
that tag instead of the local path.

```sh
cd quic
# remove the dev override and pin the just-released core
go mod edit -dropreplace=go.arpabet.com/value-rpc
go mod edit -require=go.arpabet.com/value-rpc@vX.Y.Z
go mod tidy
go build ./... && go test ./...        # builds against the published core, no ../
git add go.mod go.sum
git commit -m "quic: release vA.B.C against value-rpc vX.Y.Z"
git tag value-rpc/quic/vA.B.C
git push origin <tag>
```

## 4. Restore the dev override

After tagging, re-add the `replace` so day-to-day development keeps building
against the working tree:

```sh
cd quic
go mod edit -replace=go.arpabet.com/value-rpc=../
git add go.mod && git commit -m "quic: restore local replace for development"
```

## Downstream

`servion/vrpc` (and any other consumer) bumps its `require go.arpabet.com/value-rpc`
to the new tag; if it used a local `replace`, drop it. The QUIC submodule must NOT
be bundled into core (`git show <core-tag>:go.mod` must contain no `quic-go`).
