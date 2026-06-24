/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valuerpc

import (
	"strings"
	"testing"
)

// TestHashChainForwardVerify reveals a whole chain in reverse and checks that each
// link verifies as exactly one forward step from the previous one, that the chain
// reports exhaustion, and that a link never verifies as a forward step from itself
// (only as an idempotent SameToken).
func TestHashChainForwardVerify(t *testing.T) {
	const n = 50
	ch, err := NewHashChain(n)
	if err != nil {
		t.Fatalf("new chain: %v", err)
	}
	if ch.Remaining() != n {
		t.Fatalf("Remaining = %d, want %d", ch.Remaining(), n)
	}

	last := ch.Anchor()
	for i := 0; i < n; i++ {
		tok := ch.Next()
		if tok == "" {
			t.Fatalf("chain exhausted early at %d", i)
		}
		step, ok := VerifyHashStep(tok, last, DefaultResyncWindow)
		if !ok || step != 1 {
			t.Fatalf("link %d: VerifyHashStep = (%d, %v), want (1, true)", i, step, ok)
		}
		// A replay of the just-accepted link is not a forward step, but is the same
		// token (the idempotent-retry path on the server).
		if _, ok := VerifyHashStep(tok, tok, DefaultResyncWindow); ok {
			t.Fatalf("link %d verified as a forward step from itself", i)
		}
		if !SameToken(tok, tok) {
			t.Fatalf("link %d: SameToken(tok, tok) = false", i)
		}
		last = tok
	}

	if got := ch.Next(); got != "" {
		t.Fatalf("exhausted chain returned %q, want empty", got)
	}
	if ch.Remaining() != 0 {
		t.Fatalf("Remaining = %d after exhaustion, want 0", ch.Remaining())
	}
}

// TestHashChainToleratesGaps is the loss-tolerance property: when the client skips
// ahead (dropped reconnect handshakes), the server re-syncs by hashing the
// presented link forward the right number of steps.
func TestHashChainToleratesGaps(t *testing.T) {
	ch, err := NewHashChain(100)
	if err != nil {
		t.Fatalf("new chain: %v", err)
	}
	anchor := ch.Anchor()

	const consumed = 10 // 9 "dropped" links + the one that reaches the server
	var tok string
	for i := 0; i < consumed; i++ {
		tok = ch.Next()
	}
	step, ok := VerifyHashStep(tok, anchor, DefaultResyncWindow)
	if !ok || step != consumed {
		t.Fatalf("gap resync: VerifyHashStep = (%d, %v), want (%d, true)", step, ok, consumed)
	}
}

// TestHashChainWindowBound checks the resync window both caps the work and is
// honoured: a gap larger than the window fails, a sufficient window succeeds.
func TestHashChainWindowBound(t *testing.T) {
	ch, err := NewHashChain(100)
	if err != nil {
		t.Fatalf("new chain: %v", err)
	}
	anchor := ch.Anchor()

	var tok string
	for i := 0; i < 20; i++ {
		tok = ch.Next()
	}
	if _, ok := VerifyHashStep(tok, anchor, 5); ok {
		t.Fatal("verify succeeded with a gap larger than the window")
	}
	if _, ok := VerifyHashStep(tok, anchor, 50); !ok {
		t.Fatal("verify failed within an adequate window")
	}
}

// TestHashChainRejectsForgery checks that forged, replayed, and malformed links are
// rejected rather than accepted (or panicking).
func TestHashChainRejectsForgery(t *testing.T) {
	ch, err := NewHashChain(10)
	if err != nil {
		t.Fatalf("new chain: %v", err)
	}
	anchor := ch.Anchor()

	// A well-formed but forged 32-byte link cannot hash forward to the anchor.
	if _, ok := VerifyHashStep(strings.Repeat("ab", 32), anchor, DefaultResyncWindow); ok {
		t.Fatal("forged link verified against the anchor")
	}
	// Preimage resistance: the anchor itself does not verify "forward" to anything
	// already revealed, and a future link cannot be guessed — covered by the fact
	// that the only values that verify are genuine pre-images produced by Next.

	// Malformed inputs (wrong length, non-hex, empty) are rejected, not fatal.
	for _, bad := range []string{"", "xyz", "nothex-nothex-nothex-nothex-nothex-nothex-nothex-nothex-nothexx", strings.Repeat("a", 63)} {
		if _, ok := VerifyHashStep(bad, anchor, DefaultResyncWindow); ok {
			t.Fatalf("malformed link %q verified", bad)
		}
		if SameToken(bad, anchor) {
			t.Fatalf("malformed link %q compared equal to the anchor", bad)
		}
	}
	if SameToken("", "") {
		t.Fatal("empty tokens compared equal")
	}
}
