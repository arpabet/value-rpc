/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valuerpc

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"sync"
)

// Reverse hash chain (S/KEY / Lamport style) for replay-resistant session
// resumption. A static bearer token — the previous design — can be captured once
// by a passive eavesdropper on an untrusted link and replayed forever to hijack a
// session. A reverse hash chain replaces it with a one-time token per reconnect:
//
//	seed = h[0],  h[i] = SHA-256(h[i-1]),  anchor = h[N]
//
// The client commits to the chain by sending the anchor h[N] on first connect;
// the server stores it. On each reconnect the client reveals the next pre-image
// in reverse order (h[N-1], then h[N-2], …). The server authorizes the resume iff
// hashing the presented value forward a few times reproduces the last value it
// accepted, then advances its stored value to the presented one. Preimage
// resistance means an eavesdropper who captured h[N-k] cannot compute the next
// token h[N-k-1], and a replay of an already-consumed link no longer hashes
// forward to the stored value — so sniffed tokens are useless on the next
// reconnect.
//
// Loss tolerance (self-healing): the client ALWAYS advances its chain on a
// reconnect, even when the handshake carrying the token is dropped, so it never
// reveals a link twice. The server absorbs the gap by hashing the presented value
// forward up to a bounded window (DefaultResyncWindow) instead of requiring an
// exact one-step match — so a run of lost reconnect handshakes self-heals on the
// next successful one.
//
// Scope — read this: this hardens resumption against PASSIVE capture-and-replay
// over an untrusted (e.g. non-TLS) link. It is NOT a secure channel: it provides
// no confidentiality and does not stop an ACTIVE on-path attacker who can suppress
// the real handshake and present the same pre-image first. Resume is additionally
// gated by the authenticated principal (the credential), so that active attack
// also requires forging the credential. For confidentiality and integrity, run
// over TLS or a Noise transport; the chain is an auth hardening, not a substitute.
const (
	// DefaultHashChainLen is how many one-time resumption tokens a client
	// precomputes per session: one is consumed per reconnect, so it bounds the
	// number of reconnects before a fresh session must be established. 10000 links
	// is ~320 KB of precomputed SHA-256 state — negligible — and far more
	// reconnects than a normal session ever performs.
	DefaultHashChainLen = 10000

	// DefaultResyncWindow bounds how many times the server hashes a presented
	// pre-image forward looking for a match. It is therefore both (a) the number of
	// consecutive dropped reconnect handshakes the chain self-heals across, and
	// (b) the most SHA-256 work a bogus token can force on the server (a DoS
	// bound). Keep it modest.
	DefaultResyncWindow = 1024

	hashChainLinkBytes = sha256.Size // 32
)

// HashChain is a client-held reverse hash chain. It is safe for concurrent use:
// Next is serialized so a reconnect racing a connect can never reveal a link
// twice.
type HashChain struct {
	mu    sync.Mutex
	links [][hashChainLinkBytes]byte // links[0]=seed … links[n]=anchor
	next  int                        // index of the next pre-image to reveal
}

// NewHashChain precomputes a chain of n links (n+1 stored values including the
// seed and the anchor) from a fresh cryptographically-random seed. n <= 0 uses
// DefaultHashChainLen. The seed is never exposed; only the anchor and, later,
// pre-images down from it are revealed.
func NewHashChain(n int) (*HashChain, error) {
	if n <= 0 {
		n = DefaultHashChainLen
	}
	links := make([][hashChainLinkBytes]byte, n+1)
	if _, err := rand.Read(links[0][:]); err != nil {
		return nil, err
	}
	for i := 1; i <= n; i++ {
		links[i] = sha256.Sum256(links[i-1][:])
	}
	return &HashChain{links: links, next: n - 1}, nil
}

// Anchor returns the chain's public commitment h[N] (hex-encoded). It is sent on
// the first handshake and is safe to resend on a retried first connect: it
// reveals nothing an observer cannot derive and the server stores it
// idempotently. Resending the anchor does NOT consume a link.
func (c *HashChain) Anchor() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return hex.EncodeToString(c.links[len(c.links)-1][:])
}

// Next reveals the next one-time resumption token (the next pre-image, in reverse
// order) and advances the chain. It ALWAYS advances, even though the handshake
// carrying the token may then be lost: the server self-heals across the gap by
// hashing forward (DefaultResyncWindow), so a link must never be revealed twice.
// Returns "" when the chain is exhausted and a fresh session is required.
func (c *HashChain) Next() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.next < 0 {
		return ""
	}
	tok := hex.EncodeToString(c.links[c.next][:])
	c.next--
	return tok
}

// Remaining reports how many one-time tokens are left before the chain is
// exhausted.
func (c *HashChain) Remaining() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.next + 1
}

// VerifyHashStep reports whether presented is a valid next link for a chain whose
// last accepted value is lastVerified, tolerating up to window dropped reconnect
// handshakes by hashing presented forward until it reproduces lastVerified. On
// success it returns the number of hash steps consumed (>= 1) and true; the
// caller must then store presented as the new last-verified value. presented and
// lastVerified are hex-encoded SHA-256 links (the wire form). A value that does
// not reach lastVerified within window steps — including a replay of an
// already-consumed link, or lastVerified itself — returns (0, false). window <= 0
// uses DefaultResyncWindow.
func VerifyHashStep(presented, lastVerified string, window int) (int, bool) {
	cur, ok := decodeLink(presented)
	if !ok {
		return 0, false
	}
	target, ok := decodeLink(lastVerified)
	if !ok {
		return 0, false
	}
	if window <= 0 {
		window = DefaultResyncWindow
	}
	h := cur[:]
	for step := 1; step <= window; step++ {
		sum := sha256.Sum256(h)
		h = sum[:]
		// lastVerified is already public (revealed on a prior handshake); the
		// constant-time compare is defence-in-depth and matches house style.
		if subtle.ConstantTimeCompare(h, target[:]) == 1 {
			return step, true
		}
	}
	return 0, false
}

// SameToken reports whether two hex-encoded links are equal, in constant time. The
// server uses it to accept an idempotent handshake retry — a lost handshake
// *response* makes the client re-present the same value — without advancing the
// chain. Empty or malformed inputs never compare equal.
func SameToken(a, b string) bool {
	da, ok := decodeLink(a)
	if !ok {
		return false
	}
	db, ok := decodeLink(b)
	if !ok {
		return false
	}
	return subtle.ConstantTimeCompare(da[:], db[:]) == 1
}

func decodeLink(s string) ([hashChainLinkBytes]byte, bool) {
	var out [hashChainLinkBytes]byte
	if len(s) != hex.EncodedLen(hashChainLinkBytes) {
		return out, false
	}
	if _, err := hex.Decode(out[:], []byte(s)); err != nil {
		return out, false
	}
	return out, true
}
