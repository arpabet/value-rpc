/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valueserver

import (
	"context"

	vrpc "go.arpabet.com/value-rpc/valuerpc"
)

type peerCtxKey struct{}

func contextWithPeer(ctx context.Context, p vrpc.Peer) context.Context {
	return context.WithValue(ctx, peerCtxKey{}, p)
}

// PeerFromContext returns the Peer handle for the connected client whose request
// is currently being served, so a handler can call back into that client
// (server->client reverse RPC) — CallFunction, GetStream, PutStream, or Chat, the
// same surface the client uses toward the server. The handle stays valid for the
// connection's lifetime, so a handler can cache it — e.g. keyed by the
// authenticated principal — to reach this client later from a different request
// (the pattern for routing a call from client A through the server to client B).
// Returns false when the served connection has no peer handle.
func PeerFromContext(ctx context.Context) (vrpc.Peer, bool) {
	p, ok := ctx.Value(peerCtxKey{}).(vrpc.Peer)
	return p, ok
}
