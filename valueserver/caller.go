/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valueserver

import (
	"context"

	vrpc "go.arpabet.com/value-rpc/valuerpc"
)

type clientCtxKey struct{}

func contextWithClient(ctx context.Context, p vrpc.Peer) context.Context {
	return context.WithValue(ctx, clientCtxKey{}, p)
}

// ClientFromContext returns the Peer handle for the client whose request is
// currently being served, so a handler can call back into that client
// (server->client reverse RPC) — CallFunction, GetStream, PutStream, or Chat,
// the same surface the client uses toward the server. The handle stays valid for
// the connection's lifetime, so a handler can cache it — e.g. keyed by the
// authenticated principal — to reach this client later from a different request
// (the pattern for routing a call from client A through the server to client B).
// Returns false when the served connection has no peer handle.
func ClientFromContext(ctx context.Context) (vrpc.Peer, bool) {
	p, ok := ctx.Value(clientCtxKey{}).(vrpc.Peer)
	return p, ok
}
