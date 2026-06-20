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

func contextWithClient(ctx context.Context, c vrpc.Caller) context.Context {
	return context.WithValue(ctx, clientCtxKey{}, c)
}

// ClientFromContext returns the caller handle for the client whose request is
// currently being served, so a handler can call a function back on that client
// (server->client reverse RPC). The handle stays valid for the connection's
// lifetime, so a handler can cache it — e.g. keyed by the authenticated
// principal — to reach this client later from a different request (the pattern
// for routing a call from client A through the server to client B). Returns
// false when the served connection has no caller handle.
func ClientFromContext(ctx context.Context) (vrpc.Caller, bool) {
	c, ok := ctx.Value(clientCtxKey{}).(vrpc.Caller)
	return c, ok
}
