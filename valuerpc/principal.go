/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valuerpc

import "context"

type principalKey struct{}

// ContextWithPrincipal returns a copy of ctx carrying the authenticated
// principal — the identity the server's Authenticator derived for the
// connection during the handshake. The server attaches it to every handler's
// context so handlers can authorize and attribute work to the connection-bound
// identity (read it back with PrincipalFromContext) instead of trusting a value
// the caller put in the request payload.
func ContextWithPrincipal(ctx context.Context, principal string) context.Context {
	return context.WithValue(ctx, principalKey{}, principal)
}

// PrincipalFromContext returns the authenticated principal attached to ctx, or
// "" when the connection was not authenticated (no Authenticator, or an
// anonymous principal).
func PrincipalFromContext(ctx context.Context) string {
	p, _ := ctx.Value(principalKey{}).(string)
	return p
}
