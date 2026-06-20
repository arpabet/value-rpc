/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valueclient

import (
	"context"

	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valuerpc"
)

// must be fast function
type PerformanceMonitor func(name string, elapsed int64)
type ConnectionHandler func(connected value.Map)

type Client interface {
	// Peer is the established-connection surface for calling the server:
	// CallFunction, GetStream, PutStream, Chat. The context bounds each call: a
	// deadline sooner than SetTimeout becomes the request SLA, and cancelling it
	// cancels the call (and, for a stream, tears the stream down). Pass
	// context.Background() when no deadline/cancellation is needed. Embedding Peer
	// here makes the relationship explicit: a connected Client *is* a Peer.
	valuerpc.Peer

	ClientId() int64

	Connect() error

	// ConnectContext is Connect with a context bounding the dial. Without a
	// context deadline the configured dial timeout (WithDialTimeout) applies.
	ConnectContext(ctx context.Context) error

	Reconnect() error

	IsActive() bool

	Stats() map[string]int64

	SetMonitor(PerformanceMonitor)

	SetConnectionHandler(ConnectionHandler)

	SetErrorHandler(ErrorHandler)

	SetTimeout(timeoutMls int64)

	// SetCredential sets the credential sent in the handshake (and re-sent on
	// every reconnect) for the server's Authenticator to validate. Call before
	// Connect. Pass nil to clear it.
	SetCredential(credential value.Value)

	CancelRequest(requestId int64)

	// Registrar is the handler-registration surface for serving the server's
	// reverse calls/streams (AddFunction / AddOutgoingStream / AddIncomingStream /
	// AddChat) — identical to valueserver.Server's, so the two sides of value-rpc
	// register handlers the same way. Registrations persist across reconnects.
	valuerpc.Registrar

	Close() error
}
