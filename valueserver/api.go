/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valueserver

import (
	"context"
	"net"

	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valuerpc"
)

// Handler signatures receive a context.Context as their first argument. The
// context is cancelled when the connection drops, the server shuts down, the
// request is cancelled (client CancelRequest), or the client's SLA deadline
// elapses — handlers should honour it for cancellation and deadline propagation
// to downstream work.
type Function func(ctx context.Context, args value.Value) (value.Value, error)
type OutgoingStream func(ctx context.Context, args value.Value) (<-chan value.Value, error)
type IncomingStream func(ctx context.Context, args value.Value, inC <-chan value.Value) error
type Chat func(ctx context.Context, args value.Value, inC <-chan value.Value) (<-chan value.Value, error)

// ConnectAuthorizer is called once per new connection, before the handshake. If
// it returns an error the connection is rejected and closed. Combine it with
// valuerpc.PeerCredOf for Unix-domain-socket peer authorization.
type ConnectAuthorizer func(conn valuerpc.MsgConn) error

type Server interface {
	AddFunction(name string, args valuerpc.TypeDef, res valuerpc.TypeDef, cb Function) error

	// GET for client
	AddOutgoingStream(name string, args valuerpc.TypeDef, cb OutgoingStream) error

	// PUT for client
	AddIncomingStream(name string, args valuerpc.TypeDef, cb IncomingStream) error

	// Dual channel chat
	AddChat(name string, args valuerpc.TypeDef, cb Chat) error

	// Addr returns the address the server is listening on. Useful when binding
	// to an ephemeral port (":0").
	Addr() net.Addr

	// SetConnectAuthorizer installs a per-connection authorization hook (called
	// before the handshake). Call before Run. Passing nil clears it.
	SetConnectAuthorizer(fn ConnectAuthorizer)

	Run() error

	Close() error
}
