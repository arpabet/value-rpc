/*
 * Copyright (c) 2025 Karagatan LLC.
 * SPDX-License-Identifier: Apache-2.0
 */

package valueserver

import (
	"net"

	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valuerpc"
)


type Function func(args value.Value) (value.Value, error)
type OutgoingStream func(args value.Value) (<-chan value.Value, error)
type IncomingStream func(args value.Value, inC <-chan value.Value) error
type Chat func(args value.Value, inC <-chan value.Value) (<-chan value.Value, error)

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

	Run() error

	Close() error
}

