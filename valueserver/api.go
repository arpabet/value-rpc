/**
  Copyright (c) 2022 Arpabet, LLC. All rights reserved.
*/

package valueserver

import (
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

	Run() error

	Close() error
}

