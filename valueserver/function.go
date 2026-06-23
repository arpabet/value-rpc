/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valueserver

import (
	vrpc "go.arpabet.com/value-rpc/valuerpc"
	"golang.org/x/xerrors"
)

var ErrFunctionAlreadyExist = xerrors.New("function already exist")

type functionType int

const (
	singleFunction functionType = iota
	outgoingStream
	incomingStream
	chat
)

type function struct {
	name      string
	args      vrpc.TypeDef
	res       vrpc.TypeDef
	ft        functionType
	singleFn  vrpc.Function
	outStream vrpc.OutgoingStream
	inStream  vrpc.IncomingStream
	chat      vrpc.Chat
}

func (t *rpcServer) hasFunction(name string) bool {
	if _, ok := t.functionMap.Load(name); ok {
		return true
	}
	return false
}

func (t *rpcServer) AddFunction(name string, args vrpc.TypeDef, res vrpc.TypeDef, cb vrpc.Function) error {
	if t.hasFunction(name) {
		return ErrFunctionAlreadyExist
	}

	fn := &function{
		name:     name,
		args:     args,
		res:      res,
		ft:       singleFunction,
		singleFn: cb,
	}

	t.functionMap.Store(name, fn)
	return nil
}

// GET for client
func (t *rpcServer) AddOutgoingStream(name string, args vrpc.TypeDef, cb vrpc.OutgoingStream) error {
	if t.hasFunction(name) {
		return ErrFunctionAlreadyExist
	}

	fn := &function{
		name:      name,
		args:      args,
		res:       vrpc.Void,
		ft:        outgoingStream,
		outStream: cb,
	}

	t.functionMap.Store(name, fn)
	return nil
}

// PUT for client
func (t *rpcServer) AddIncomingStream(name string, args vrpc.TypeDef, cb vrpc.IncomingStream) error {
	if t.hasFunction(name) {
		return ErrFunctionAlreadyExist
	}

	fn := &function{
		name:     name,
		args:     args,
		res:      vrpc.Void,
		ft:       incomingStream,
		inStream: cb,
	}

	t.functionMap.Store(name, fn)
	return nil
}

func (t *rpcServer) AddChat(name string, args vrpc.TypeDef, cb vrpc.Chat) error {
	if t.hasFunction(name) {
		return ErrFunctionAlreadyExist
	}

	fn := &function{
		name: name,
		args: args,
		res:  vrpc.Void,
		ft:   chat,
		chat: cb,
	}

	t.functionMap.Store(name, fn)
	return nil
}
