/**
  Copyright (c) 2022 Arpabet, LLC. All rights reserved.
*/

package valueserver

import (
	"errors"
	vrpc "go.arpabet.com/value-rpc/valuerpc"
)


var ErrFunctionAlreadyExist = errors.New("function already exist")

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
	singleFn  Function
	outStream OutgoingStream
	inStream  IncomingStream
	chat      Chat
}

func (t *rpcServer) hasFunction(name string) bool {
	if _, ok := t.functionMap.Load(name); ok {
		return true
	}
	return false
}

func (t *rpcServer) AddFunction(name string, args vrpc.TypeDef, res vrpc.TypeDef, cb Function) error {
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
func (t *rpcServer) AddOutgoingStream(name string, args vrpc.TypeDef, cb OutgoingStream) error {
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
func (t *rpcServer) AddIncomingStream(name string, args vrpc.TypeDef, cb IncomingStream) error {
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

func (t *rpcServer) AddChat(name string, args vrpc.TypeDef, cb Chat) error {
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
