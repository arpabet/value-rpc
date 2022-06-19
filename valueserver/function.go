/**
    Copyright (c) 2020-2022 Arpabet, Inc.

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in
	all copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
	THE SOFTWARE.
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
