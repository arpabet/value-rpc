/*
 *
 * Copyright 2020-present Arpabet Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package valueserver

import (
	"errors"
	vrpc "arpabet.pkg.is/value-rpc/valuerpc"
)

/**
@author Alex Shvid
*/

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
