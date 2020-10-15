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
	"arpabet.pkg.is/value"
	"arpabet.pkg.is/value-rpc/valuerpc"
)

/**
@author Alex Shvid
*/

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

