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

package server

import "github.com/consensusdb/value"

/**
@author Alex Shvid
*/

type Function func(args value.List) (value.Value, error)
type OutgoingStream func(args value.List) (<-chan value.Value, error)
type IncomingStream func(args value.List, inC <-chan value.Value) error
type Chat func(args value.List, inC <-chan value.Value) (<-chan value.Value, error)

type Server interface {
	AddFunction(name string, numArgs int, cb Function) error

	// GET for client
	AddOutgoingStream(name string, numArgs int, cb OutgoingStream) error

	// PUT for client
	AddIncomingStream(name string, numArgs int, cb IncomingStream) error

	// Dual channel chat
	AddChat(name string, numArgs int, cb Chat) error

	Run() error

	Close() error
}
