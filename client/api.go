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

package client

/**
@author Alex Shvid
*/

import (
	"github.com/consensusdb/value"
)

// must be fast function
type PerformanceMonitor func(name string, elapsed int64)
type ConnectionHandler func(connected value.Map)

type Client interface {
	ClientId() int64

	Connect() error

	Reconnect() error

	IsActive() bool

	Stats() map[string]int64

	SetMonitor(PerformanceMonitor)

	SetConnectionHandler(ConnectionHandler)

	SetErrorHandler(ErrorHandler)

	SetTimeout(timeoutMls int64)

	CancelRequest(requestId int64)

	CallFunction(name string, args value.List) (value.Value, error)

	GetStream(name string, args value.List, receiveCap int) (<-chan value.Value, int64, error)

	PutStream(name string, args value.List, putCh <-chan value.Value) error

	Chat(name string, args value.List, receiveCap int, putCh <-chan value.Value) (<-chan value.Value, int64, error)

	Close() error
}
