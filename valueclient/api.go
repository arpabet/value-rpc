/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: Apache-2.0
 */

package valueclient


import (
	"go.arpabet.com/value"
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

	CallFunction(name string, args value.Value) (value.Value, error)

	GetStream(name string, args value.Value, receiveCap int) (<-chan value.Value, int64, error)

	PutStream(name string, args value.Value, putCh <-chan value.Value) error

	Chat(name string, args value.Value, receiveCap int, putCh <-chan value.Value) (<-chan value.Value, int64, error)

	Close() error
}
