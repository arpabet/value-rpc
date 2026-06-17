/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valueclient

import (
	"context"

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

	// The context bounds the call: if it carries a deadline sooner than
	// SetTimeout, that deadline is sent to the server as the request SLA; if the
	// context is cancelled (or its deadline elapses), the request is cancelled
	// (CancelRequest) and the call returns ctx.Err(). For streams the context
	// governs the whole stream lifetime — cancelling it tears the stream down.
	// Pass context.Background() when no deadline/cancellation is needed.
	CallFunction(ctx context.Context, name string, args value.Value) (value.Value, error)

	GetStream(ctx context.Context, name string, args value.Value, receiveCap int) (<-chan value.Value, int64, error)

	PutStream(ctx context.Context, name string, args value.Value, putCh <-chan value.Value) error

	Chat(ctx context.Context, name string, args value.Value, receiveCap int, putCh <-chan value.Value) (<-chan value.Value, int64, error)

	Close() error
}
