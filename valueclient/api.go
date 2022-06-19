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
