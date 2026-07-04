/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valueclient

import (
	"context"
	"testing"

	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valuerpc"
)

// captureErrorHandler records the last ProtocolError so tests can assert the
// reverse-id validation fired.
type captureErrorHandler struct {
	lastProtocolErr error
}

func (h *captureErrorHandler) BadConnection(error)                  {}
func (h *captureErrorHandler) ProtocolError(_ value.Map, err error) { h.lastProtocolErr = err }
func (h *captureErrorHandler) StreamError(int64, error)             {}

// TestServeInboundRejectsNonNegativeId verifies R4 on the client side: a
// server-initiated request (reverse call or reverse stream) that carries a
// non-negative id — which would shadow the client's own outstanding requests —
// is rejected as a protocol error and not dispatched.
func TestServeInboundRejectsNonNegativeId(t *testing.T) {
	t.Cleanup(func() {}) // keep imports tidy across Go versions

	c := NewClientWithDialer(valuerpc.NewMemDialer("unused-reverse-id")).(*rpcClient)
	defer c.Close()

	handled := false
	_ = c.AddFunction("rev", valuerpc.Any, valuerpc.Any,
		func(context.Context, value.Value) (value.Value, error) {
			handled = true
			return value.Null, nil
		})

	eh := &captureErrorHandler{}
	c.SetErrorHandler(eh)

	// A reverse FunctionRequest must use a negative id; a positive one is invalid.
	badCall := value.EmptyMap(true).
		Put(valuerpc.DefaultDialect.MessageTypeField, valuerpc.FunctionRequest.Long()).
		Put(valuerpc.DefaultDialect.RequestIdField, value.Long(7)).
		Put(valuerpc.DefaultDialect.FunctionNameField, value.Utf8("rev"))
	c.serveInboundCall(badCall)
	if eh.lastProtocolErr == nil {
		t.Fatal("positive-id reverse call was not rejected")
	}
	if handled {
		t.Fatal("positive-id reverse call was dispatched to the handler")
	}

	// Same rule for a reverse stream open.
	eh.lastProtocolErr = nil
	badStream := value.EmptyMap(true).
		Put(valuerpc.DefaultDialect.MessageTypeField, valuerpc.GetStreamRequest.Long()).
		Put(valuerpc.DefaultDialect.RequestIdField, value.Long(3)).
		Put(valuerpc.DefaultDialect.FunctionNameField, value.Utf8("rev"))
	c.serveInboundStream(valuerpc.GetStreamRequest, badStream)
	if eh.lastProtocolErr == nil {
		t.Fatal("positive-id reverse stream open was not rejected")
	}
}
