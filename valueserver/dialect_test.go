/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valueserver_test

import (
	"context"
	"testing"
	"time"

	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valueclient"
	"go.arpabet.com/value-rpc/valuerpc"
	"go.arpabet.com/value-rpc/valueserver"
)

// TestCustomDialectEndToEnd installs a custom wire dialect process-wide and
// verifies (a) a matching client still works end to end, and (b) a peer speaking
// the standard dialect is rejected at the handshake — i.e. the dialect makes the
// protocol incompatible by design across the whole stack.
func TestCustomDialectEndToEnd(t *testing.T) {
	old := valuerpc.DefaultDialect
	t.Cleanup(func() { valuerpc.DefaultDialect = old })

	custom := valuerpc.NewDialect()
	custom.Magic = "ZZ"
	custom.MagicField = "z"
	custom.MessageTypeField = "q"
	custom.RequestIdField = "r"
	custom.FunctionNameField = "f9"
	custom.ArgumentsField = "a9"
	custom.ResultField = "x9"
	valuerpc.DefaultDialect = custom // both client and server now speak it

	addr, stop := newServer(t, func(s valueserver.Server) {
		s.AddFunction("echo", valuerpc.Any, valuerpc.Any,
			func(_ context.Context, a value.Value) (value.Value, error) { return a, nil })
	})
	defer stop()

	// (a) A matching client round-trips over the custom dialect.
	cli := dialClient(t, addr)
	defer cli.Close()
	res, err := cli.CallFunction(context.Background(), "echo", value.Utf8("hi"))
	if err != nil {
		t.Fatalf("call under custom dialect: %v", err)
	}
	if got := res.(value.String).String(); got != "hi" {
		t.Fatalf("echo = %q, want hi", got)
	}

	// (b) A standard-dialect handshake is rejected by the custom-dialect server.
	std := valuerpc.NewDialect() // standard "vRPC"/"m"/"t"/... markers
	dialer := valuerpc.NewDialer(addr, "", valueclient.KeepAlivePeriod, valueclient.DefaultTimeout, valuerpc.MaxFrameSize)
	conn, err := dialer.Dial(context.Background())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	if err := conn.WriteMessage(std.NewHandshakeRequest(1, "")); err != nil {
		t.Fatalf("write standard handshake: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.ReadMessage(); err == nil {
		t.Fatal("custom-dialect server accepted a standard-dialect handshake (dialects should be incompatible)")
	}
}
