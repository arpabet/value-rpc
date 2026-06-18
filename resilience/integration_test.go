/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package resilience_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/resilience"
	"go.arpabet.com/value-rpc/valueclient"
	vrpc "go.arpabet.com/value-rpc/valuerpc"
	"go.arpabet.com/value-rpc/valueserver"
	"go.uber.org/zap"
)

// TestRetryEndToEnd wires the Retry interceptor into a real client via
// WithInterceptors and confirms it transparently recovers a transient failure
// over an actual connection.
func TestRetryEndToEnd(t *testing.T) {
	var calls int32
	srv, err := valueserver.NewMemServer("resilience-e2e", zap.NewNop())
	if err != nil {
		t.Fatalf("server: %v", err)
	}
	srv.AddFunction("flaky", vrpc.Any, vrpc.Any,
		func(_ context.Context, _ value.Value) (value.Value, error) {
			if atomic.AddInt32(&calls, 1) < 3 {
				return nil, vrpc.NewError(vrpc.CodeUnavailable, "transient")
			}
			return value.Utf8("ok"), nil
		})
	go srv.Run()
	defer srv.Close()

	cli := valueclient.NewMemClient("resilience-e2e", valueclient.WithInterceptors(
		resilience.Retry(
			resilience.WithMaxAttempts(5),
			resilience.WithBackoff(time.Millisecond, 5*time.Millisecond),
		),
	))
	if err := cli.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer cli.Close()

	res, err := cli.CallFunction(context.Background(), "flaky", nil)
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if res.(value.String).String() != "ok" {
		t.Fatalf("result = %v, want ok", res)
	}
	if n := atomic.LoadInt32(&calls); n != 3 {
		t.Fatalf("server saw %d calls, want 3 (2 transient + 1 success)", n)
	}
}
