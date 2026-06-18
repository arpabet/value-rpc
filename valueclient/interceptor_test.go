/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valueclient_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valueclient"
	"go.arpabet.com/value-rpc/valuerpc"
	"go.arpabet.com/value-rpc/valueserver"
)

// TestUnaryInterceptorRetry shows a retry interceptor (the governance use case):
// it re-invokes on a retryable CodeUnavailable, classified via valuerpc.CodeOf.
func TestUnaryInterceptorRetry(t *testing.T) {
	var calls int32
	sock, stop := serve(t, func(s valueserver.Server) {
		s.AddFunction("flaky", valuerpc.Any, valuerpc.Any,
			func(_ context.Context, _ value.Value) (value.Value, error) {
				if atomic.AddInt32(&calls, 1) == 1 {
					return nil, valuerpc.NewError(valuerpc.CodeUnavailable, "transient")
				}
				return value.Utf8("ok"), nil
			})
	})
	defer stop()

	retry := func(ctx context.Context, method string, req value.Value, next valuerpc.Invoker) (value.Value, error) {
		res, err := next(ctx, method, req)
		if valuerpc.CodeOf(err) == valuerpc.CodeUnavailable {
			return next(ctx, method, req) // one retry
		}
		return res, err
	}

	cli := dial(t, sock, valueclient.WithInterceptors(retry))
	defer cli.Close()

	res, err := cli.CallFunction(context.Background(), "flaky", nil)
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if res.(value.String).String() != "ok" {
		t.Fatalf("result = %v", res)
	}
	if n := atomic.LoadInt32(&calls); n != 2 {
		t.Fatalf("server saw %d calls, want 2 (one transient + one retry)", n)
	}
}

// TestUnaryInterceptorOrderAndShortCircuit verifies interceptors run outermost-
// first and that one can short-circuit without reaching the server.
func TestUnaryInterceptorOrderAndShortCircuit(t *testing.T) {
	var served int32
	sock, stop := serve(t, func(s valueserver.Server) {
		s.AddFunction("echo", valuerpc.Any, valuerpc.Any,
			func(_ context.Context, a value.Value) (value.Value, error) {
				atomic.AddInt32(&served, 1)
				return a, nil
			})
	})
	defer stop()

	var mu sync.Mutex
	var order []string
	record := func(name string) valuerpc.ClientInterceptor {
		return func(ctx context.Context, method string, req value.Value, next valuerpc.Invoker) (value.Value, error) {
			mu.Lock()
			order = append(order, name)
			mu.Unlock()
			return next(ctx, method, req)
		}
	}
	// "deny" short-circuits: it never calls next, so the server is not reached.
	deny := func(_ context.Context, _ string, _ value.Value, _ valuerpc.Invoker) (value.Value, error) {
		return nil, valuerpc.NewError(valuerpc.CodeUnauthenticated, "denied")
	}

	cli := dial(t, sock, valueclient.WithInterceptors(record("first"), record("second"), deny))
	defer cli.Close()

	_, err := cli.CallFunction(context.Background(), "echo", value.Utf8("hi"))
	if valuerpc.CodeOf(err) != valuerpc.CodeUnauthenticated {
		t.Fatalf("code = %v, want Unauthenticated (short-circuited)", valuerpc.CodeOf(err))
	}
	mu.Lock()
	defer mu.Unlock()
	if len(order) != 2 || order[0] != "first" || order[1] != "second" {
		t.Fatalf("interceptor order = %v, want [first second]", order)
	}
	if atomic.LoadInt32(&served) != 0 {
		t.Fatal("server should not have been reached after a short-circuit")
	}
}
