/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package resilience_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/resilience"
	"go.arpabet.com/value-rpc/valueclient"
	vrpc "go.arpabet.com/value-rpc/valuerpc"
	"go.arpabet.com/value-rpc/valueserver"
	"go.uber.org/zap"
)

// Example wires a governance chain into a client with WithInterceptors and shows
// Retry transparently recovering a transient failure. The chain is, outermost
// first: circuit breaker → retry → per-attempt timeout → the call.
func Example() {
	srv, err := valueserver.NewMemServer("resilience-example", zap.NewNop())
	if err != nil {
		panic(err)
	}
	defer srv.Close()

	var attempts int32
	srv.AddFunction("getQuote", vrpc.Any, vrpc.Any,
		func(_ context.Context, _ value.Value) (value.Value, error) {
			if atomic.AddInt32(&attempts, 1) < 3 {
				return nil, vrpc.NewError(vrpc.CodeUnavailable, "warming up") // transient
			}
			return value.Utf8("hello"), nil
		})
	go srv.Run()

	cli := valueclient.NewMemClient("resilience-example", valueclient.WithInterceptors(
		resilience.CircuitBreaker(),
		resilience.Retry(
			resilience.WithMaxAttempts(5),
			resilience.WithBackoff(time.Millisecond, 10*time.Millisecond),
		),
		resilience.Timeout(time.Second),
	))
	if err := cli.Connect(); err != nil {
		panic(err)
	}
	defer cli.Close()

	res, err := cli.CallFunction(context.Background(), "getQuote", nil)
	if err != nil {
		panic(err)
	}
	fmt.Println(res.(value.String).String())
	fmt.Println("server attempts:", atomic.LoadInt32(&attempts))
	// Output:
	// hello
	// server attempts: 3
}

// Example_fallback returns a default value when the call fails instead of
// surfacing the error to the caller.
func Example_fallback() {
	srv, err := valueserver.NewMemServer("resilience-fallback", zap.NewNop())
	if err != nil {
		panic(err)
	}
	defer srv.Close()
	srv.AddFunction("risky", vrpc.Any, vrpc.Any,
		func(_ context.Context, _ value.Value) (value.Value, error) {
			return nil, vrpc.NewError(vrpc.CodeInternal, "boom")
		})
	go srv.Run()

	cli := valueclient.NewMemClient("resilience-fallback", valueclient.WithInterceptors(
		resilience.Fallback(func(_ context.Context, _ string, _ value.Value, _ error) (value.Value, error) {
			return value.Utf8("cached"), nil
		}),
	))
	if err := cli.Connect(); err != nil {
		panic(err)
	}
	defer cli.Close()

	res, _ := cli.CallFunction(context.Background(), "risky", nil)
	fmt.Println(res.(value.String).String())
	// Output:
	// cached
}
