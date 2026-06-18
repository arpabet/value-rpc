/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valuerpc_test

import (
	"context"
	"testing"

	"go.arpabet.com/value"
	vrpc "go.arpabet.com/value-rpc/valuerpc"
)

func TestChainClientInterceptors(t *testing.T) {
	var trace []string
	final := func(_ context.Context, _ string, _ value.Value) (value.Value, error) {
		trace = append(trace, "final")
		return value.Utf8("done"), nil
	}
	mk := func(name string) vrpc.ClientInterceptor {
		return func(ctx context.Context, method string, req value.Value, next vrpc.Invoker) (value.Value, error) {
			trace = append(trace, name+":in")
			res, err := next(ctx, method, req)
			trace = append(trace, name+":out")
			return res, err
		}
	}

	inv := vrpc.ChainClientInterceptors(final, mk("a"), mk("b"), mk("c"))
	res, err := inv(context.Background(), "m", nil)
	if err != nil || res.(value.String).String() != "done" {
		t.Fatalf("res=%v err=%v", res, err)
	}
	// a is outermost: a:in, b:in, c:in, final, c:out, b:out, a:out
	want := []string{"a:in", "b:in", "c:in", "final", "c:out", "b:out", "a:out"}
	if len(trace) != len(want) {
		t.Fatalf("trace = %v, want %v", trace, want)
	}
	for i := range want {
		if trace[i] != want[i] {
			t.Fatalf("trace[%d] = %q, want %q (full: %v)", i, trace[i], want[i], trace)
		}
	}
}

func TestChainClientInterceptorsEmpty(t *testing.T) {
	called := false
	final := func(context.Context, string, value.Value) (value.Value, error) {
		called = true
		return nil, nil
	}
	inv := vrpc.ChainClientInterceptors(final)
	if _, err := inv(context.Background(), "m", nil); err != nil {
		t.Fatalf("err = %v", err)
	}
	if !called {
		t.Fatal("empty chain must return the final invoker unchanged")
	}
}
