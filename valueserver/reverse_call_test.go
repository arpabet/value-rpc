/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valueserver_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valueclient"
	"go.arpabet.com/value-rpc/valuerpc"
	"go.arpabet.com/value-rpc/valueserver"
)

// TestReverseUnaryCall: a server handler calls a function the client registered
// (server->client reverse RPC) and returns the result to the original caller.
// This exercises the full bidirectional spine — a client-initiated call and a
// server-initiated call multiplexed on one connection, with the request-id
// spaces (client positive, server negative) kept distinct.
func TestReverseUnaryCall(t *testing.T) {
	addr, stop := newServer(t, func(s valueserver.Server) {
		// "reverse" calls back into the client that invoked it and relays the result.
		s.AddFunction("reverse", valuerpc.Any, valuerpc.Any,
			func(ctx context.Context, args value.Value) (value.Value, error) {
				caller, ok := valueserver.ClientFromContext(ctx)
				if !ok {
					return nil, valuerpc.NewError(valuerpc.CodeInternal, "no client in context")
				}
				return caller.CallFunction(ctx, "clientEcho", args)
			})
	})
	defer stop()

	cli := valueclient.NewClient(addr, "")
	// Register before Connect so the handler is callable as soon as we connect.
	cli.AddFunction("clientEcho", valuerpc.Any, valuerpc.Any,
		func(_ context.Context, args value.Value) (value.Value, error) {
			return value.Utf8("client:" + args.(value.String).String()), nil
		})
	if err := cli.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer cli.Close()

	res, err := cli.CallFunction(context.Background(), "reverse", value.Utf8("hi"))
	if err != nil {
		t.Fatalf("reverse call: %v", err)
	}
	if got := res.(value.String).String(); got != "client:hi" {
		t.Fatalf("got %q, want %q", got, "client:hi")
	}
}

// TestReverseUnaryError: an error returned by the client handler propagates back
// through the server's CallFunction to the original caller with its Code intact.
func TestReverseUnaryError(t *testing.T) {
	addr, stop := newServer(t, func(s valueserver.Server) {
		s.AddFunction("reverseFail", valuerpc.Any, valuerpc.Any,
			func(ctx context.Context, args value.Value) (value.Value, error) {
				caller, ok := valueserver.ClientFromContext(ctx)
				if !ok {
					return nil, valuerpc.NewError(valuerpc.CodeInternal, "no client in context")
				}
				return caller.CallFunction(ctx, "clientFail", args)
			})
	})
	defer stop()

	cli := valueclient.NewClient(addr, "")
	cli.AddFunction("clientFail", valuerpc.Any, valuerpc.Any,
		func(_ context.Context, _ value.Value) (value.Value, error) {
			return nil, valuerpc.NewError(valuerpc.CodeInvalidArgument, "client says no")
		})
	if err := cli.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer cli.Close()

	_, err := cli.CallFunction(context.Background(), "reverseFail", value.Utf8("x"))
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if code := valuerpc.CodeOf(err); code != valuerpc.CodeInvalidArgument {
		t.Fatalf("got code %v, want CodeInvalidArgument", code)
	}
}

// TestReverseUnaryConcurrent stresses many concurrent client-initiated calls,
// each of which triggers a server-initiated call back on the same connection,
// to confirm the bidirectional request-id allocation never collides.
func TestReverseUnaryConcurrent(t *testing.T) {
	addr, stop := newServer(t, func(s valueserver.Server) {
		s.AddFunction("reverse", valuerpc.Any, valuerpc.Any,
			func(ctx context.Context, args value.Value) (value.Value, error) {
				caller, ok := valueserver.ClientFromContext(ctx)
				if !ok {
					return nil, valuerpc.NewError(valuerpc.CodeInternal, "no client in context")
				}
				return caller.CallFunction(ctx, "clientEcho", args)
			})
	})
	defer stop()

	cli := valueclient.NewClient(addr, "")
	cli.AddFunction("clientEcho", valuerpc.Any, valuerpc.Any,
		func(_ context.Context, args value.Value) (value.Value, error) {
			return value.Utf8("c:" + args.(value.String).String()), nil
		})
	if err := cli.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer cli.Close()

	const n = 50
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			in := fmt.Sprintf("%d", i)
			res, err := cli.CallFunction(context.Background(), "reverse", value.Utf8(in))
			if err != nil {
				errs <- err
				return
			}
			if got := res.(value.String).String(); got != "c:"+in {
				errs <- fmt.Errorf("got %q, want c:%s", got, in)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}
