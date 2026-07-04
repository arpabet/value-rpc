/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valueserver_test

import (
	"context"
	"testing"

	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valueclient"
	"go.arpabet.com/value-rpc/valuerpc"
	"go.arpabet.com/value-rpc/valueserver"
)

// TestReversePutStreamWaitUsesRequestCtx verifies the R3 fix: a handler can open
// a reverse push bound to its OWN request context and await completion with
// PutStreamWait, so the push fully drains before the handler returns — the
// pattern that silently truncated with plain PutStream (the handler returning
// cancelled the request ctx mid-push).
func TestReversePutStreamWaitUsesRequestCtx(t *testing.T) {
	const n = 200
	addr, stop := newServer(t, func(s valueserver.Server) {
		s.AddFunction("pushAll", valuerpc.Any, valuerpc.Any,
			func(ctx context.Context, _ value.Value) (value.Value, error) {
				peer, ok := valueserver.PeerFromContext(ctx)
				if !ok {
					return nil, valuerpc.NewError(valuerpc.CodeInternal, "no client")
				}
				putC := make(chan value.Value)
				go func() {
					defer close(putC)
					for i := int64(1); i <= n; i++ {
						putC <- value.Long(i)
					}
				}()
				// Bind the push to the REQUEST ctx (previously a footgun) and
				// await completion — the stream must fully drain here.
				done, err := peer.PutStreamWait(ctx, "sink", nil, putC)
				if err != nil {
					return nil, err
				}
				if err := <-done; err != nil {
					return nil, err
				}
				return value.Utf8("drained"), nil
			})
	})
	defer stop()

	got := make(chan int64, 1)
	cli := valueclient.NewClient(addr, "")
	cli.AddIncomingStream("sink", valuerpc.Any,
		func(_ context.Context, _ value.Value, inC <-chan value.Value) error {
			go func() {
				var s int64
				for v := range inC {
					s += v.(value.Number).Long()
				}
				got <- s
			}()
			return nil
		})
	if err := cli.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer cli.Close()

	res, err := cli.CallFunction(context.Background(), "pushAll", nil)
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if res.(value.String).String() != "drained" {
		t.Fatalf("handler result = %q, want drained", res)
	}
	if s, want := <-got, int64(n*(n+1)/2); s != want {
		t.Fatalf("client summed %d, want %d (push was truncated)", s, want)
	}
}
