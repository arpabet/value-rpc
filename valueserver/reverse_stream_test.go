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

// TestReverseGetStream: a server handler opens a GetStream *toward the client*
// (the client serves it with AddOutgoingStream) and receives every value.
func TestReverseGetStream(t *testing.T) {
	const n = 100
	addr, stop := newServer(t, func(s valueserver.Server) {
		s.AddFunction("sumFromClient", valuerpc.Any, valuerpc.Any,
			func(ctx context.Context, _ value.Value) (value.Value, error) {
				caller, ok := valueserver.PeerFromContext(ctx)
				if !ok {
					return nil, valuerpc.NewError(valuerpc.CodeInternal, "no client")
				}
				ch, _, err := caller.GetStream(ctx, "count", value.Long(n), 16)
				if err != nil {
					return nil, err
				}
				var sum int64
				for v := range ch {
					sum += v.(value.Number).Long()
				}
				return value.Long(sum), nil
			})
	})
	defer stop()

	cli := valueclient.NewClient(addr, "")
	cli.AddOutgoingStream("count", valuerpc.Any,
		func(_ context.Context, args value.Value) (<-chan value.Value, error) {
			count := args.(value.Number).Long()
			out := make(chan value.Value)
			go func() {
				defer close(out)
				for i := int64(1); i <= count; i++ {
					out <- value.Long(i)
				}
			}()
			return out, nil
		})
	if err := cli.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer cli.Close()

	res, err := cli.CallFunction(context.Background(), "sumFromClient", nil)
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if got, want := res.(value.Number).Long(), int64(n*(n+1)/2); got != want {
		t.Fatalf("sum = %d, want %d", got, want)
	}
}

// TestReversePutStream: a server handler opens a PutStream *toward the client*
// (the client serves it with AddIncomingStream) and streams values to it.
func TestReversePutStream(t *testing.T) {
	const n = 100
	sums := make(chan int64, 1)
	addr, stop := newServer(t, func(s valueserver.Server) {
		s.AddFunction("pushToClient", valuerpc.Any, valuerpc.Any,
			func(ctx context.Context, _ value.Value) (value.Value, error) {
				caller, ok := valueserver.PeerFromContext(ctx)
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
				// A background push outlives this unary handler, so bind it to the
				// connection lifetime (Background), not the request ctx — otherwise
				// the handler returning would cancel and truncate the stream.
				if err := caller.PutStream(context.Background(), "sink", nil, putC); err != nil {
					return nil, err
				}
				return value.Utf8("ok"), nil
			})
	})
	defer stop()

	cli := valueclient.NewClient(addr, "")
	cli.AddIncomingStream("sink", valuerpc.Any,
		func(_ context.Context, _ value.Value, inC <-chan value.Value) error {
			go func() {
				var s int64
				for v := range inC {
					s += v.(value.Number).Long()
				}
				sums <- s
			}()
			return nil
		})
	if err := cli.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer cli.Close()

	if _, err := cli.CallFunction(context.Background(), "pushToClient", nil); err != nil {
		t.Fatalf("call: %v", err)
	}
	if got, want := <-sums, int64(n*(n+1)/2); got != want {
		t.Fatalf("client summed %d, want %d", got, want)
	}
}

// TestReverseChat: a server handler opens a Chat *toward the client* (the client
// serves it with AddChat). The server streams values; the client doubles each and
// streams them back; the server sums the echoes. Exercises bidirectional flow +
// half-close in the reverse direction.
func TestReverseChat(t *testing.T) {
	const n = 50
	addr, stop := newServer(t, func(s valueserver.Server) {
		s.AddFunction("chatWithClient", valuerpc.Any, valuerpc.Any,
			func(ctx context.Context, _ value.Value) (value.Value, error) {
				caller, ok := valueserver.PeerFromContext(ctx)
				if !ok {
					return nil, valuerpc.NewError(valuerpc.CodeInternal, "no client")
				}
				out := make(chan value.Value)
				go func() {
					defer close(out)
					for i := int64(1); i <= n; i++ {
						out <- value.Long(i)
					}
				}()
				inC, _, err := caller.Chat(ctx, "double", nil, 16, out)
				if err != nil {
					return nil, err
				}
				var sum int64
				for v := range inC {
					sum += v.(value.Number).Long()
				}
				return value.Long(sum), nil
			})
	})
	defer stop()

	cli := valueclient.NewClient(addr, "")
	cli.AddChat("double", valuerpc.Any,
		func(_ context.Context, _ value.Value, inC <-chan value.Value) (<-chan value.Value, error) {
			out := make(chan value.Value)
			go func() {
				defer close(out)
				for v := range inC {
					out <- value.Long(v.(value.Number).Long() * 2)
				}
			}()
			return out, nil
		})
	if err := cli.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer cli.Close()

	res, err := cli.CallFunction(context.Background(), "chatWithClient", nil)
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if got, want := res.(value.Number).Long(), int64(2*(n*(n+1)/2)); got != want {
		t.Fatalf("chat sum = %d, want %d", got, want)
	}
}
