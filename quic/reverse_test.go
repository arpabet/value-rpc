/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valuequic_test

import (
	"context"
	"testing"
	"time"

	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valuerpc"
	"go.arpabet.com/value-rpc/valueserver"
)

// TestQUIC_ReverseUnary exercises server->client (reverse) RPC over QUIC: a
// server handler calls a function the client registered. This needs the server
// to open a stream and the client to accept it.
func TestQUIC_ReverseUnary(t *testing.T) {
	srv, cli := serverAuth(t, func(s valueserver.Server) {
		s.AddFunction("callBack", valuerpc.Any, valuerpc.Any,
			func(ctx context.Context, _ value.Value) (value.Value, error) {
				peer, ok := valueserver.PeerFromContext(ctx)
				if !ok {
					return nil, valuerpc.NewError(valuerpc.CodeInternal, "no peer")
				}
				return peer.CallFunction(ctx, "clientFn", value.Tuple(value.Utf8("ping")))
			})
	})
	defer srv.Close()
	cli.AddFunction("clientFn", valuerpc.Any, valuerpc.Any,
		func(_ context.Context, args value.Value) (value.Value, error) {
			return value.Utf8("client-got:" + args.(value.List).GetStringAt(0).String()), nil
		})
	mustConnect(t, cli)
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	res, err := cli.CallFunction(ctx, "callBack", nil)
	if err != nil {
		t.Fatalf("reverse call over quic: %v", err)
	}
	if got := res.(value.String).String(); got != "client-got:ping" {
		t.Fatalf("reverse result = %q, want client-got:ping", got)
	}
}

// TestQUIC_ReverseGetStream: a server handler pulls a stream the client serves.
func TestQUIC_ReverseGetStream(t *testing.T) {
	srv, cli := serverAuth(t, func(s valueserver.Server) {
		s.AddFunction("pull", valuerpc.Any, valuerpc.Any,
			func(ctx context.Context, _ value.Value) (value.Value, error) {
				peer, ok := valueserver.PeerFromContext(ctx)
				if !ok {
					return nil, valuerpc.NewError(valuerpc.CodeInternal, "no peer")
				}
				ch, _, err := peer.GetStream(ctx, "feed", value.Tuple(value.Long(5)), 16)
				if err != nil {
					return nil, err
				}
				var sum int64
				for v := range ch {
					if v != nil && v.Kind() != value.NULL {
						sum += v.(value.Number).Long()
					}
				}
				return value.Long(sum), nil
			})
	})
	defer srv.Close()
	cli.AddOutgoingStream("feed", valuerpc.Any,
		func(_ context.Context, args value.Value) (<-chan value.Value, error) {
			n := args.(value.List).GetNumberAt(0).Long()
			out := make(chan value.Value)
			go func() {
				defer close(out)
				for i := int64(1); i <= n; i++ {
					out <- value.Long(i)
				}
			}()
			return out, nil
		})
	mustConnect(t, cli)
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	res, err := cli.CallFunction(ctx, "pull", nil)
	if err != nil {
		t.Fatalf("reverse get-stream over quic: %v", err)
	}
	if got := res.(value.Number).Long(); got != 15 {
		t.Fatalf("reverse stream sum = %d, want 15", got)
	}
}

// TestQUIC_ReverseChat: a server handler opens a bidirectional chat toward the
// client (the hardest FIN path — both sides send and end).
func TestQUIC_ReverseChat(t *testing.T) {
	srv, cli := serverAuth(t, func(s valueserver.Server) {
		s.AddFunction("startChat", valuerpc.Any, valuerpc.Any,
			func(ctx context.Context, _ value.Value) (value.Value, error) {
				peer, ok := valueserver.PeerFromContext(ctx)
				if !ok {
					return nil, valuerpc.NewError(valuerpc.CodeInternal, "no peer")
				}
				sendC := make(chan value.Value, 3)
				go func() {
					defer close(sendC)
					for _, s := range []string{"x", "y", "z"} {
						sendC <- value.Utf8(s)
					}
				}()
				recvC, _, err := peer.Chat(ctx, "clientChat", nil, 16, sendC)
				if err != nil {
					return nil, err
				}
				var got []string
				for v := range recvC {
					if v != nil && v.Kind() != value.NULL {
						got = append(got, v.(value.String).String())
					}
				}
				return value.Utf8(joinStrings(got)), nil
			})
	})
	defer srv.Close()
	cli.AddChat("clientChat", valuerpc.Any,
		func(_ context.Context, _ value.Value, inC <-chan value.Value) (<-chan value.Value, error) {
			out := make(chan value.Value)
			go func() {
				defer close(out)
				for v := range inC {
					out <- value.Utf8("C:" + v.(value.String).String())
				}
			}()
			return out, nil
		})
	mustConnect(t, cli)
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	res, err := cli.CallFunction(ctx, "startChat", nil)
	if err != nil {
		t.Fatalf("reverse chat over quic: %v", err)
	}
	if got := res.(value.String).String(); got != "C:x,C:y,C:z" {
		t.Fatalf("reverse chat result = %q, want C:x,C:y,C:z", got)
	}
}

func joinStrings(ss []string) string {
	out := ""
	for i, s := range ss {
		if i > 0 {
			out += ","
		}
		out += s
	}
	return out
}

// TestQUIC_LargePutStream uploads far more values than the initial flow-control
// window, so the server must send StreamCredit REPLENISHMENTS after StreamReady.
// If the consumer's write half is FIN'd early those grants are dropped and the
// producer stalls.
func TestQUIC_LargePutStream(t *testing.T) {
	old := valuerpc.DefaultMaxPending
	valuerpc.DefaultMaxPending = 16 // tiny window so replenishment is required
	defer func() { valuerpc.DefaultMaxPending = old }()

	const n = 500
	got := make(chan int64, 1)
	srv, cli := serverAuth(t, func(s valueserver.Server) {
		s.AddIncomingStream("upload", valuerpc.Any,
			func(_ context.Context, _ value.Value, inC <-chan value.Value) error {
				go func() {
					var count int64
					for range inC {
						count++
					}
					got <- count
				}()
				return nil
			})
	})
	defer srv.Close()
	mustConnect(t, cli)
	defer cli.Close()
	cli.SetTimeout(8000)

	putC := make(chan value.Value)
	go func() {
		defer close(putC)
		for i := 0; i < n; i++ {
			putC <- value.Long(int64(i))
		}
	}()
	if err := cli.PutStream(context.Background(), "upload", nil, putC); err != nil {
		t.Fatalf("put stream: %v", err)
	}
	select {
	case count := <-got:
		if count != n {
			t.Fatalf("server received %d values, want %d", count, n)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("large put-stream stalled (credit replenishment dropped after early FIN)")
	}
}
