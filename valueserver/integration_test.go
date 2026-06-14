/*
 * Copyright (c) 2025 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valueserver_test

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valueclient"
	"go.arpabet.com/value-rpc/valuerpc"
	"go.arpabet.com/value-rpc/valueserver"
	"go.uber.org/zap"
)

// freeAddr returns a loopback address that is free at call time. There is a
// tiny race between releasing it and the server re-binding, which is acceptable
// for tests.
func freeAddr(t testing.TB) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	defer l.Close()
	return l.Addr().String()
}

// newServer starts a server with the given setup and returns its address plus a
// cleanup func.
func newServer(t testing.TB, setup func(s valueserver.Server)) (string, func()) {
	t.Helper()
	addr := freeAddr(t)
	srv, err := valueserver.NewServer(addr, zap.NewNop())
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	setup(srv)
	go srv.Run()
	// give the listener a moment to start accepting
	time.Sleep(50 * time.Millisecond)
	return addr, func() { srv.Close() }
}

func dialClient(t testing.TB, addr string) valueclient.Client {
	t.Helper()
	cli := valueclient.NewClient(addr, "")
	if err := cli.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	return cli
}

func TestUnaryCall(t *testing.T) {
	addr, stop := newServer(t, func(s valueserver.Server) {
		s.AddFunction("echo", valuerpc.List(valuerpc.String), valuerpc.String,
			func(args value.Value) (value.Value, error) {
				in := args.(value.List).GetStringAt(0).String()
				return value.Utf8("echo:" + in), nil
			})
	})
	defer stop()

	cli := dialClient(t, addr)
	defer cli.Close()

	res, err := cli.CallFunction("echo", value.Tuple(value.Utf8("hi")))
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if got := res.(value.String).String(); got != "echo:hi" {
		t.Fatalf("result = %q, want %q", got, "echo:hi")
	}
}

func TestUnaryCall_ServerError(t *testing.T) {
	addr, stop := newServer(t, func(s valueserver.Server) {
		// NOTE: args are valuerpc.Any, NOT Void. A Void function called with nil
		// args is rejected before it ever runs (see TestVoidArgsRejected /
		// FINDINGS.md BUG-2), which would make this test pass for the wrong reason.
		s.AddFunction("boom", valuerpc.Any, valuerpc.String,
			func(args value.Value) (value.Value, error) {
				return nil, fmt.Errorf("kaboom")
			})
	})
	defer stop()

	cli := dialClient(t, addr)
	defer cli.Close()

	_, err := cli.CallFunction("boom", nil)
	if err == nil {
		t.Fatal("expected an error from a failing server function")
	}
}

// TestVoidArgsRejected documents BUG-2: a function registered with valuerpc.Void
// cannot be called with nil args. The client serializes nil into the args field,
// it arrives as value.Null, and Verify(Null, Void) is false (Verify only special-
// cases Go nil and empty collections). This breaks the shipped example program at
// its first Void call. Workarounds: register with valuerpc.Any, or pass an empty
// list as args.
func TestVoidArgsRejected(t *testing.T) {
	addr, stop := newServer(t, func(s valueserver.Server) {
		s.AddFunction("ping", valuerpc.Void, valuerpc.String,
			func(args value.Value) (value.Value, error) {
				return value.Utf8("pong"), nil
			})
	})
	defer stop()

	cli := dialClient(t, addr)
	defer cli.Close()

	_, err := cli.CallFunction("ping", nil)
	if err == nil {
		t.Skip("Void + nil args now accepted — BUG-2 appears FIXED; update this test")
	}
	t.Logf("BUG-2 confirmed: Void function called with nil args rejected: %v", err)
}

func TestUnaryCall_UnknownFunction(t *testing.T) {
	addr, stop := newServer(t, func(s valueserver.Server) {})
	defer stop()

	cli := dialClient(t, addr)
	defer cli.Close()

	if _, err := cli.CallFunction("nope", nil); err == nil {
		t.Fatal("expected an error calling an unregistered function")
	}
}

func TestServerStreaming(t *testing.T) {
	addr, stop := newServer(t, func(s valueserver.Server) {
		s.AddOutgoingStream("count", valuerpc.List(valuerpc.Number),
			func(args value.Value) (<-chan value.Value, error) {
				n := int(args.(value.List).GetNumberAt(0).Long())
				out := make(chan value.Value, n)
				go func() {
					for i := 0; i < n; i++ {
						out <- value.Long(int64(i))
					}
					close(out)
				}()
				return out, nil
			})
	})
	defer stop()

	cli := dialClient(t, addr)
	defer cli.Close()

	readC, _, err := cli.GetStream("count", value.Tuple(value.Long(5)), 16)
	if err != nil {
		t.Fatalf("get stream: %v", err)
	}

	var got []int64
	var phantom int
	for v := range readC {
		// BUG (see FINDINGS.md): the client pushes a phantom value.Null on
		// StreamEnd because processResponse compares Get(ValueField) to Go nil
		// instead of value.Null. Tolerate it here and count it.
		if v == nil || v.Kind() == value.NULL {
			phantom++
			continue
		}
		got = append(got, v.(value.Number).Long())
	}
	if len(got) != 5 {
		t.Fatalf("received %d numeric values %v, want 5", len(got), got)
	}
	if phantom > 0 {
		t.Logf("BUG observed: %d phantom Null value(s) delivered on the stream (StreamEnd handler)", phantom)
	}
}

func TestClientStreaming(t *testing.T) {
	var (
		mu    sync.Mutex
		sum   int64
		doneC = make(chan struct{})
	)
	addr, stop := newServer(t, func(s valueserver.Server) {
		// valuerpc.Any (not Void) so the nil-args call passes verification; see BUG-2.
		s.AddIncomingStream("sum", valuerpc.Any,
			func(args value.Value, inC <-chan value.Value) error {
				go func() {
					for v := range inC {
						if v != nil {
							mu.Lock()
							sum += v.(value.Number).Long()
							mu.Unlock()
						}
					}
					close(doneC)
				}()
				return nil
			})
	})
	defer stop()

	cli := dialClient(t, addr)
	defer cli.Close()

	putC := make(chan value.Value, 4)
	if err := cli.PutStream("sum", nil, putC); err != nil {
		t.Fatalf("put stream: %v", err)
	}
	for i := int64(1); i <= 4; i++ {
		putC <- value.Long(i)
	}
	close(putC)

	select {
	case <-doneC:
	case <-time.After(3 * time.Second):
		t.Fatal("server never observed end of client stream")
	}

	mu.Lock()
	defer mu.Unlock()
	if sum != 10 {
		t.Fatalf("server summed %d, want 10 (1+2+3+4)", sum)
	}
}

// TestConcurrentUnaryCalls exercises the multiplexing/dispatch path: many
// goroutines share one client and one connection, and each must receive exactly
// its own response (correct requestId routing under load).
func TestConcurrentUnaryCalls(t *testing.T) {
	addr, stop := newServer(t, func(s valueserver.Server) {
		s.AddFunction("square", valuerpc.List(valuerpc.Number), valuerpc.Number,
			func(args value.Value) (value.Value, error) {
				n := args.(value.List).GetNumberAt(0).Long()
				return value.Long(n * n), nil
			})
	})
	defer stop()

	cli := dialClient(t, addr)
	defer cli.Close()
	cli.SetTimeout(5000)

	const workers = 64
	const callsPer = 50
	var wg sync.WaitGroup
	errCh := make(chan error, workers*callsPer)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for i := 0; i < callsPer; i++ {
				n := int64(base*callsPer + i)
				res, err := cli.CallFunction("square", value.Tuple(value.Long(n)))
				if err != nil {
					errCh <- fmt.Errorf("n=%d: %w", n, err)
					return
				}
				if got := res.(value.Number).Long(); got != n*n {
					errCh <- fmt.Errorf("square(%d) = %d, want %d (response routed to wrong caller?)", n, got, n*n)
					return
				}
			}
		}(w)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}

// TestChatBidirectional is SKIPPED: the Chat shutdown path double-closes the
// response channel and panics the process (see FINDINGS.md BUG-7 and
// valueclient.TestRequestCtx_GetThenPutClose_DoubleClose). Enabling this test
// crashes `go test` for the whole package, so it is guarded until the bug is
// fixed.
func TestChatBidirectional(t *testing.T) {
	t.Skip("BUG-7: Chat double-closes resultCh and panics the process; see FINDINGS.md")
}

func BenchmarkUnaryCallLoopback(b *testing.B) {
	addr, stop := newServer(b, func(s valueserver.Server) {
		s.AddFunction("noop", valuerpc.List(valuerpc.Number), valuerpc.Number,
			func(args value.Value) (value.Value, error) {
				return args.(value.List).GetNumberAt(0), nil
			})
	})
	defer stop()

	cli := dialClient(b, addr)
	defer cli.Close()
	cli.SetTimeout(5000)

	arg := value.Tuple(value.Long(1))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := cli.CallFunction("noop", arg); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUnaryCallParallel(b *testing.B) {
	addr, stop := newServer(b, func(s valueserver.Server) {
		s.AddFunction("noop", valuerpc.List(valuerpc.Number), valuerpc.Number,
			func(args value.Value) (value.Value, error) {
				return args.(value.List).GetNumberAt(0), nil
			})
	})
	defer stop()

	cli := dialClient(b, addr)
	defer cli.Close()
	cli.SetTimeout(5000)

	arg := value.Tuple(value.Long(1))
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := cli.CallFunction("noop", arg); err != nil {
				b.Fatal(err)
			}
		}
	})
}
