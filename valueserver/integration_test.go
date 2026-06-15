/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: Apache-2.0
 */

package valueserver_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valueclient"
	"go.arpabet.com/value-rpc/valuerpc"
	"go.arpabet.com/value-rpc/valueserver"
	"go.uber.org/zap"
)

// newServer starts a server bound to an ephemeral port and returns its actual
// address plus a cleanup func. Using Addr() avoids any reserve/rebind race.
func newServer(t testing.TB, setup func(s valueserver.Server)) (string, func()) {
	t.Helper()
	srv, err := valueserver.NewServer("127.0.0.1:0", zap.NewNop())
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	setup(srv)
	go srv.Run()
	return srv.Addr().String(), func() { srv.Close() }
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

// TestVoidArgsAccepted pins the BUG-2 fix: a function registered with
// valuerpc.Void can be called with nil args (which arrive as value.Null).
func TestVoidArgsAccepted(t *testing.T) {
	addr, stop := newServer(t, func(s valueserver.Server) {
		s.AddFunction("ping", valuerpc.Void, valuerpc.String,
			func(args value.Value) (value.Value, error) {
				return value.Utf8("pong"), nil
			})
	})
	defer stop()

	cli := dialClient(t, addr)
	defer cli.Close()

	res, err := cli.CallFunction("ping", nil)
	if err != nil {
		t.Fatalf("BUG-2: Void function with nil args should be accepted, got: %v", err)
	}
	if got := res.(value.String).String(); got != "pong" {
		t.Fatalf("result = %q, want %q", got, "pong")
	}
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
	for v := range readC {
		// BUG-4 fix: no phantom value.Null may be delivered on the stream.
		if v == nil || v.Kind() == value.NULL {
			t.Fatalf("BUG-4: phantom Null delivered on stream")
		}
		got = append(got, v.(value.Number).Long())
	}
	if len(got) != 5 {
		t.Fatalf("received %d numeric values %v, want 5", len(got), got)
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

// TestChatBidirectional exercises the full bidirectional path end-to-end. It
// also covers the chat half-close fix: the client closes its send side and must
// still receive every echo the server produced (no dropped messages, no phantom
// Null, no double-close panic).
func TestChatBidirectional(t *testing.T) {
	addr, stop := newServer(t, func(s valueserver.Server) {
		s.AddChat("echo", valuerpc.Any,
			func(args value.Value, inC <-chan value.Value) (<-chan value.Value, error) {
				outC := make(chan value.Value, 8)
				go func() {
					defer close(outC)
					for v := range inC {
						if v == nil || v.Kind() == value.NULL {
							continue
						}
						outC <- value.Utf8("echo:" + v.(value.String).String())
					}
				}()
				return outC, nil
			})
	})
	defer stop()

	cli := dialClient(t, addr)
	defer cli.Close()
	cli.SetTimeout(5000)

	sendC := make(chan value.Value, 4)
	readC, _, err := cli.Chat("echo", nil, 16, sendC)
	if err != nil {
		t.Fatalf("chat: %v", err)
	}

	inputs := []string{"a", "bb", "ccc", "dddd"}
	go func() {
		for _, s := range inputs {
			sendC <- value.Utf8(s)
		}
		close(sendC) // half-close send; must still receive all echoes
	}()

	var got []string
	for v := range readC {
		if v == nil || v.Kind() == value.NULL {
			t.Fatalf("BUG-4: phantom Null in chat stream")
		}
		got = append(got, v.(value.String).String())
	}

	if len(got) != len(inputs) {
		t.Fatalf("received %d echoes %v, want %d", len(got), got, len(inputs))
	}
	for i, s := range inputs {
		if want := "echo:" + s; got[i] != want {
			t.Fatalf("echo[%d] = %q, want %q", i, got[i], want)
		}
	}
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

// TestServerSurvivesHandlerPanic verifies the recover added to the request
// goroutine (BUG-3 family): a panicking handler must not crash the server, and
// subsequent calls must still succeed.
func TestServerSurvivesHandlerPanic(t *testing.T) {
	addr, stop := newServer(t, func(s valueserver.Server) {
		s.AddFunction("boom", valuerpc.Any, valuerpc.Any,
			func(args value.Value) (value.Value, error) {
				panic("handler exploded")
			})
		s.AddFunction("ok", valuerpc.Any, valuerpc.String,
			func(args value.Value) (value.Value, error) {
				return value.Utf8("alive"), nil
			})
	})
	defer stop()

	cli := dialClient(t, addr)
	defer cli.Close()
	cli.SetTimeout(500)

	// The panicking call gets no response and times out — but must not crash
	// the server.
	_, _ = cli.CallFunction("boom", nil)

	cli.SetTimeout(3000)
	res, err := cli.CallFunction("ok", nil)
	if err != nil {
		t.Fatalf("server did not survive a handler panic: %v", err)
	}
	if got := res.(value.String).String(); got != "alive" {
		t.Fatalf("result = %q, want %q", got, "alive")
	}
}

// TestGracefulShutdown verifies Close() drains and returns promptly (BUG-14).
func TestGracefulShutdown(t *testing.T) {
	addr, stop := newServer(t, func(s valueserver.Server) {
		s.AddFunction("ping", valuerpc.Any, valuerpc.String,
			func(args value.Value) (value.Value, error) {
				return value.Utf8("pong"), nil
			})
	})

	cli := dialClient(t, addr)
	if _, err := cli.CallFunction("ping", nil); err != nil {
		t.Fatalf("call: %v", err)
	}
	cli.Close()

	done := make(chan struct{})
	go func() { stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close() did not return within 5s — graceful drain hung")
	}
}

func TestWrongArgTypeRejected(t *testing.T) {
	addr, stop := newServer(t, func(s valueserver.Server) {
		s.AddFunction("needsNumber", valuerpc.List(valuerpc.Number), valuerpc.Number,
			func(args value.Value) (value.Value, error) {
				return args.(value.List).GetNumberAt(0), nil
			})
	})
	defer stop()

	cli := dialClient(t, addr)
	defer cli.Close()

	if _, err := cli.CallFunction("needsNumber", value.Tuple(value.Utf8("not a number"))); err == nil {
		t.Fatal("expected arg verification to reject a wrong-typed argument")
	}
}

// TestLargeServerStream pushes enough values to overflow the receive buffer,
// exercising the throttle/backpressure path, and checks every value arrives in
// order with no loss and no phantom Null.
func TestLargeServerStream(t *testing.T) {
	const n = 10000
	addr, stop := newServer(t, func(s valueserver.Server) {
		s.AddOutgoingStream("range", valuerpc.Any,
			func(args value.Value) (<-chan value.Value, error) {
				out := make(chan value.Value)
				go func() {
					defer close(out)
					for i := 0; i < n; i++ {
						out <- value.Long(int64(i))
					}
				}()
				return out, nil
			})
	})
	defer stop()

	cli := dialClient(t, addr)
	defer cli.Close()
	cli.SetTimeout(5000)

	readC, _, err := cli.GetStream("range", nil, 256)
	if err != nil {
		t.Fatalf("get stream: %v", err)
	}
	count := 0
	for v := range readC {
		if v == nil || v.Kind() == value.NULL {
			t.Fatalf("BUG-4: phantom Null on large stream")
		}
		if got := v.(value.Number).Long(); got != int64(count) {
			t.Fatalf("value %d out of order: got %d", count, got)
		}
		count++
	}
	if count != n {
		t.Fatalf("received %d values, want %d", count, n)
	}
}

// TestMultipleConcurrentStreams opens several server streams over one client
// connection at once; each must receive exactly its own ordered values (correct
// requestId multiplexing under load).
func TestMultipleConcurrentStreams(t *testing.T) {
	addr, stop := newServer(t, func(s valueserver.Server) {
		s.AddOutgoingStream("seq", valuerpc.List(valuerpc.Number, valuerpc.Number),
			func(args value.Value) (<-chan value.Value, error) {
				l := args.(value.List)
				start := l.GetNumberAt(0).Long()
				cnt := l.GetNumberAt(1).Long()
				out := make(chan value.Value)
				go func() {
					defer close(out)
					for i := int64(0); i < cnt; i++ {
						out <- value.Long(start + i)
					}
				}()
				return out, nil
			})
	})
	defer stop()

	cli := dialClient(t, addr)
	defer cli.Close()
	cli.SetTimeout(5000)

	const streams = 8
	const per = 500
	var wg sync.WaitGroup
	errc := make(chan error, streams)

	for s := 0; s < streams; s++ {
		wg.Add(1)
		go func(base int64) {
			defer wg.Done()
			readC, _, err := cli.GetStream("seq", value.Tuple(value.Long(base*per), value.Long(per)), 64)
			if err != nil {
				errc <- fmt.Errorf("stream %d: %w", base, err)
				return
			}
			want := base * per
			for v := range readC {
				if v == nil || v.Kind() == value.NULL {
					continue
				}
				if got := v.(value.Number).Long(); got != want {
					errc <- fmt.Errorf("stream %d: got %d want %d (cross-talk?)", base, got, want)
					return
				}
				want++
			}
			if want != base*per+per {
				errc <- fmt.Errorf("stream %d incomplete: ended at %d", base, want)
			}
		}(int64(s))
	}

	wg.Wait()
	close(errc)
	for err := range errc {
		t.Error(err)
	}
}

// BenchmarkServerStreamRecv measures per-value server-streaming throughput: a
// single stream delivers b.N values over loopback.
func BenchmarkServerStreamRecv(b *testing.B) {
	addr, stop := newServer(b, func(s valueserver.Server) {
		s.AddOutgoingStream("range", valuerpc.List(valuerpc.Number),
			func(args value.Value) (<-chan value.Value, error) {
				n := args.(value.List).GetNumberAt(0).Long()
				out := make(chan value.Value, 256)
				go func() {
					defer close(out)
					for i := int64(0); i < n; i++ {
						out <- value.Long(i)
					}
				}()
				return out, nil
			})
	})
	defer stop()

	cli := dialClient(b, addr)
	defer cli.Close()
	cli.SetTimeout(60000)

	b.ReportAllocs()
	b.ResetTimer()
	readC, _, err := cli.GetStream("range", value.Tuple(value.Long(int64(b.N))), 256)
	if err != nil {
		b.Fatal(err)
	}
	got := 0
	for v := range readC {
		if v != nil && v.Kind() != value.NULL {
			got++
		}
	}
	if got != b.N {
		b.Fatalf("received %d values, want %d", got, b.N)
	}
}

// BenchmarkChatEcho measures bidirectional round-trip throughput: b.N messages
// are echoed back over a single chat.
func BenchmarkChatEcho(b *testing.B) {
	addr, stop := newServer(b, func(s valueserver.Server) {
		s.AddChat("echo", valuerpc.Any,
			func(args value.Value, inC <-chan value.Value) (<-chan value.Value, error) {
				out := make(chan value.Value, 64)
				go func() {
					defer close(out)
					for v := range inC {
						out <- v
					}
				}()
				return out, nil
			})
	})
	defer stop()

	cli := dialClient(b, addr)
	defer cli.Close()
	cli.SetTimeout(60000)

	sendC := make(chan value.Value, 64)
	readC, _, err := cli.Chat("echo", nil, 64, sendC)
	if err != nil {
		b.Fatal(err)
	}

	msg := value.Utf8("ping")
	b.ReportAllocs()
	b.ResetTimer()
	go func() {
		for i := 0; i < b.N; i++ {
			sendC <- msg
		}
		close(sendC)
	}()
	got := 0
	for range readC {
		got++
	}
	if got != b.N {
		b.Fatalf("received %d echoes, want %d", got, b.N)
	}
}
