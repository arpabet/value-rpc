/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valueserver_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
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
			func(_ context.Context, args value.Value) (value.Value, error) {
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
			func(_ context.Context, args value.Value) (value.Value, error) {
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
			func(_ context.Context, args value.Value) (value.Value, error) {
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
			func(_ context.Context, args value.Value) (<-chan value.Value, error) {
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
			func(_ context.Context, args value.Value, inC <-chan value.Value) error {
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

// TestMaxConcurrentRequestsRejectsFlood verifies the per-connection handler cap:
// once MaxConcurrentRequests handlers are in flight, further requests are
// rejected with an error response (instead of spawning unbounded goroutines or
// stalling the connection), and the accepted ones still complete normally.
func TestMaxConcurrentRequestsRejectsFlood(t *testing.T) {
	const cap = 4
	old := valueserver.MaxConcurrentRequests
	valueserver.MaxConcurrentRequests = cap
	defer func() { valueserver.MaxConcurrentRequests = old }()

	release := make(chan struct{})
	var releaseOnce sync.Once
	closeRelease := func() { releaseOnce.Do(func() { close(release) }) }
	var active, maxActive int64
	addr, stop := newServer(t, func(s valueserver.Server) {
		s.AddFunction("hold", valuerpc.Any, valuerpc.Any,
			func(_ context.Context, args value.Value) (value.Value, error) {
				n := atomic.AddInt64(&active, 1)
				for {
					m := atomic.LoadInt64(&maxActive)
					if n <= m || atomic.CompareAndSwapInt64(&maxActive, m, n) {
						break
					}
				}
				<-release // occupy the slot until the test releases
				atomic.AddInt64(&active, -1)
				return value.Utf8("ok"), nil
			})
	})
	defer stop()
	defer closeRelease()

	cli := dialClient(t, addr)
	defer cli.Close()
	cli.SetTimeout(5000)

	const n = 12
	var wg sync.WaitGroup
	var okCount, busyCount int64
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := cli.CallFunction("hold", nil); err != nil {
				atomic.AddInt64(&busyCount, 1)
			} else {
				atomic.AddInt64(&okCount, 1)
			}
		}()
	}

	// The over-cap requests are rejected promptly; wait for them.
	deadline := time.After(5 * time.Second)
	for atomic.LoadInt64(&busyCount) < n-cap {
		select {
		case <-deadline:
			t.Fatalf("only %d requests rejected, want %d", atomic.LoadInt64(&busyCount), n-cap)
		case <-time.After(10 * time.Millisecond):
		}
	}
	closeRelease() // let the accepted handlers finish
	wg.Wait()

	if okCount != cap {
		t.Errorf("accepted %d requests, want %d", okCount, cap)
	}
	if busyCount != n-cap {
		t.Errorf("rejected %d requests, want %d", busyCount, n-cap)
	}
	if maxActive > cap {
		t.Errorf("%d handlers ran concurrently, exceeds cap %d", maxActive, cap)
	}
}

// TestHandlerReceivesSLADeadline verifies the client's SLA is propagated to the
// handler as a context deadline.
func TestHandlerReceivesSLADeadline(t *testing.T) {
	hasDeadline := make(chan bool, 1)
	addr, stop := newServer(t, func(s valueserver.Server) {
		s.AddFunction("checkDeadline", valuerpc.Any, valuerpc.Any,
			func(ctx context.Context, args value.Value) (value.Value, error) {
				_, ok := ctx.Deadline()
				hasDeadline <- ok
				return value.Utf8("ok"), nil
			})
	})
	defer stop()

	cli := dialClient(t, addr)
	defer cli.Close()
	cli.SetTimeout(2000) // sent as the SLA on the request

	if _, err := cli.CallFunction("checkDeadline", nil); err != nil {
		t.Fatalf("call: %v", err)
	}
	if !<-hasDeadline {
		t.Fatal("handler context carried no deadline despite the client SLA")
	}
}

// TestUnaryHandlerContextCanceled verifies that cancelling an in-flight unary
// call (here via the client's timeout, which sends CancelRequest) cancels the
// handler's context — exercising the ctx-based unary cancellation that replaced
// the leak-prone canceledRequests set.
func TestUnaryHandlerContextCanceled(t *testing.T) {
	canceled := make(chan struct{})
	addr, stop := newServer(t, func(s valueserver.Server) {
		s.AddFunction("block", valuerpc.Any, valuerpc.Any,
			func(ctx context.Context, args value.Value) (value.Value, error) {
				<-ctx.Done() // block until the request is cancelled
				close(canceled)
				return nil, ctx.Err()
			})
	})
	defer stop()

	cli := dialClient(t, addr)
	defer cli.Close()
	cli.SetTimeout(200) // times out, which sends CancelRequest to the server

	if _, err := cli.CallFunction("block", nil); err == nil {
		t.Fatal("expected a timeout error from the blocked call")
	}
	select {
	case <-canceled:
	case <-time.After(3 * time.Second):
		t.Fatal("handler context was not cancelled after the client cancelled the request")
	}
}

// TestConcurrentUnaryCalls exercises the multiplexing/dispatch path: many
// goroutines share one client and one connection, and each must receive exactly
// its own response (correct requestId routing under load).
func TestConcurrentUnaryCalls(t *testing.T) {
	addr, stop := newServer(t, func(s valueserver.Server) {
		s.AddFunction("square", valuerpc.List(valuerpc.Number), valuerpc.Number,
			func(_ context.Context, args value.Value) (value.Value, error) {
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

// TestSlowStreamConsumerDoesNotBlockOthers is the regression test for BUG-6
// (head-of-line blocking). One client opens a server-stream and never reads it,
// while the server pushes values as fast as it can. The stream's buffer fills,
// but because the client's response loop delivers through a pump (non-blocking)
// instead of a blocking channel send, every other multiplexed request on the
// same connection must keep completing. Before the fix the full stream buffer
// froze the single response loop and these unary calls all timed out.
func TestSlowStreamConsumerDoesNotBlockOthers(t *testing.T) {
	stopProducing := make(chan struct{})
	addr, stop := newServer(t, func(s valueserver.Server) {
		s.AddFunction("square", valuerpc.List(valuerpc.Number), valuerpc.Number,
			func(_ context.Context, args value.Value) (value.Value, error) {
				n := args.(value.List).GetNumberAt(0).Long()
				return value.Long(n * n), nil
			})
		// Emits continuously until the test ends or the request is cancelled.
		s.AddOutgoingStream("firehose", valuerpc.Any,
			func(_ context.Context, args value.Value) (<-chan value.Value, error) {
				outC := make(chan value.Value)
				go func() {
					defer close(outC)
					for i := int64(0); ; i++ {
						select {
						case outC <- value.Long(i):
						case <-stopProducing:
							return
						}
					}
				}()
				return outC, nil
			})
	})
	defer stop()
	defer close(stopProducing)

	cli := dialClient(t, addr)
	defer cli.Close()
	cli.SetTimeout(5000)

	// Open the firehose and deliberately never drain it: a stuck slow consumer.
	stream, _, err := cli.GetStream("firehose", nil, 4)
	if err != nil {
		t.Fatalf("get stream: %v", err)
	}
	_ = stream // intentionally not read

	// Give the server a moment to saturate the stream's buffer + pump.
	time.Sleep(200 * time.Millisecond)

	// Unary calls on the SAME connection must still complete promptly.
	done := make(chan error, 1)
	go func() {
		for i := int64(1); i <= 200; i++ {
			res, err := cli.CallFunction("square", value.Tuple(value.Long(i)))
			if err != nil {
				done <- fmt.Errorf("square(%d): %w", i, err)
				return
			}
			if got := res.(value.Number).Long(); got != i*i {
				done <- fmt.Errorf("square(%d) = %d, want %d", i, got, i*i)
				return
			}
		}
		done <- nil
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("unary calls blocked behind a slow stream consumer (BUG-6 head-of-line blocking)")
	}
}

// TestChatBidirectional exercises the full bidirectional path end-to-end. It
// also covers the chat half-close fix: the client closes its send side and must
// still receive every echo the server produced (no dropped messages, no phantom
// Null, no double-close panic).
func TestChatBidirectional(t *testing.T) {
	addr, stop := newServer(t, func(s valueserver.Server) {
		s.AddChat("echo", valuerpc.Any,
			func(_ context.Context, args value.Value, inC <-chan value.Value) (<-chan value.Value, error) {
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
			func(_ context.Context, args value.Value) (value.Value, error) {
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
			func(_ context.Context, args value.Value) (value.Value, error) {
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
			func(_ context.Context, args value.Value) (value.Value, error) {
				panic("handler exploded")
			})
		s.AddFunction("ok", valuerpc.Any, valuerpc.String,
			func(_ context.Context, args value.Value) (value.Value, error) {
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
			func(_ context.Context, args value.Value) (value.Value, error) {
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
			func(_ context.Context, args value.Value) (value.Value, error) {
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
			func(_ context.Context, args value.Value) (<-chan value.Value, error) {
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
			func(_ context.Context, args value.Value) (<-chan value.Value, error) {
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
			func(_ context.Context, args value.Value) (<-chan value.Value, error) {
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
			func(_ context.Context, args value.Value, inC <-chan value.Value) (<-chan value.Value, error) {
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
