/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valueserver_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valueclient"
	"go.arpabet.com/value-rpc/valuerpc"
	"go.arpabet.com/value-rpc/valueserver"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
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

	res, err := cli.CallFunction(context.Background(), "echo", value.Tuple(value.Utf8("hi")))
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

	_, err := cli.CallFunction(context.Background(), "boom", nil)
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

	res, err := cli.CallFunction(context.Background(), "ping", nil)
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

	if _, err := cli.CallFunction(context.Background(), "nope", nil); err == nil {
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

	readC, _, err := cli.GetStream(context.Background(), "count", value.Tuple(value.Long(5)), 16)
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
	if err := cli.PutStream(context.Background(), "sum", nil, putC); err != nil {
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
			if _, err := cli.CallFunction(context.Background(), "hold", nil); err != nil {
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

// TestMaxConcurrentStreamsRejectsExcess verifies that streams beyond
// MaxConcurrentStreams are rejected, and that closing streams frees slots.
func TestMaxConcurrentStreamsRejectsExcess(t *testing.T) {
	const cap = 3
	old := valueserver.MaxConcurrentStreams
	valueserver.MaxConcurrentStreams = cap
	defer func() { valueserver.MaxConcurrentStreams = old }()

	release := make(chan struct{})
	var releaseOnce sync.Once
	closeRelease := func() { releaseOnce.Do(func() { close(release) }) }
	addr, stop := newServer(t, func(s valueserver.Server) {
		s.AddOutgoingStream("hold", valuerpc.Any,
			func(ctx context.Context, args value.Value) (<-chan value.Value, error) {
				out := make(chan value.Value)
				go func() {
					defer close(out)
					select {
					case <-release: // held open until the test releases
					case <-ctx.Done():
					}
				}()
				return out, nil
			})
	})
	defer stop()
	defer closeRelease()

	cli := dialClient(t, addr)
	defer cli.Close()
	cli.SetTimeout(5000)

	// Open exactly cap streams; each stays open (its handler blocks).
	for i := 0; i < cap; i++ {
		if _, _, err := cli.GetStream(context.Background(), "hold", nil, 1); err != nil {
			t.Fatalf("stream %d should open under the cap: %v", i, err)
		}
	}
	// One more must be rejected.
	if _, _, err := cli.GetStream(context.Background(), "hold", nil, 1); err == nil {
		t.Fatal("a stream beyond MaxConcurrentStreams should be rejected")
	}

	// Releasing the held streams frees their slots; a new stream then opens.
	closeRelease()
	deadline := time.After(5 * time.Second)
	for {
		_, _, err := cli.GetStream(context.Background(), "hold", nil, 1)
		if err == nil {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("stream slot not freed after streams closed: %v", err)
		case <-time.After(20 * time.Millisecond):
		}
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

	if _, err := cli.CallFunction(context.Background(), "checkDeadline", nil); err != nil {
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

	if _, err := cli.CallFunction(context.Background(), "block", nil); err == nil {
		t.Fatal("expected a timeout error from the blocked call")
	}
	select {
	case <-canceled:
	case <-time.After(3 * time.Second):
		t.Fatal("handler context was not cancelled after the client cancelled the request")
	}
}

// TestClientContextCancelUnary: cancelling the caller's context returns
// context.Canceled (even though the client timeout is long).
func TestClientContextCancelUnary(t *testing.T) {
	addr, stop := newServer(t, func(s valueserver.Server) {
		s.AddFunction("block", valuerpc.Any, valuerpc.Any,
			func(ctx context.Context, args value.Value) (value.Value, error) {
				<-ctx.Done()
				return nil, ctx.Err()
			})
	})
	defer stop()

	cli := dialClient(t, addr)
	defer cli.Close()
	cli.SetTimeout(60000) // long, so the context (not the timer) ends the call

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := cli.CallFunction(ctx, "block", nil)
		errCh <- err
	}()
	time.Sleep(100 * time.Millisecond) // let the call reach the server
	cancel()

	select {
	case err := <-errCh:
		if err != context.Canceled {
			t.Fatalf("got %v, want context.Canceled", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("CallFunction did not return after context cancel")
	}
}

// TestClientContextDeadlineUnary: a context deadline shorter than SetTimeout
// returns context.DeadlineExceeded.
func TestClientContextDeadlineUnary(t *testing.T) {
	addr, stop := newServer(t, func(s valueserver.Server) {
		s.AddFunction("block", valuerpc.Any, valuerpc.Any,
			func(ctx context.Context, args value.Value) (value.Value, error) {
				<-ctx.Done()
				return nil, ctx.Err()
			})
	})
	defer stop()

	cli := dialClient(t, addr)
	defer cli.Close()
	cli.SetTimeout(60000)

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	// The deadline is also sent as the server SLA, so both sides expire at ~150ms:
	// the client's local ctx.Done usually wins (context.DeadlineExceeded), but the
	// server's deadline response can arrive first (CodeDeadlineExceeded). Accept
	// either — both confirm the deadline propagated and cancelled the call.
	_, err := cli.CallFunction(ctx, "block", nil)
	if !errors.Is(err, context.DeadlineExceeded) && valuerpc.CodeOf(err) != valuerpc.CodeDeadlineExceeded {
		t.Fatalf("got %v, want a deadline error", err)
	}
}

// TestClientContextCancelStream: cancelling the context tears down a stream —
// the returned channel closes.
func TestClientContextCancelStream(t *testing.T) {
	addr, stop := newServer(t, func(s valueserver.Server) {
		s.AddOutgoingStream("hold", valuerpc.Any,
			func(ctx context.Context, args value.Value) (<-chan value.Value, error) {
				out := make(chan value.Value)
				go func() {
					defer close(out)
					<-ctx.Done()
				}()
				return out, nil
			})
	})
	defer stop()

	cli := dialClient(t, addr)
	defer cli.Close()

	ctx, cancel := context.WithCancel(context.Background())
	readC, _, err := cli.GetStream(ctx, "hold", nil, 4)
	if err != nil {
		t.Fatalf("get stream: %v", err)
	}

	cancel()
	done := make(chan struct{})
	go func() {
		for range readC { // drain until closed
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("stream channel did not close after context cancel")
	}
}

// TestHighThroughputStreamLossless guards the inbound flow-control fix: a fast
// producer streaming many values through a chat echo must have every value
// delivered, not silently truncated when the buffer fills. (Before inbound
// throttling, the StreamPump dropped values past its bound — see the BenchmarkChatEcho
// regression.)
func TestHighThroughputStreamLossless(t *testing.T) {
	const n = 50000
	addr, stop := newServer(t, func(s valueserver.Server) {
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

	cli := dialClient(t, addr)
	defer cli.Close()
	cli.SetTimeout(60000)

	sendC := make(chan value.Value, 64)
	readC, _, err := cli.Chat(context.Background(), "echo", nil, 64, sendC)
	if err != nil {
		t.Fatalf("chat: %v", err)
	}
	go func() {
		for i := 0; i < n; i++ {
			sendC <- value.Long(int64(i))
		}
		close(sendC)
	}()

	got := 0
	for range readC {
		got++
	}
	if got != n {
		t.Fatalf("received %d echoes, want %d (inbound stream truncated — flow-control regression)", got, n)
	}
}

// TestInboundOverflowSurfaced pins #13: a *misbehaving* peer that ignores its
// flow-control credit and floods values overruns the bounded buffer; the server
// must surface that as an explicit error instead of silently dropping. (A
// cooperating client cannot trigger this — credit-based flow control makes a
// stuck consumer stall losslessly — so we drive it with a raw connection that
// ignores credit.)
func TestInboundOverflowSurfaced(t *testing.T) {
	oldICap, oldPending := valueserver.IncomingQueueCap, valuerpc.DefaultMaxPending
	defer func() {
		valueserver.IncomingQueueCap = oldICap
		valuerpc.DefaultMaxPending = oldPending
	}()
	valueserver.IncomingQueueCap = 4
	valuerpc.DefaultMaxPending = 4

	addr, stop := newServer(t, func(s valueserver.Server) {
		s.AddIncomingStream("blackhole", valuerpc.Any,
			func(ctx context.Context, args value.Value, inC <-chan value.Value) error {
				go func() { <-ctx.Done() }() // never reads inC
				return nil
			})
	})
	defer stop()

	dialer := valuerpc.NewDialer(addr, "", valueclient.KeepAlivePeriod, valueclient.DefaultTimeout, valuerpc.MaxFrameSize)
	conn, err := dialer.Dial(context.Background())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	if err := conn.WriteMessage(valuerpc.NewHandshakeRequest(0x5151, "")); err != nil {
		t.Fatalf("handshake: %v", err)
	}
	if _, err := conn.ReadMessage(); err != nil { // HandshakeResponse
		t.Fatalf("handshake response: %v", err)
	}

	const rid = int64(1)
	put := value.EmptyMap(true).
		Put(valuerpc.MessageTypeField, valuerpc.PutStreamRequest.Long()).
		Put(valuerpc.RequestIdField, value.Long(rid)).
		Put(valuerpc.FunctionNameField, value.Utf8("blackhole")).
		Put(valuerpc.ArgumentsField, value.Null)
	if err := conn.WriteMessage(put); err != nil {
		t.Fatalf("put request: %v", err)
	}

	// Flood far more than the credit window + buffer, ignoring StreamCredit.
	go func() {
		for i := 0; i < 500; i++ {
			sv := value.EmptyMap(true).
				Put(valuerpc.MessageTypeField, valuerpc.StreamValue.Long()).
				Put(valuerpc.RequestIdField, value.Long(rid)).
				Put(valuerpc.ValueField, value.Long(int64(i)))
			if conn.WriteMessage(sv) != nil {
				return
			}
		}
	}()

	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	for {
		msg, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("expected a truncation error response, got read error: %v", err)
		}
		if mt, ok := valuerpc.GetNumberField(msg, valuerpc.MessageTypeField); ok &&
			valuerpc.MessageType(mt.Long()) == valuerpc.ErrorResponse {
			return // server surfaced the overflow
		}
	}
}

// TestServerOptionPerInstance verifies #12: a server configured via a functional
// option enforces that setting per-instance, independent of the package-level
// default (which stays at 4096 here). It also confirms options are read at
// construction, not from a mutable global at runtime.
func TestServerOptionPerInstance(t *testing.T) {
	const limit = 2
	srv, err := valueserver.NewServer("127.0.0.1:0", zap.NewNop(),
		valueserver.WithMaxConcurrentRequests(limit))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	defer srv.Close()

	release := make(chan struct{})
	var once sync.Once
	closeRelease := func() { once.Do(func() { close(release) }) }
	defer closeRelease()

	srv.AddFunction("hold", valuerpc.Any, valuerpc.Any,
		func(_ context.Context, _ value.Value) (value.Value, error) {
			<-release
			return value.Utf8("ok"), nil
		})
	go srv.Run()

	cli := dialClient(t, srv.Addr().String())
	defer cli.Close()
	cli.SetTimeout(5000)

	const n = 6
	var wg sync.WaitGroup
	var busy int64
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := cli.CallFunction(context.Background(), "hold", nil); err != nil {
				atomic.AddInt64(&busy, 1)
			}
		}()
	}

	deadline := time.After(5 * time.Second)
	for atomic.LoadInt64(&busy) < n-limit {
		select {
		case <-deadline:
			t.Fatalf("rejected %d, want %d — per-instance WithMaxConcurrentRequests not applied", atomic.LoadInt64(&busy), n-limit)
		case <-time.After(10 * time.Millisecond):
		}
	}
	closeRelease()
	wg.Wait()
}

// TestServerMaxFrameSizeOption verifies the per-instance WithMaxFrameSize option
// is enforced on inbound frames, independent of the (large) package default — so
// the limit is per-server and captured at construction, not read from the global.
func TestServerMaxFrameSizeOption(t *testing.T) {
	srv, err := valueserver.NewServer("127.0.0.1:0", zap.NewNop(),
		valueserver.WithMaxFrameSize(256))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	defer srv.Close()
	srv.AddFunction("echo", valuerpc.Any, valuerpc.Any,
		func(_ context.Context, args value.Value) (value.Value, error) { return args, nil })
	go srv.Run()

	cli := dialClient(t, srv.Addr().String())
	defer cli.Close()
	cli.SetTimeout(1000)

	// A small message is under the limit and round-trips fine.
	if _, err := cli.CallFunction(context.Background(), "echo", value.Utf8("hi")); err != nil {
		t.Fatalf("small call should succeed: %v", err)
	}
	// A message larger than the per-server 256-byte limit is rejected by the
	// server's frame check (which would pass under the ~16 MiB global default).
	big := value.Utf8(string(make([]byte, 4096)))
	if _, err := cli.CallFunction(context.Background(), "echo", big); err == nil {
		t.Fatal("a frame over the per-server MaxFrameSize should be rejected")
	}
}

// metricsRec is a recording valuerpc.Metrics for tests.
type metricsRec struct {
	mu                       sync.Mutex
	begin, end, errs, stream int
	inflight, reconnects     int
}

func (m *metricsRec) RequestBegin(string) {
	m.mu.Lock()
	m.begin++
	m.inflight++
	m.mu.Unlock()
}
func (m *metricsRec) RequestEnd(_ string, code valuerpc.Code, _ time.Duration) {
	m.mu.Lock()
	m.end++
	m.inflight--
	if code != valuerpc.CodeOK {
		m.errs++
	}
	m.mu.Unlock()
}
func (m *metricsRec) StreamValue(string) { m.mu.Lock(); m.stream++; m.mu.Unlock() }
func (m *metricsRec) Reconnect()         { m.mu.Lock(); m.reconnects++; m.mu.Unlock() }

// metricsSnapshot is a mutex-free copy, safe to pass by value / format.
type metricsSnapshot struct {
	begin, end, errs, stream, inflight, reconnects int
}

func (m *metricsRec) snapshot() metricsSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	return metricsSnapshot{begin: m.begin, end: m.end, errs: m.errs, stream: m.stream, inflight: m.inflight, reconnects: m.reconnects}
}

// TestClientMetrics verifies WithMetrics receives request/error counters, the
// in-flight gauge (begin−end), and stream throughput.
// TestServerMetrics verifies the same valuerpc.Metrics interface wired on the
// server: request/error counters, in-flight gauge (begin−end), and stream
// throughput for unary and streaming requests.
func TestServerMetrics(t *testing.T) {
	rec := &metricsRec{}
	srv, err := valueserver.NewServer("127.0.0.1:0", zap.NewNop(), valueserver.WithMetrics(rec))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	srv.AddFunction("echo", valuerpc.Any, valuerpc.Any,
		func(_ context.Context, a value.Value) (value.Value, error) { return a, nil })
	srv.AddFunction("boom", valuerpc.Any, valuerpc.Any,
		func(_ context.Context, _ value.Value) (value.Value, error) {
			return nil, valuerpc.NewError(valuerpc.CodeInvalidArgument, "no")
		})
	srv.AddOutgoingStream("count", valuerpc.Any,
		func(_ context.Context, _ value.Value) (<-chan value.Value, error) {
			out := make(chan value.Value, 3)
			go func() {
				defer close(out)
				for i := 0; i < 3; i++ {
					out <- value.Long(int64(i))
				}
			}()
			return out, nil
		})
	go srv.Run()
	defer srv.Close()

	cli := dialClient(t, srv.Addr().String())
	defer cli.Close()
	ctx := context.Background()

	if _, err := cli.CallFunction(ctx, "echo", value.Utf8("hi")); err != nil {
		t.Fatalf("echo: %v", err)
	}
	if _, err := cli.CallFunction(ctx, "boom", nil); err == nil {
		t.Fatal("boom should error")
	}
	readC, _, err := cli.GetStream(ctx, "count", nil, 8)
	if err != nil {
		t.Fatalf("get stream: %v", err)
	}
	for range readC {
	}

	// The stream's RequestEnd fires asynchronously (server teardown) — wait for
	// all 3 requests to settle.
	deadline := time.After(3 * time.Second)
	for {
		s := rec.snapshot()
		if s.end >= 3 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("server metrics did not settle: %+v", s)
		case <-time.After(10 * time.Millisecond):
		}
	}
	s := rec.snapshot()
	if s.begin != 3 || s.end != 3 {
		t.Errorf("begin=%d end=%d, want 3/3", s.begin, s.end)
	}
	if s.inflight != 0 {
		t.Errorf("in-flight gauge = %d, want 0", s.inflight)
	}
	if s.errs != 1 {
		t.Errorf("error count = %d, want 1 (boom)", s.errs)
	}
	if s.stream < 3 {
		t.Errorf("stream throughput = %d, want >= 3", s.stream)
	}
}

func TestClientMetrics(t *testing.T) {
	addr, stop := newServer(t, func(s valueserver.Server) {
		s.AddFunction("echo", valuerpc.Any, valuerpc.Any,
			func(_ context.Context, a value.Value) (value.Value, error) { return a, nil })
		s.AddFunction("boom", valuerpc.Any, valuerpc.Any,
			func(_ context.Context, _ value.Value) (value.Value, error) {
				return nil, valuerpc.NewError(valuerpc.CodeInvalidArgument, "no")
			})
		s.AddOutgoingStream("count", valuerpc.Any,
			func(_ context.Context, _ value.Value) (<-chan value.Value, error) {
				out := make(chan value.Value, 3)
				go func() {
					defer close(out)
					for i := 0; i < 3; i++ {
						out <- value.Long(int64(i))
					}
				}()
				return out, nil
			})
	})
	defer stop()

	rec := &metricsRec{}
	cli := valueclient.NewClient(addr, "", valueclient.WithMetrics(rec))
	defer cli.Close()
	ctx := context.Background()

	if _, err := cli.CallFunction(ctx, "echo", value.Utf8("hi")); err != nil {
		t.Fatalf("echo: %v", err)
	}
	if _, err := cli.CallFunction(ctx, "boom", nil); err == nil {
		t.Fatal("boom should error")
	}
	readC, _, err := cli.GetStream(ctx, "count", nil, 8)
	if err != nil {
		t.Fatalf("get stream: %v", err)
	}
	for range readC {
	}

	// RequestEnd fires asynchronously (response loop) — wait for all 3 to settle.
	deadline := time.After(3 * time.Second)
	for {
		s := rec.snapshot()
		if s.end >= 3 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("metrics did not settle: %+v", s)
		case <-time.After(10 * time.Millisecond):
		}
	}
	s := rec.snapshot()
	if s.begin != 3 || s.end != 3 {
		t.Errorf("begin=%d end=%d, want 3/3", s.begin, s.end)
	}
	if s.inflight != 0 {
		t.Errorf("in-flight gauge = %d, want 0", s.inflight)
	}
	if s.errs != 1 {
		t.Errorf("error count = %d, want 1 (boom)", s.errs)
	}
	if s.stream < 3 {
		t.Errorf("stream throughput = %d, want >= 3", s.stream)
	}
}

// TestReconnectBackoff: with a backoff policy, the client re-establishes the
// connection on its own after an outage — without a request driving the dial.
func TestReconnectBackoff(t *testing.T) {
	// A stable port so a replacement server can rebind after the first stops.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := l.Addr().String()
	l.Close()

	newSrv := func() valueserver.Server {
		s, err := valueserver.NewServer(addr, zap.NewNop())
		if err != nil {
			t.Fatalf("bind %s: %v", addr, err)
		}
		s.AddFunction("ping", valuerpc.Any, valuerpc.Any,
			func(_ context.Context, _ value.Value) (value.Value, error) { return value.Utf8("pong"), nil })
		go s.Run()
		return s
	}

	srvA := newSrv()
	cli := valueclient.NewClient(addr, "", valueclient.WithReconnectPolicy(valueclient.ReconnectPolicy{
		InitialBackoff: 20 * time.Millisecond,
		MaxBackoff:     100 * time.Millisecond,
		MaxAttempts:    -1, // retry until the server returns
		Jitter:         true,
	}))
	defer cli.Close()
	if err := cli.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	if _, err := cli.CallFunction(context.Background(), "ping", nil); err != nil {
		t.Fatalf("ping: %v", err)
	}

	// Drop the connection (stopping the server triggers the client's BadConnection
	// → reconnect backoff), then bring a replacement up on the same address.
	srvA.Close()
	time.Sleep(150 * time.Millisecond)
	srvB := newSrv()
	defer srvB.Close()

	// The backoff loop must reconnect by itself; assert IsActive WITHOUT making a
	// call (a call would drive ensureConnection and mask the backoff).
	deadline := time.After(3 * time.Second)
	for !cli.IsActive() {
		select {
		case <-deadline:
			t.Fatal("client did not auto-reconnect via backoff")
		case <-time.After(10 * time.Millisecond):
		}
	}
	// And it is usable once reconnected.
	if _, err := cli.CallFunction(context.Background(), "ping", nil); err != nil {
		t.Fatalf("ping after auto-reconnect: %v", err)
	}
}

// TestReconnectFailFast: by default an in-flight request is failed fast with
// ErrConnectionLost (CodeUnavailable) when the connection drops, rather than
// hanging until its timeout.
func TestReconnectFailFast(t *testing.T) {
	hold := make(chan struct{})
	started := make(chan struct{})
	var once sync.Once
	addr, stop := newServer(t, func(s valueserver.Server) {
		s.AddFunction("hold", valuerpc.Any, valuerpc.Any,
			func(_ context.Context, _ value.Value) (value.Value, error) {
				once.Do(func() { close(started) })
				<-hold
				return value.Utf8("late"), nil
			})
	})
	defer stop()
	defer close(hold)

	// Long timeout: a fail-fast result proves the policy, not the call timer.
	cli := valueclient.NewClient(addr, "", valueclient.WithTimeout(10000))
	defer cli.Close()
	if err := cli.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}

	errc := make(chan error, 1)
	go func() {
		_, err := cli.CallFunction(context.Background(), "hold", nil)
		errc <- err
	}()

	<-started       // request is in flight on the server
	cli.Reconnect() // drop + re-establish; in-flight must fail fast

	select {
	case err := <-errc:
		if valuerpc.CodeOf(err) != valuerpc.CodeUnavailable {
			t.Fatalf("in-flight call code = %v, want Unavailable (fail-fast)", valuerpc.CodeOf(err))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("in-flight call did not fail fast on reconnect (hung)")
	}
}

// TestReconnectReplayIdempotent: with an opt-in replay policy, an in-flight
// idempotent unary call is re-sent on the new connection and completes, instead
// of failing.
func TestReconnectReplayIdempotent(t *testing.T) {
	var calls int32
	firstHold := make(chan struct{})
	started := make(chan struct{})
	addr, stop := newServer(t, func(s valueserver.Server) {
		s.AddFunction("idem", valuerpc.Any, valuerpc.Any,
			func(_ context.Context, _ value.Value) (value.Value, error) {
				if atomic.AddInt32(&calls, 1) == 1 {
					close(started)
					<-firstHold // first attempt is abandoned by the reconnect
					return value.Utf8("first"), nil
				}
				return value.Utf8("pong"), nil // the replayed attempt succeeds
			})
	})
	defer stop()
	defer close(firstHold)

	cli := valueclient.NewClient(addr, "", valueclient.WithTimeout(10000),
		valueclient.WithReconnectPolicy(valueclient.ReconnectPolicy{
			ReplayUnary: func(method string) bool { return method == "idem" },
		}))
	defer cli.Close()
	if err := cli.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}

	resc := make(chan string, 1)
	errc := make(chan error, 1)
	go func() {
		r, err := cli.CallFunction(context.Background(), "idem", nil)
		if err != nil {
			errc <- err
			return
		}
		resc <- r.(value.String).String()
	}()

	<-started
	cli.Reconnect()

	select {
	case r := <-resc:
		if r != "pong" {
			t.Fatalf("replayed result = %q, want pong", r)
		}
	case err := <-errc:
		t.Fatalf("idempotent call should have been replayed, got error: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("replayed call never completed")
	}
}

// TestMetadataPropagation verifies trace/baggage metadata flows from the call's
// context, through the envelope, onto the server handler's context — both as raw
// metadata and via the WithMetadataExtractor enrichment hook.
func TestMetadataPropagation(t *testing.T) {
	type tpKey struct{}   // client: trace id carried in the call context
	type spanKey struct{} // server: value produced by the extractor

	var (
		mu           sync.Mutex
		gotMD        valuerpc.Metadata
		gotExtracted string
	)

	srv, err := valueserver.NewServer("127.0.0.1:0", zap.NewNop(),
		valueserver.WithMetadataExtractor(func(ctx context.Context, md valuerpc.Metadata) context.Context {
			return context.WithValue(ctx, spanKey{}, md["traceparent"])
		}))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	srv.AddFunction("trace", valuerpc.Any, valuerpc.Any,
		func(ctx context.Context, _ value.Value) (value.Value, error) {
			mu.Lock()
			gotMD = valuerpc.MetadataFromContext(ctx)
			gotExtracted, _ = ctx.Value(spanKey{}).(string)
			mu.Unlock()
			return value.Utf8("ok"), nil
		})
	go srv.Run()
	defer srv.Close()

	cli := valueclient.NewClient(srv.Addr().String(), "",
		valueclient.WithMetadata(func(ctx context.Context) valuerpc.Metadata {
			tp, _ := ctx.Value(tpKey{}).(string)
			return valuerpc.Metadata{"traceparent": tp, "baggage": "x=1"}
		}))
	defer cli.Close()

	ctx := context.WithValue(context.Background(), tpKey{}, "00-abc-def-01")
	if _, err := cli.CallFunction(ctx, "trace", nil); err != nil {
		t.Fatalf("call: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if gotMD["traceparent"] != "00-abc-def-01" || gotMD["baggage"] != "x=1" {
		t.Errorf("handler metadata = %v, want traceparent=00-abc-def-01 baggage=x=1", gotMD)
	}
	if gotExtracted != "00-abc-def-01" {
		t.Errorf("extractor-enriched ctx value = %q, want %q", gotExtracted, "00-abc-def-01")
	}
}

// TestClientLogger verifies the client routes diagnostics through the injected
// *zap.Logger (WithLogger) — the same structured logger glue apps pass to the
// server — instead of stdlib log.
func TestClientLogger(t *testing.T) {
	addr, stop := newServer(t, func(s valueserver.Server) {
		s.AddFunction("ping", valuerpc.Any, valuerpc.Any,
			func(_ context.Context, _ value.Value) (value.Value, error) { return value.Utf8("pong"), nil })
	})
	defer stop()

	core, logs := observer.New(zap.DebugLevel)
	cli := valueclient.NewClient(addr, "", valueclient.WithLogger(zap.New(core)))
	defer cli.Close()
	if err := cli.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	if _, err := cli.CallFunction(context.Background(), "ping", nil); err != nil {
		t.Fatalf("call: %v", err)
	}

	// The default connection handler logs "connection established" (Debug) from
	// the response loop goroutine once the handshake reply arrives.
	deadline := time.After(2 * time.Second)
	for logs.FilterMessage("connection established").Len() == 0 {
		select {
		case <-deadline:
			t.Fatal("expected a 'connection established' entry via the injected logger")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// TestConnectContextCanceled: a cancelled context aborts the dial promptly
// instead of blocking on connection establishment.
func TestConnectContextCanceled(t *testing.T) {
	cli := valueclient.NewClient("192.0.2.1:9", "") // TEST-NET-1: unroutable
	defer cli.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled

	start := time.Now()
	if err := cli.ConnectContext(ctx); err == nil {
		t.Fatal("expected ConnectContext to fail for a cancelled context")
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("cancelled dial was not aborted promptly: took %v", d)
	}
}

// TestConnectDialTimeout: WithDialTimeout bounds a dial to an unreachable peer so
// Connect cannot hang on the OS default connect timeout.
func TestConnectDialTimeout(t *testing.T) {
	cli := valueclient.NewClient("192.0.2.1:9", "", // TEST-NET-1: blackholed
		valueclient.WithDialTimeout(200*time.Millisecond))
	defer cli.Close()

	start := time.Now()
	if err := cli.Connect(); err == nil {
		t.Fatal("expected the dial to an unroutable address to fail")
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("dial was not bounded by WithDialTimeout: took %v", d)
	}
}

// TestErrorCodes verifies the machine-readable error model: server-classified
// failures and handler-supplied codes round-trip to the caller, who can branch
// with valuerpc.CodeOf / errors.As instead of string-matching.
func TestErrorCodes(t *testing.T) {
	addr, stop := newServer(t, func(s valueserver.Server) {
		s.AddFunction("bad", valuerpc.Any, valuerpc.Any,
			func(_ context.Context, _ value.Value) (value.Value, error) {
				return nil, valuerpc.NewError(valuerpc.CodeInvalidArgument, "bad input")
			})
		s.AddFunction("boom", valuerpc.Any, valuerpc.Any,
			func(_ context.Context, _ value.Value) (value.Value, error) {
				return nil, fmt.Errorf("kaboom") // plain error -> Internal
			})
	})
	defer stop()

	cli := dialClient(t, addr)
	defer cli.Close()
	ctx := context.Background()

	if _, err := cli.CallFunction(ctx, "missing", nil); valuerpc.CodeOf(err) != valuerpc.CodeNotFound {
		t.Errorf("unknown function: code = %v, want NotFound", valuerpc.CodeOf(err))
	}

	_, err := cli.CallFunction(ctx, "bad", nil)
	if valuerpc.CodeOf(err) != valuerpc.CodeInvalidArgument {
		t.Errorf("handler coded error: code = %v, want InvalidArgument", valuerpc.CodeOf(err))
	}
	var rpcErr *valuerpc.Error
	if !errors.As(err, &rpcErr) || rpcErr.Code != valuerpc.CodeInvalidArgument {
		t.Errorf("errors.As did not yield *valuerpc.Error{InvalidArgument}: %v", err)
	}

	if _, err := cli.CallFunction(ctx, "boom", nil); valuerpc.CodeOf(err) != valuerpc.CodeInternal {
		t.Errorf("plain handler error: code = %v, want Internal", valuerpc.CodeOf(err))
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
				res, err := cli.CallFunction(context.Background(), "square", value.Tuple(value.Long(n)))
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
	stream, _, err := cli.GetStream(context.Background(), "firehose", nil, 4)
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
			res, err := cli.CallFunction(context.Background(), "square", value.Tuple(value.Long(i)))
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
	readC, _, err := cli.Chat(context.Background(), "echo", nil, 16, sendC)
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
		if _, err := cli.CallFunction(context.Background(), "noop", arg); err != nil {
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
			if _, err := cli.CallFunction(context.Background(), "noop", arg); err != nil {
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
	_, _ = cli.CallFunction(context.Background(), "boom", nil)

	cli.SetTimeout(3000)
	res, err := cli.CallFunction(context.Background(), "ok", nil)
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
	if _, err := cli.CallFunction(context.Background(), "ping", nil); err != nil {
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

	if _, err := cli.CallFunction(context.Background(), "needsNumber", value.Tuple(value.Utf8("not a number"))); err == nil {
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

	readC, _, err := cli.GetStream(context.Background(), "range", nil, 256)
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
			readC, _, err := cli.GetStream(context.Background(), "seq", value.Tuple(value.Long(base*per), value.Long(per)), 64)
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
	readC, _, err := cli.GetStream(context.Background(), "range", value.Tuple(value.Long(int64(b.N))), 256)
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
	readC, _, err := cli.Chat(context.Background(), "echo", nil, 64, sendC)
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
