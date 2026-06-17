/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valueclient_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
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

var sockSeq int64

func tmpSock(t testing.TB) string {
	t.Helper()
	return filepath.Join(os.TempDir(),
		fmt.Sprintf("vrpc-cli-%d-%d.sock", time.Now().UnixNano(), atomic.AddInt64(&sockSeq, 1)))
}

// serve starts a Unix-socket server with the given handlers and options, and
// returns its socket path plus a cleanup func.
func serve(t testing.TB, setup func(valueserver.Server), opts ...valueserver.ServerOption) (string, func()) {
	t.Helper()
	sock := tmpSock(t)
	srv, err := valueserver.NewUnixServer(sock, zap.NewNop(), opts...)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	setup(srv)
	go srv.Run()
	return sock, func() { srv.Close(); os.Remove(sock) }
}

func dial(t testing.TB, sock string, opts ...valueclient.ClientOption) valueclient.Client {
	t.Helper()
	cli := valueclient.NewUnixClient(sock, opts...)
	if err := cli.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	return cli
}

// TestCallPatterns exercises all four interaction patterns through the client,
// covering sendRequest, processResponse, the result pump, credit gating, and
// streamOut.
func TestCallPatterns(t *testing.T) {
	sock, stop := serve(t, func(s valueserver.Server) {
		s.AddFunction("echo", valuerpc.Any, valuerpc.Any,
			func(_ context.Context, a value.Value) (value.Value, error) { return a, nil })
		s.AddOutgoingStream("count", valuerpc.Any,
			func(_ context.Context, a value.Value) (<-chan value.Value, error) {
				n := int(a.(value.Number).Long())
				out := make(chan value.Value)
				go func() {
					defer close(out)
					for i := 0; i < n; i++ {
						out <- value.Long(int64(i))
					}
				}()
				return out, nil
			})
		s.AddIncomingStream("sum", valuerpc.Any,
			func(_ context.Context, _ value.Value, in <-chan value.Value) error {
				go func() {
					for range in {
					}
				}()
				return nil
			})
		s.AddChat("rev", valuerpc.Any,
			func(_ context.Context, _ value.Value, in <-chan value.Value) (<-chan value.Value, error) {
				out := make(chan value.Value)
				go func() {
					defer close(out)
					for v := range in {
						out <- v
					}
				}()
				return out, nil
			})
	})
	defer stop()

	cli := dial(t, sock)
	defer cli.Close()
	ctx := context.Background()

	// unary
	res, err := cli.CallFunction(ctx, "echo", value.Utf8("hi"))
	if err != nil || res.(value.String).String() != "hi" {
		t.Fatalf("echo: res=%v err=%v", res, err)
	}

	// server stream
	readC, _, err := cli.GetStream(ctx, "count", value.Long(50), 8)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	got := 0
	for range readC {
		got++
	}
	if got != 50 {
		t.Fatalf("count delivered %d, want 50", got)
	}

	// client stream
	putC := make(chan value.Value, 4)
	if err := cli.PutStream(ctx, "sum", nil, putC); err != nil {
		t.Fatalf("sum: %v", err)
	}
	for i := 0; i < 10; i++ {
		putC <- value.Long(int64(i))
	}
	close(putC)

	// chat (bidirectional)
	chatIn := make(chan value.Value, 4)
	chatOut, _, err := cli.Chat(ctx, "rev", nil, 8, chatIn)
	if err != nil {
		t.Fatalf("chat: %v", err)
	}
	chatIn <- value.Utf8("a")
	chatIn <- value.Utf8("b")
	close(chatIn)
	n := 0
	for range chatOut {
		n++
	}
	if n != 2 {
		t.Fatalf("chat echoed %d, want 2", n)
	}
}

// TestHighThroughputStreamLossless drives enough values that the credit-based
// flow control (pump + gate) must engage; every value must arrive.
func TestHighThroughputStreamLossless(t *testing.T) {
	const total = 5000
	sock, stop := serve(t, func(s valueserver.Server) {
		s.AddOutgoingStream("burst", valuerpc.Any,
			func(_ context.Context, _ value.Value) (<-chan value.Value, error) {
				out := make(chan value.Value)
				go func() {
					defer close(out)
					for i := 0; i < total; i++ {
						out <- value.Long(int64(i))
					}
				}()
				return out, nil
			})
	})
	defer stop()

	cli := dial(t, sock, valueclient.WithStreamMaxPending(32))
	defer cli.Close()

	readC, _, err := cli.GetStream(context.Background(), "burst", nil, 16)
	if err != nil {
		t.Fatalf("burst: %v", err)
	}
	got := 0
	for range readC {
		got++
	}
	if got != total {
		t.Fatalf("received %d of %d (lossy)", got, total)
	}
}

func TestCallErrorAndUnknown(t *testing.T) {
	sock, stop := serve(t, func(s valueserver.Server) {
		s.AddFunction("boom", valuerpc.Any, valuerpc.Any,
			func(_ context.Context, _ value.Value) (value.Value, error) {
				return nil, valuerpc.NewError(valuerpc.CodeInvalidArgument, "bad")
			})
	})
	defer stop()

	cli := dial(t, sock)
	defer cli.Close()
	ctx := context.Background()

	if _, err := cli.CallFunction(ctx, "boom", nil); valuerpc.CodeOf(err) != valuerpc.CodeInvalidArgument {
		t.Errorf("boom code = %v, want InvalidArgument", valuerpc.CodeOf(err))
	}
	if _, err := cli.CallFunction(ctx, "nope", nil); valuerpc.CodeOf(err) != valuerpc.CodeNotFound {
		t.Errorf("unknown code = %v, want NotFound", valuerpc.CodeOf(err))
	}
}

func TestMetadataInjection(t *testing.T) {
	var got valuerpc.Metadata
	var mu sync.Mutex
	sock, stop := serve(t, func(s valueserver.Server) {
		s.AddFunction("trace", valuerpc.Any, valuerpc.Any,
			func(ctx context.Context, _ value.Value) (value.Value, error) {
				mu.Lock()
				got = valuerpc.MetadataFromContext(ctx)
				mu.Unlock()
				return value.Utf8("ok"), nil
			})
	})
	defer stop()

	cli := dial(t, sock, valueclient.WithMetadata(func(context.Context) valuerpc.Metadata {
		return valuerpc.Metadata{"traceparent": "abc"}
	}))
	defer cli.Close()

	if _, err := cli.CallFunction(context.Background(), "trace", nil); err != nil {
		t.Fatalf("call: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if got["traceparent"] != "abc" {
		t.Errorf("server metadata = %v, want traceparent=abc", got)
	}
}

type metricsRec struct {
	mu                       sync.Mutex
	begin, end, errs, stream int
	reconnects               int
}

func (m *metricsRec) RequestBegin(string) { m.mu.Lock(); m.begin++; m.mu.Unlock() }
func (m *metricsRec) RequestEnd(_ string, code valuerpc.Code, _ time.Duration) {
	m.mu.Lock()
	m.end++
	if code != valuerpc.CodeOK {
		m.errs++
	}
	m.mu.Unlock()
}
func (m *metricsRec) StreamValue(string) { m.mu.Lock(); m.stream++; m.mu.Unlock() }
func (m *metricsRec) Reconnect()         { m.mu.Lock(); m.reconnects++; m.mu.Unlock() }

func TestMetricsRecorded(t *testing.T) {
	sock, stop := serve(t, func(s valueserver.Server) {
		s.AddFunction("echo", valuerpc.Any, valuerpc.Any,
			func(_ context.Context, a value.Value) (value.Value, error) { return a, nil })
	})
	defer stop()

	rec := &metricsRec{}
	cli := dial(t, sock, valueclient.WithMetrics(rec))
	defer cli.Close()

	for i := 0; i < 3; i++ {
		if _, err := cli.CallFunction(context.Background(), "echo", value.Long(int64(i))); err != nil {
			t.Fatalf("echo: %v", err)
		}
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if rec.begin != 3 || rec.end != 3 {
		t.Errorf("begin=%d end=%d, want 3/3", rec.begin, rec.end)
	}
}

// TestReconnectFailsInFlight: an in-flight request is failed fast with
// ErrConnectionLost (CodeUnavailable) when the connection is re-established under
// it (the default reconnect policy), rather than hanging until its timeout.
func TestReconnectFailsInFlight(t *testing.T) {
	hold := make(chan struct{})
	started := make(chan struct{})
	var once sync.Once
	sock, stop := serve(t, func(s valueserver.Server) {
		s.AddFunction("hold", valuerpc.Any, valuerpc.Any,
			func(_ context.Context, _ value.Value) (value.Value, error) {
				once.Do(func() { close(started) })
				<-hold
				return value.Utf8("late"), nil
			})
	})
	defer stop()
	defer close(hold)

	// A long call timeout: a fail-fast result proves the policy, not the timer.
	cli := dial(t, sock, valueclient.WithTimeout(10000))
	defer cli.Close()

	errc := make(chan error, 1)
	go func() {
		_, err := cli.CallFunction(context.Background(), "hold", nil)
		errc <- err
	}()
	<-started
	cli.Reconnect() // re-establish under the in-flight call; default fail-fast

	select {
	case err := <-errc:
		if valuerpc.CodeOf(err) != valuerpc.CodeUnavailable {
			t.Fatalf("in-flight call code = %v, want Unavailable (fail-fast)", valuerpc.CodeOf(err))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("in-flight call did not fail fast on reconnect")
	}
}

func TestReconnectBackoffAutoReestablish(t *testing.T) {
	sock := tmpSock(t)
	newSrv := func() valueserver.Server {
		srv, err := valueserver.NewUnixServer(sock, zap.NewNop())
		if err != nil {
			t.Fatalf("server: %v", err)
		}
		srv.AddFunction("ping", valuerpc.Any, valuerpc.Any,
			func(_ context.Context, _ value.Value) (value.Value, error) { return value.Utf8("pong"), nil })
		go srv.Run()
		return srv
	}

	srvA := newSrv()
	cli := valueclient.NewUnixClient(sock, valueclient.WithReconnectPolicy(valueclient.ReconnectPolicy{
		InitialBackoff: 20 * time.Millisecond,
		MaxBackoff:     100 * time.Millisecond,
		MaxAttempts:    -1,
		Jitter:         true,
	}))
	defer cli.Close()
	defer os.Remove(sock)
	if err := cli.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	if _, err := cli.CallFunction(context.Background(), "ping", nil); err != nil {
		t.Fatalf("ping: %v", err)
	}

	srvA.Close()
	time.Sleep(120 * time.Millisecond)
	os.Remove(sock)
	srvB := newSrv()
	defer srvB.Close()

	deadline := time.After(3 * time.Second)
	for !cli.IsActive() {
		select {
		case <-deadline:
			t.Fatal("client did not auto-reconnect via backoff")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestLifecycle(t *testing.T) {
	sock, stop := serve(t, func(s valueserver.Server) {
		s.AddFunction("echo", valuerpc.Any, valuerpc.Any,
			func(_ context.Context, a value.Value) (value.Value, error) { return a, nil })
	})
	defer stop()

	var monMu sync.Mutex
	monCalls := 0
	cli := dial(t, sock)
	cli.SetMonitor(func(name string, elapsed int64) {
		monMu.Lock()
		monCalls++
		monMu.Unlock()
	})
	cli.SetTimeout(2000)

	if cli.ClientId() == 0 {
		t.Error("ClientId should be non-zero")
	}
	if !cli.IsActive() {
		t.Error("IsActive should be true after Connect")
	}
	if _, err := cli.CallFunction(context.Background(), "echo", value.Long(1)); err != nil {
		t.Fatalf("echo: %v", err)
	}
	if st := cli.Stats(); st["requests"] < 1 {
		t.Errorf("Stats requests = %d, want >= 1", st["requests"])
	}

	monMu.Lock()
	if monCalls == 0 {
		t.Error("performance monitor was never called")
	}
	monMu.Unlock()

	// Close is idempotent.
	if err := cli.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	if err := cli.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
	if cli.IsActive() {
		t.Error("IsActive should be false after Close")
	}
}

func TestConnectContextCanceled(t *testing.T) {
	cli := valueclient.NewClient("192.0.2.1:9", "") // unroutable
	defer cli.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := cli.ConnectContext(ctx); err == nil {
		t.Fatal("expected ConnectContext to fail on a cancelled context")
	}
}

func TestCredentialAuth(t *testing.T) {
	sock, stop := serve(t, func(s valueserver.Server) {
		s.SetAuthenticator(func(_ valuerpc.MsgConn, cred value.Value) (string, error) {
			if cred == nil || cred.(value.String).String() != "sekret" {
				return "", errors.New("denied")
			}
			return "alice", nil
		})
		s.AddFunction("ping", valuerpc.Any, valuerpc.Any,
			func(_ context.Context, _ value.Value) (value.Value, error) { return value.Utf8("pong"), nil })
	})
	defer stop()

	// Wrong/absent credential is rejected at handshake.
	bad := valueclient.NewUnixClient(sock)
	if err := bad.Connect(); err == nil {
		if _, err := bad.CallFunction(context.Background(), "ping", nil); err == nil {
			t.Error("expected the call to fail without a valid credential")
		}
	}
	bad.Close()

	// Correct credential authenticates.
	good := valueclient.NewUnixClient(sock)
	good.SetCredential(value.Utf8("sekret"))
	if err := good.Connect(); err != nil {
		t.Fatalf("authenticated connect: %v", err)
	}
	defer good.Close()
	if _, err := good.CallFunction(context.Background(), "ping", nil); err != nil {
		t.Fatalf("authenticated call: %v", err)
	}
}
