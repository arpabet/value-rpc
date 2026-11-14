/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valueserver_test

import (
	"context"
	"io"
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
	"go.uber.org/zap/zapcore"
)

// heldListener hands out connections whose writes a test can hold: a sender
// that has not come to its queue yet, or a peer that does not read.
type heldListener struct {
	valuerpc.Listener
	conns chan *heldConn
}

func (l *heldListener) Accept() (valuerpc.MsgConn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	h := &heldConn{MsgConn: c, at: make(chan struct{}, 1), open: make(chan struct{}), closed: make(chan struct{})}
	l.conns <- h
	return h, nil
}

// heldConn is a server-side connection with a gate in front of its writes.
type heldConn struct {
	valuerpc.MsgConn
	hold   atomic.Bool   // writes wait at the gate from now on
	at     chan struct{} // hears that a write has come to the gate
	open   chan struct{} // the gate: a test closes it to let the writes go on
	closed chan struct{} // closed with the connection, which fails a held write
	once   sync.Once
	// writing says a write is between the gate and its end; underWrite, that
	// the connection was closed while one was.
	writing    atomic.Bool
	underWrite atomic.Bool
}

func (c *heldConn) WriteMessage(m value.Map) error {
	if c.hold.Load() {
		c.writing.Store(true)
		defer c.writing.Store(false)
		select {
		case c.at <- struct{}{}:
		default:
		}
		select {
		case <-c.open:
		case <-c.closed:
			return net.ErrClosed
		}
	}
	return c.MsgConn.WriteMessage(m)
}

func (c *heldConn) Close() error {
	c.once.Do(func() {
		if c.writing.Load() {
			c.underWrite.Store(true)
		}
		close(c.closed)
	})
	return c.MsgConn.Close()
}

// awaitSignal waits for a test's event; the deadline only bounds a failure.
func awaitSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(30 * time.Second):
		t.Fatalf("%s: not within 30 s", what)
	}
}

type callResult struct {
	v   value.Value
	err error
}

// endOf hears of the end of a method's call on the server: the handler has
// returned and the server has made its answer.
type endOf struct {
	valuerpc.NopMetrics
	method string
	once   sync.Once
	ended  chan struct{}
}

func (m *endOf) RequestEnd(method string, _ valuerpc.Code, _ time.Duration) {
	if method == m.method {
		m.once.Do(func() { close(m.ended) })
	}
}

// heldAnswer is a server with one call whose handler has returned and whose
// answer the server's sender is writing, held at the connection's gate: an
// answer that is queued when the server closes. A second call is in its
// handler still, and waits for its context to end.
type heldAnswer struct {
	srv      valueserver.Server
	conn     *heldConn
	flushing chan struct{} // closed when Close says it writes what is queued
	got      chan callResult
	running  context.Context // the context of the call that is still running
	ended    chan struct{}   // closed when that call has ended on the server
}

// done is called at a test's end, the server closed: the call that was still
// running has ended, so that nothing of this test runs into the next one.
func (h *heldAnswer) done(t *testing.T) {
	t.Helper()
	awaitSignal(t, h.ended, "the end of the call that was still running")
}

func newHeldAnswer(t *testing.T, opts ...valueserver.ServerOption) *heldAnswer {
	t.Helper()
	inner, err := valuerpc.NewStreamListener("tcp", "127.0.0.1:0", 0, 0, 0)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	h := &heldAnswer{flushing: make(chan struct{}), got: make(chan callResult, 1), ended: make(chan struct{})}
	opts = append(opts, valueserver.WithMetrics(&endOf{method: "wait", ended: h.ended}))
	lis := &heldListener{Listener: inner, conns: make(chan *heldConn, 1)}
	var once sync.Once
	logger := zap.New(
		zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), zapcore.AddSync(io.Discard), zap.DebugLevel),
		zap.Hooks(func(e zapcore.Entry) error {
			if e.Message == "write the queued messages before the connections close" {
				once.Do(func() { close(h.flushing) })
			}
			return nil
		}))
	h.srv, err = valueserver.NewServerWithListener(lis, logger, opts...)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	t.Cleanup(func() { h.srv.Close() })
	entered, release := make(chan struct{}), make(chan struct{})
	h.srv.AddFunction("work", valuerpc.Void, valuerpc.String,
		func(_ context.Context, _ value.Value) (value.Value, error) {
			close(entered)
			<-release
			return value.Utf8("done"), nil
		})
	contexts := make(chan context.Context, 1)
	h.srv.AddFunction("wait", valuerpc.Void, valuerpc.String,
		func(ctx context.Context, _ value.Value) (value.Value, error) {
			contexts <- ctx
			<-ctx.Done()
			return nil, ctx.Err()
		})
	go h.srv.Run()

	cli := valueclient.NewClient(h.srv.Addr().String(), "")
	if err := cli.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { cli.Close() })
	cli.SetTimeout(30000)
	select {
	case h.conn = <-lis.conns:
	case <-time.After(30 * time.Second):
		t.Fatal("the server did not accept the client's connection")
	}
	go func() {
		v, err := cli.CallFunction(context.Background(), "work", nil)
		h.got <- callResult{v, err}
	}()
	awaitSignal(t, entered, "the call in its handler")
	go func() { _, _ = cli.CallFunction(context.Background(), "wait", nil) }()
	select {
	case h.running = <-contexts:
	case <-time.After(30 * time.Second):
		t.Fatal("the second call did not come to its handler within 30 s")
	}
	// The first handler returns, and its answer stops at the gate.
	h.conn.hold.Store(true)
	close(release)
	awaitSignal(t, h.conn.at, "the answer at the connection")
	return h
}

// TestCloseWritesWhatIsQueued: an answer the server had queued for a
// connection when Close was called is written before the connection closes,
// and its call ends with it. Close used to close the connections first: the
// call then failed with a lost connection, though its handler had returned.
// A handler that is still running is cancelled as before, and only when what
// was ready is written: its answer to the cancellation is not what the wait
// is for.
func TestCloseWritesWhatIsQueued(t *testing.T) {
	h := newHeldAnswer(t, valueserver.WithCloseFlushTimeout(time.Minute))
	closed := make(chan struct{})
	go func() {
		h.srv.Close()
		close(closed)
	}()
	select {
	case <-h.flushing:
	case <-closed:
		t.Fatal("Close returned with an answer queued, and did not write it")
	case <-time.After(30 * time.Second):
		t.Fatal("Close neither returned nor came to write what is queued within 30 s")
	}
	select {
	case <-closed:
		t.Fatal("Close returned while the answer was being written")
	default:
	}
	if err := h.running.Err(); err != nil {
		t.Fatalf("the handler that is still running was cancelled before the queued answer was written: %v", err)
	}
	close(h.conn.open)
	select {
	case r := <-h.got:
		if r.err != nil {
			t.Fatalf("the call the server had finished: %v", r.err)
		}
		if s, ok := r.v.(value.String); !ok || s.String() != "done" {
			t.Fatalf("the call's answer = %v, want done", r.v)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the call did not end within 30 s")
	}
	awaitSignal(t, closed, "Close")
	if h.conn.underWrite.Load() {
		t.Fatal("the connection was closed while the answer was being written")
	}
	if h.running.Err() == nil {
		t.Fatal("Close returned and the handler that is still running was not cancelled")
	}
	h.done(t)
}

// TestCloseDoesNotWaitForAPeerForGood: the wait for what is queued is bounded.
// A connection whose write does not go on is closed under it when the bound
// has passed, and at once when the wait is switched off.
func TestCloseDoesNotWaitForAPeerForGood(t *testing.T) {
	for _, tc := range []struct {
		name  string
		bound time.Duration
	}{
		{"after the bound", 200 * time.Millisecond},
		{"at once without the wait", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHeldAnswer(t, valueserver.WithCloseFlushTimeout(tc.bound))
			started := time.Now()
			closed := make(chan struct{})
			go func() {
				h.srv.Close()
				close(closed)
			}()
			awaitSignal(t, closed, "Close, the answer's write holding still")
			if waited := time.Since(started); waited < tc.bound {
				t.Fatalf("Close returned after %s; it waits %s for what is queued", waited, tc.bound)
			}
			if !h.conn.underWrite.Load() {
				t.Fatal("the gate was not held when the connection closed: the test proved nothing")
			}
			select {
			case <-h.flushing:
				if tc.bound <= 0 {
					t.Fatal("Close waited for what is queued with the wait switched off")
				}
			default:
				if tc.bound > 0 {
					t.Fatal("Close did not come to write what is queued")
				}
			}
			select {
			case r := <-h.got:
				if r.err == nil {
					t.Fatalf("the call was answered %v through a connection that was closed under the answer", r.v)
				}
			case <-time.After(30 * time.Second):
				t.Fatal("the call did not end within 30 s")
			}
			h.done(t)
		})
	}
}
