/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valueclient

// Regression tests for the 2026-07 review fixes. They are internal (package
// valueclient) so they can observe requestCtxMap, whose leakage was the core of
// the retirement bugs.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valuerpc"
	"go.arpabet.com/value-rpc/valueserver"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
)

var fixSockSeq int64

func fixServe(t testing.TB, setup func(valueserver.Server)) (string, func()) {
	t.Helper()
	sock := filepath.Join(os.TempDir(),
		fmt.Sprintf("vrpc-fix-%d-%d.sock", time.Now().UnixNano(), atomic.AddInt64(&fixSockSeq, 1)))
	srv, err := valueserver.NewUnixServer(sock, zap.NewNop())
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	setup(srv)
	go srv.Run()
	return sock, func() { srv.Close(); os.Remove(sock) }
}

func requestMapLen(c Client) int {
	n := 0
	c.(*rpcClient).requestCtxMap.Range(func(_, _ any) bool { n++; return true })
	return n
}

// waitFor polls cond up to 2s — teardown paths run on their own goroutines.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// A cancelled stream must be retired from the request map on a live connection:
// the server does not acknowledge a stream cancel, so the local retirement in
// CancelRequest is the only thing standing between "cancel" and a per-request
// memory leak. The stream channel must also close so a draining consumer exits.
func TestCancelledStreamRetiresRequestEntry(t *testing.T) {
	sock, stop := fixServe(t, func(s valueserver.Server) {
		_ = s.AddOutgoingStream("ticks", valuerpc.Any,
			func(ctx context.Context, _ value.Value) (<-chan value.Value, error) {
				out := make(chan value.Value)
				go func() {
					defer close(out)
					for i := 0; ; i++ {
						select {
						case out <- value.Long(int64(i)):
						case <-ctx.Done():
							return
						}
					}
				}()
				return out, nil
			})
	})
	defer stop()

	cli := NewUnixClient(sock)
	defer cli.Close()
	if err := cli.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	ch, _, err := cli.GetStream(ctx, "ticks", nil, 8)
	if err != nil {
		t.Fatalf("get stream: %v", err)
	}
	<-ch // stream is live
	cancel()

	waitFor(t, "request map to drain after cancel", func() bool { return requestMapLen(cli) == 0 })
	waitFor(t, "stream channel to close after cancel", func() bool {
		for {
			if _, ok := <-ch; !ok {
				return true
			}
		}
	})
}

// A unary call that times out client-side must be retired from the request map
// even when the server handler never replies (it is stuck), not parked until an
// answer that may never come.
func TestUnaryTimeoutRetiresRequestEntry(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	sock, stop := fixServe(t, func(s valueserver.Server) {
		_ = s.AddFunction("stuck", valuerpc.Any, valuerpc.Any,
			func(ctx context.Context, _ value.Value) (value.Value, error) {
				<-release // ignores ctx on purpose
				return value.Null, nil
			})
	})
	defer stop()

	cli := NewUnixClient(sock)
	defer cli.Close()
	if err := cli.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	cli.SetTimeout(50)

	_, err := cli.CallFunction(context.Background(), "stuck", nil)
	if err == nil {
		t.Fatal("call to a stuck handler should time out")
	}
	waitFor(t, "request map to drain after timeout", func() bool { return requestMapLen(cli) == 0 })
}

// SetTimeout(0) must mean "no client-side timer" (the call is bounded by ctx
// and the wire SLA is omitted) — not "every call times out instantly".
func TestSetTimeoutZeroDisablesClientTimer(t *testing.T) {
	sock, stop := fixServe(t, func(s valueserver.Server) {
		_ = s.AddFunction("slow", valuerpc.Any, valuerpc.Any,
			func(ctx context.Context, _ value.Value) (value.Value, error) {
				time.Sleep(100 * time.Millisecond)
				return value.Utf8("done"), nil
			})
	})
	defer stop()

	cli := NewUnixClient(sock)
	defer cli.Close()
	if err := cli.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	cli.SetTimeout(0)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err := cli.CallFunction(ctx, "slow", nil)
	if err != nil {
		t.Fatalf("call with SetTimeout(0) failed: %v", err)
	}
	if got := res.(value.String).String(); got != "done" {
		t.Fatalf("got %q, want done", got)
	}
}

// After Close, Connect/Reconnect must fail with ErrClientClosed instead of
// resurrecting a connection (leaking its loops and socket) on a closed client.
func TestConnectAfterCloseFails(t *testing.T) {
	sock, stop := fixServe(t, func(s valueserver.Server) {})
	defer stop()

	cli := NewUnixClient(sock)
	if err := cli.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	if err := cli.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := cli.Connect(); err == nil {
		t.Fatal("Connect on a closed client should fail")
	}
	if cli.IsActive() {
		t.Fatal("closed client must not report an active connection")
	}
}

// A handshake the server refuses (here: authentication) must surface from
// Connect as the server's coded error — not as an opaque EOF the caller cannot
// distinguish from a network failure.
func TestHandshakeRejectionSurfacesCodedError(t *testing.T) {
	sock, stop := fixServe(t, func(s valueserver.Server) {
		s.SetAuthenticator(func(_ valuerpc.MsgConn, cred value.Value) (string, error) {
			return "", xerrors.New("nope")
		})
	})
	defer stop()

	cli := NewUnixClient(sock)
	defer cli.Close()
	err := cli.Connect()
	if err == nil {
		t.Fatal("connect should be rejected by the authenticator")
	}
	if code := valuerpc.CodeOf(err); code != valuerpc.CodeUnauthenticated {
		t.Fatalf("connect error code = %v (%v), want Unauthenticated", code, err)
	}
}
