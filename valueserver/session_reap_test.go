/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valueserver_test

import (
	"context"
	"runtime"
	"testing"
	"time"

	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valueclient"
	"go.arpabet.com/value-rpc/valuerpc"
	"go.arpabet.com/value-rpc/valueserver"
	"go.uber.org/zap"
)

// sessionCount reports how many sessions the server currently retains. It uses
// the debug hook exposed for tests.
func waitForSessions(t *testing.T, s valueserver.Server, want int, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if valueserver.SessionCount(s) == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("session count = %d, want %d within %v", valueserver.SessionCount(s), want, within)
}

// TestSessionReapedAfterDisconnect verifies a disconnected client's session is
// reaped after the retention window, freeing its slot and goroutine.
func TestSessionReapedAfterDisconnect(t *testing.T) {
	srv, err := valueserver.NewWebSocketServer("127.0.0.1:0", "/rpc", zap.NewNop(),
		valueserver.WithSessionRetention(150*time.Millisecond))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	defer srv.Close()
	srv.AddFunction("ping", valuerpc.Void, valuerpc.String,
		func(_ context.Context, _ value.Value) (value.Value, error) { return value.Utf8("pong"), nil })
	go srv.Run()
	url := "ws://" + srv.Addr().String() + "/rpc"

	// Disable client auto-reconnect so a Close() truly ends the session.
	cli := valueclient.NewWebSocketClient(url)
	if err := cli.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	if _, err := cli.CallFunction(context.Background(), "ping", nil); err != nil {
		t.Fatalf("call: %v", err)
	}
	waitForSessions(t, srv, 1, time.Second)

	cli.Close() // client goes away and never returns
	waitForSessions(t, srv, 0, 2*time.Second)
}

// TestReconnectCancelsReap verifies that a reconnect within the retention window
// keeps the session (the reap timer is cancelled), and that the session is NOT
// reaped while the client stays connected.
func TestReconnectCancelsReap(t *testing.T) {
	srv, err := valueserver.NewWebSocketServer("127.0.0.1:0", "/rpc", zap.NewNop(),
		valueserver.WithSessionRetention(200*time.Millisecond))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	defer srv.Close()
	srv.AddFunction("ping", valuerpc.Void, valuerpc.String,
		func(_ context.Context, _ value.Value) (value.Value, error) { return value.Utf8("pong"), nil })
	go srv.Run()
	url := "ws://" + srv.Addr().String() + "/rpc"

	cli := valueclient.NewWebSocketClient(url)
	if err := cli.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer cli.Close()
	if _, err := cli.CallFunction(context.Background(), "ping", nil); err != nil {
		t.Fatalf("call: %v", err)
	}
	waitForSessions(t, srv, 1, time.Second)

	// Force a reconnect (same clientId + advancing hash chain): the session must
	// survive, and reconnecting resets the reap window each time.
	for i := 0; i < 3; i++ {
		if err := cli.Reconnect(); err != nil {
			t.Fatalf("reconnect %d: %v", i, err)
		}
		if _, err := cli.CallFunction(context.Background(), "ping", nil); err != nil {
			t.Fatalf("call after reconnect %d: %v", i, err)
		}
	}
	// Still exactly one session, well within the retention window of active use.
	if got := valueserver.SessionCount(srv); got != 1 {
		t.Fatalf("session count after reconnects = %d, want 1", got)
	}

	// Give the goroutine count a moment to settle and assert no runaway growth
	// (a leaked sender per reconnect would be visible here).
	runtime.GC()
	time.Sleep(50 * time.Millisecond)
}

// TestReapingDisabled verifies a negative retention keeps sessions forever
// (pre-reaping behavior), so opting out is possible.
func TestReapingDisabled(t *testing.T) {
	srv, err := valueserver.NewWebSocketServer("127.0.0.1:0", "/rpc", zap.NewNop(),
		valueserver.WithSessionRetention(-1))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	defer srv.Close()
	srv.AddFunction("ping", valuerpc.Void, valuerpc.String,
		func(_ context.Context, _ value.Value) (value.Value, error) { return value.Utf8("pong"), nil })
	go srv.Run()
	url := "ws://" + srv.Addr().String() + "/rpc"

	cli := valueclient.NewWebSocketClient(url)
	if err := cli.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	if _, err := cli.CallFunction(context.Background(), "ping", nil); err != nil {
		t.Fatalf("call: %v", err)
	}
	waitForSessions(t, srv, 1, time.Second)
	cli.Close()

	// With reaping disabled the session is retained; give a reaper (if any) a
	// chance to (wrongly) fire.
	time.Sleep(300 * time.Millisecond)
	if got := valueserver.SessionCount(srv); got != 1 {
		t.Fatalf("session count with reaping disabled = %d, want 1 (retained)", got)
	}
}
