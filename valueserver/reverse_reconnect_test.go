/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valueserver

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.arpabet.com/value"
	vrpc "go.arpabet.com/value-rpc/valuerpc"
	"go.uber.org/zap"
)

// blockingConn is a minimal MsgConn whose ReadMessage blocks until Close, for
// exercising servingClient lifecycle without a real socket.
type blockingConn struct {
	closed chan struct{}
	once   sync.Once
}

func newBlockingConn() *blockingConn { return &blockingConn{closed: make(chan struct{})} }

func (c *blockingConn) ReadMessage() (value.Map, error) {
	<-c.closed
	return nil, vrpc.ErrClientClosed
}
func (c *blockingConn) WriteMessage(value.Map) error    { return nil }
func (c *blockingConn) SetReadDeadline(time.Time) error { return nil }
func (c *blockingConn) RemoteAddr() string              { return "blocking" }
func (c *blockingConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return nil
}

// TestReplaceConnFailsReverseInFlight verifies R2: when a reconnect replaces the
// connection, in-flight server->client reverse requests (a pending reverse unary
// and an open reverse stream) are failed fast with CodeUnavailable instead of
// hanging until their deadline or stalling forever on desynced credit.
func TestReplaceConnFailsReverseInFlight(t *testing.T) {
	cfg := newServerConfig(nil)
	cli := NewServingClient(context.Background(), 1, "", "", newBlockingConn(), &sync.Map{}, zap.NewNop(), &cfg)
	defer cli.Close()

	// A reverse unary awaiting the client's reply.
	resCh := make(chan callResult, 1)
	cli.pendingCalls.Store(int64(-1), resCh)

	// A reverse get-stream awaiting client values.
	cs := newClientStream(-2, getStreamKind, 4, 16, func(int64) {})
	cli.pendingStreams.Store(int64(-2), cs)

	// A reconnect attaches a new connection.
	if !cli.replaceConn(newBlockingConn()) {
		t.Fatal("replaceConn returned false on a live session")
	}

	// The reverse unary is failed with CodeUnavailable.
	select {
	case r := <-resCh:
		if vrpc.CodeOf(r.err) != vrpc.CodeUnavailable {
			t.Fatalf("reverse call err = %v (code %v), want CodeUnavailable", r.err, vrpc.CodeOf(r.err))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("reverse unary was not failed on reconnect (would hang until ctx deadline)")
	}

	// The reverse stream is removed and its consumer channel closed (not left to
	// stall forever).
	if _, ok := cli.pendingStreams.Load(int64(-2)); ok {
		t.Fatal("reverse stream still registered after reconnect")
	}
	select {
	case _, ok := <-cs.resultCh:
		_ = ok // closed (ok=false) or a drained buffered value then close — both unblock
	case <-time.After(2 * time.Second):
		t.Fatal("reverse stream consumer channel not closed on reconnect")
	}
	if vrpc.CodeOf(cs.err(nil)) != vrpc.CodeUnavailable {
		t.Fatalf("reverse stream err = %v, want CodeUnavailable", cs.err(nil))
	}
}

// TestServeNewRequestRejectsNonPositiveId verifies R4: a client-initiated new
// request carrying a non-positive id (the server-initiated / handshake space) is
// rejected instead of being registered as a serving request under a colliding
// negative id.
func TestServeNewRequestRejectsNonPositiveId(t *testing.T) {
	cfg := newServerConfig(nil)
	cli := NewServingClient(context.Background(), 1, "", "", newBlockingConn(), &sync.Map{}, zap.NewNop(), &cfg)
	defer cli.Close()

	for _, badId := range []int64{0, -5} {
		req := value.EmptyMap(true).
			Put(vrpc.DefaultDialect.MessageTypeField, vrpc.FunctionRequest.Long()).
			Put(vrpc.DefaultDialect.RequestIdField, value.Long(badId)).
			Put(vrpc.DefaultDialect.FunctionNameField, value.Utf8("whatever"))
		if err := cli.processRequest(req); err == nil {
			t.Fatalf("new request with id %d was accepted, want rejection", badId)
		}
		if _, ok := cli.findServingRequest(value.Long(badId)); ok {
			t.Fatalf("new request with id %d was registered as a serving request", badId)
		}
	}
}
