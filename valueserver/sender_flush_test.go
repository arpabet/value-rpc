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

// stepConn is a connection whose writes go on one at a time, when a test
// says so. It keeps the messages as they are and reads nothing of the
// process: the sender may finish a write after the test has ended.
type stepConn struct {
	mu      sync.Mutex
	written []value.Map
	at      chan struct{} // hears of each write as it comes
	step    chan struct{} // lets one write go on
}

func (c *stepConn) ReadMessage() (value.Map, error) { select {} }

func (c *stepConn) WriteMessage(m value.Map) error {
	c.at <- struct{}{}
	<-c.step
	c.mu.Lock()
	c.written = append(c.written, m)
	c.mu.Unlock()
	return nil
}

func (c *stepConn) SetReadDeadline(time.Time) error { return nil }
func (c *stepConn) RemoteAddr() string              { return "step" }
func (c *stepConn) Close() error                    { return nil }

// wrote lists the request ids of the messages written so far.
func (c *stepConn) wrote() []int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	var ids []int64
	for _, m := range c.written {
		id, _ := vrpc.GetNumberField(m, vrpc.DefaultDialect.RequestIdField)
		ids = append(ids, id.Long())
	}
	return ids
}

// TestSenderFlushWritesWhatIsQueued: a flush is answered only when the
// messages that were queued behind the write in progress are on the
// connection, in their order. The server closes a connection on that answer.
func TestSenderFlushWritesWhatIsQueued(t *testing.T) {
	cfg := newServerConfig(nil)
	conn := &stepConn{at: make(chan struct{}, 8), step: make(chan struct{})}
	cli := NewServingClient(context.Background(), 1, "", "", conn, &sync.Map{}, zap.NewNop(), &cfg)
	defer func() {
		close(conn.step) // whatever is at the gate goes on, and the sender ends
		cli.Close()
	}()
	next := func(what string) {
		t.Helper()
		select {
		case <-conn.at:
		case <-time.After(30 * time.Second):
			t.Fatalf("%s: not within 30 s", what)
		}
	}
	answer := func(id int64) value.Map { return FunctionResult(value.Long(id), value.Utf8("ok")) }

	// The sender is in the write of the first answer; two more are queued.
	if err := cli.send(answer(1)); err != nil {
		t.Fatal(err)
	}
	next("the first answer at the connection")
	for _, id := range []int64{2, 3} {
		if err := cli.send(answer(id)); err != nil {
			t.Fatal(err)
		}
	}
	ack := make(chan struct{})
	cli.flushC <- ack
	conn.step <- struct{}{}
	// The flush is being served: the second answer is at the connection, and
	// the flush is not answered while it and the third are not written.
	next("the second answer at the connection")
	select {
	case <-ack:
		t.Fatalf("the flush was answered with %v written and two answers queued", conn.wrote())
	default:
	}
	conn.step <- struct{}{}
	next("the third answer at the connection")
	select {
	case <-ack:
		t.Fatalf("the flush was answered with %v written and one answer queued", conn.wrote())
	default:
	}
	conn.step <- struct{}{}
	select {
	case <-ack:
	case <-time.After(30 * time.Second):
		t.Fatal("the flush was not answered within 30 s")
	}
	if got := conn.wrote(); len(got) != 3 || got[0] != 1 || got[1] != 2 || got[2] != 3 {
		t.Fatalf("written at the flush's answer: %v, want 1 2 3", got)
	}

	// A flush of an empty queue is answered, and one whose caller stops
	// waiting says that it was not.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if !cli.flush(ctx) {
		t.Fatal("the flush of an empty queue was not answered")
	}
	if err := cli.send(answer(4)); err != nil {
		t.Fatal(err)
	}
	next("the fourth answer at the connection")
	gone, giveUp := context.WithCancel(context.Background())
	giveUp()
	if cli.flush(gone) {
		t.Fatal("a flush whose caller had stopped waiting reported the queue written, the sender held in a write")
	}
}
