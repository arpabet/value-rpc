/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valuerpc_test

import (
	"io"
	"net"
	"testing"
	"time"

	"go.arpabet.com/value"
	vrpc "go.arpabet.com/value-rpc/valuerpc"
)

// rwcOnly exposes only io.ReadWriteCloser, hiding the net.Conn deadline/address
// methods, to prove NewMsgConn works over a non-net.Conn stream.
type rwcOnly struct{ rwc io.ReadWriteCloser }

func (r rwcOnly) Read(p []byte) (int, error)  { return r.rwc.Read(p) }
func (r rwcOnly) Write(p []byte) (int, error) { return r.rwc.Write(p) }
func (r rwcOnly) Close() error                { return r.rwc.Close() }

// TestNewMsgConn_ReadWriteCloser proves the seam accepts any io.ReadWriteCloser
// (not just net.Conn): a message round-trips, SetReadDeadline degrades to a no-op
// instead of erroring, and RemoteAddr is empty when the stream has no address.
func TestNewMsgConn_ReadWriteCloser(t *testing.T) {
	c1, c2 := net.Pipe()
	a := vrpc.NewMsgConn(rwcOnly{c1}, time.Second, vrpc.MaxFrameSize)
	b := vrpc.NewMsgConn(rwcOnly{c2}, time.Second, vrpc.MaxFrameSize)
	defer a.Close()
	defer b.Close()

	if got := a.RemoteAddr(); got != "" {
		t.Errorf("RemoteAddr over a non-net.Conn = %q, want empty", got)
	}
	// No deadline support underneath: must be a no-op, not an error.
	if err := a.SetReadDeadline(time.Now().Add(time.Hour)); err != nil {
		t.Errorf("SetReadDeadline no-op returned error: %v", err)
	}

	msg := value.EmptyMap(true).Put("hello", value.Utf8("world"))
	errc := make(chan error, 1)
	go func() { errc <- a.WriteMessage(msg) }()
	got, err := b.ReadMessage()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if werr := <-errc; werr != nil {
		t.Fatalf("write: %v", werr)
	}
	if !got.Equal(msg) {
		t.Fatalf("round-trip mismatch: got %s want %s", got, msg)
	}
}

// TestSingleConnDialer_ConsumedOnce: the first Dial yields the connection, a
// second (e.g. an RPC-layer reconnect) reports ErrConnConsumed.
func TestSingleConnDialer_ConsumedOnce(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c2.Close()
	d := vrpc.NewSingleConnDialer(c1, time.Second)

	mc, err := d.Dial()
	if err != nil {
		t.Fatalf("first Dial: %v", err)
	}
	defer mc.Close()
	if _, err := d.Dial(); err != vrpc.ErrConnConsumed {
		t.Fatalf("second Dial err = %v, want ErrConnConsumed", err)
	}
}

// TestFuncDialer_ReconnectEstablishes: each Dial calls connect again, so a
// reconnect gets a freshly established, framed connection.
func TestFuncDialer_ReconnectEstablishes(t *testing.T) {
	var dials int
	d := vrpc.NewFuncDialer(func() (io.ReadWriteCloser, error) {
		dials++
		c1, c2 := net.Pipe()
		go func() { <-time.After(time.Millisecond); c2.Close() }()
		return c1, nil
	}, time.Second)

	for i := 0; i < 3; i++ {
		mc, err := d.Dial()
		if err != nil {
			t.Fatalf("Dial %d: %v", i, err)
		}
		mc.Close()
	}
	if dials != 3 {
		t.Fatalf("connect called %d times, want 3", dials)
	}
}

// TestSingleConnListener_Lifecycle: Accept yields the connection once, then a
// later Accept blocks until Close and unblocks with ErrListenerClosed.
func TestSingleConnListener_Lifecycle(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c2.Close()
	lis := vrpc.NewSingleConnListener(c1, nil, time.Second)

	mc, err := lis.Accept()
	if err != nil {
		t.Fatalf("first Accept: %v", err)
	}
	mc.Close()

	if lis.Addr() == nil {
		t.Error("Addr should not be nil for a bring-your-own listener")
	}

	// The second Accept blocks until Close; run it in a goroutine.
	errc := make(chan error, 1)
	go func() { _, e := lis.Accept(); errc <- e }()
	time.Sleep(20 * time.Millisecond)
	if err := lis.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case e := <-errc:
		if e != vrpc.ErrListenerClosed {
			t.Fatalf("Accept after Close err = %v, want ErrListenerClosed", e)
		}
	case <-time.After(time.Second):
		t.Fatal("Accept did not unblock after Close")
	}
}
