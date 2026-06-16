/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valuerpc_test

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"go.arpabet.com/value"
	vrpc "go.arpabet.com/value-rpc/valuerpc"
)

// pipePair returns two MsgConns wired together in-memory.
func pipePair(t testing.TB) (a, b vrpc.MsgConn, raw1, raw2 net.Conn) {
	t.Helper()
	c1, c2 := net.Pipe()
	return vrpc.NewMsgConn(c1, time.Second), vrpc.NewMsgConn(c2, time.Second), c1, c2
}

func TestMsgConn_RoundTrip(t *testing.T) {
	a, b, _, _ := pipePair(t)
	defer a.Close()
	defer b.Close()

	msg := value.EmptyMap(true).
		Put("hello", value.Utf8("world")).
		Put("n", value.Long(42)).
		Put("list", value.Tuple(value.Long(1), value.Double(2.5)))

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
		t.Fatalf("round-trip mismatch:\n got %s\nwant %s", got, msg)
	}
}

func TestMsgConn_Pipeline(t *testing.T) {
	a, b, _, _ := pipePair(t)
	defer a.Close()
	defer b.Close()

	const n = 50
	errc := make(chan error, 1)
	go func() {
		for i := 0; i < n; i++ {
			if err := a.WriteMessage(value.EmptyMap(true).Put("i", value.Long(int64(i)))); err != nil {
				errc <- err
				return
			}
		}
		errc <- nil
	}()

	for i := 0; i < n; i++ {
		m, err := b.ReadMessage()
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if got := m.GetNumber("i").Long(); got != int64(i) {
			t.Fatalf("message %d arrived out of order: got %d", i, got)
		}
	}
	if err := <-errc; err != nil {
		t.Fatalf("write: %v", err)
	}
}

// TestMsgConn_RejectsOversizeFrame is the BUG-11 regression: a length prefix
// claiming a huge frame must be rejected by length before any attempt to
// allocate or read the body.
func TestMsgConn_RejectsOversizeFrame(t *testing.T) {
	old := vrpc.MaxFrameSize
	vrpc.MaxFrameSize = 1024
	defer func() { vrpc.MaxFrameSize = old }()

	c1, c2 := net.Pipe()
	b := vrpc.NewMsgConn(c2, time.Second)
	defer b.Close()
	defer c1.Close()

	go func() {
		var hdr [4]byte
		binary.BigEndian.PutUint32(hdr[:], 1<<20) // claim 1 MiB, far over the 1 KiB cap
		_ = c1.SetWriteDeadline(time.Now().Add(time.Second))
		_, _ = c1.Write(hdr[:]) // header only, no body
	}()

	_, err := b.ReadMessage()
	if err == nil {
		t.Fatal("expected oversize frame to be rejected")
	}
	if !strings.Contains(err.Error(), "frame too large") {
		t.Fatalf("expected a 'frame too large' error, got: %v", err)
	}
}

func TestMsgConn_AcceptsFrameAtLimit(t *testing.T) {
	old := vrpc.MaxFrameSize
	vrpc.MaxFrameSize = 64 * 1024
	defer func() { vrpc.MaxFrameSize = old }()

	a, b, _, _ := pipePair(t)
	defer a.Close()
	defer b.Close()

	// A reasonably large but in-limit message must round-trip.
	big := make([]rune, 4096)
	for i := range big {
		big[i] = 'x'
	}
	msg := value.EmptyMap(true).Put("blob", value.Utf8(string(big)))

	errc := make(chan error, 1)
	go func() { errc <- a.WriteMessage(msg) }()
	got, err := b.ReadMessage()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if werr := <-errc; werr != nil {
		t.Fatalf("write: %v", werr)
	}
	if got.GetString("blob").Len() != 4096 {
		t.Fatalf("blob length = %d, want 4096", got.GetString("blob").Len())
	}
}

// TestMsgConn_WireFormat guards the on-wire framing: a 4-byte big-endian length
// (excluding itself) followed by the exact value.Pack payload. Peers and older
// versions depend on this, so an accidental change should fail loudly.
func TestMsgConn_WireFormat(t *testing.T) {
	c1, c2 := net.Pipe()
	a := vrpc.NewMsgConn(c1, time.Second)
	defer a.Close()
	defer c2.Close()

	msg := value.EmptyMap(true).Put("k", value.Long(7))
	payload, err := value.Pack(msg)
	if err != nil {
		t.Fatalf("pack: %v", err)
	}

	errc := make(chan error, 1)
	go func() { errc <- a.WriteMessage(msg) }()

	raw := make([]byte, 4+len(payload))
	if _, err := io.ReadFull(c2, raw); err != nil {
		t.Fatalf("read raw: %v", err)
	}
	if werr := <-errc; werr != nil {
		t.Fatalf("write: %v", werr)
	}

	if n := binary.BigEndian.Uint32(raw[:4]); int(n) != len(payload) {
		t.Fatalf("length prefix = %d, want %d", n, len(payload))
	}
	if !bytes.Equal(raw[4:], payload) {
		t.Fatal("on-wire payload differs from value.Pack output")
	}
}

// TestMsgConn_RemoteAddrAndReadDeadline covers the two MsgConn methods added by
// the transport-seam refactor (they replaced Conn() net.Conn).
func TestMsgConn_RemoteAddrAndReadDeadline(t *testing.T) {
	c1, c2 := net.Pipe()
	a := vrpc.NewMsgConn(c1, time.Second)
	defer a.Close()
	defer c2.Close()

	if a.RemoteAddr() == "" {
		t.Error("RemoteAddr should be non-empty")
	}

	// A deadline in the past must make ReadMessage fail instead of blocking.
	if err := a.SetReadDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	if _, err := a.ReadMessage(); err == nil {
		t.Fatal("expected ReadMessage to fail past the read deadline")
	}
}

func BenchmarkMsgConnWriteRead(b *testing.B) {
	c1, c2 := net.Pipe()
	w := vrpc.NewMsgConn(c1, 5*time.Second)
	r := vrpc.NewMsgConn(c2, 5*time.Second)
	defer w.Close()
	defer r.Close()

	msg := value.EmptyMap(true).
		Put(vrpc.MessageTypeField, vrpc.FunctionRequest.Long()).
		Put(vrpc.RequestIdField, value.Long(7)).
		Put(vrpc.FunctionNameField, value.Utf8("doStuff")).
		Put(vrpc.ArgumentsField, value.Tuple(value.Utf8("hi"), value.Long(1)))

	b.ReportAllocs()
	b.ResetTimer()
	go func() {
		for i := 0; i < b.N; i++ {
			_ = w.WriteMessage(msg)
		}
	}()
	for i := 0; i < b.N; i++ {
		if _, err := r.ReadMessage(); err != nil {
			b.Fatal(err)
		}
	}
}
