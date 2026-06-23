/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valuerpc

import (
	"bufio"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"go.arpabet.com/value"
	"golang.org/x/xerrors"
)

var (
	ErrClientClosed = xerrors.New("client closed")
)

// MaxFrameSize is the default bound on the size of a single inbound message
// payload. A 4-byte length prefix would otherwise allow a peer to request a
// ~4 GiB allocation with a few bytes (BUG-11). 0 disables the check. Default
// 16 MiB (gRPC defaults to 4 MiB for comparison). It is the default used when a
// connection is created without an explicit limit; per-server/per-client
// overrides flow through NewMsgConn and are captured at construction (so the
// value is never read from this mutable global at runtime).
var MaxFrameSize = 16 * 1024 * 1024

// Wire format (unchanged from the previous goframe-based implementation):
//   [4-byte big-endian length N][N bytes MessagePack payload]
// The length does not include the 4-byte length field itself.

type MsgConn interface {
	ReadMessage() (value.Map, error)

	WriteMessage(msg value.Map) error

	// SetReadDeadline bounds the next ReadMessage; the zero time clears it. Used
	// by the handshake timeout. Transports without a meaningful deadline may
	// implement this as a best-effort no-op.
	SetReadDeadline(deadline time.Time) error

	// RemoteAddr is the peer address, for logging.
	RemoteAddr() string

	Close() error
}

// NewMsgConn frames an already-established byte stream with the length-prefix
// wire format and returns it as a MsgConn. conn is an io.ReadWriteCloser rather
// than only a net.Conn, so non-socket streams — a pluggable-transport / obfuscated
// connection, an ssh.Channel, a WebRTC data channel, an io.Pipe — can carry the
// RPC protocol unchanged. When conn also implements the optional net.Conn deadline
// and address methods they are used; otherwise SetReadDeadline is a best-effort
// no-op and RemoteAddr is empty. This is the lowest-level transport seam; a custom
// Dialer or Listener (see NewFuncDialer, NewSingleConnDialer, NewAcceptListener)
// usually wraps it.
// maxFrameSize is the inbound frame limit for this connection: > 0 enforces it,
// <= 0 means no limit. It is captured here so ReadMessage never reads the mutable
// MaxFrameSize global. Callers that want the package default pass MaxFrameSize.
func NewMsgConn(conn io.ReadWriteCloser, timeout time.Duration, maxFrameSize int) MsgConn {
	return &messageConnAdapter{
		conn:         conn,
		reader:       bufio.NewReader(conn),
		timeout:      timeout,
		maxFrameSize: maxFrameSize,
	}
}

// Optional capabilities a framed stream may also provide; a net.Conn provides all
// three. They are probed per call so any io.ReadWriteCloser works as a MsgConn.
type (
	readDeadlineConn  interface{ SetReadDeadline(time.Time) error }
	writeDeadlineConn interface{ SetWriteDeadline(time.Time) error }
	remoteAddrConn    interface{ RemoteAddr() net.Addr }
)

// reusableWriteBufCap bounds the per-connection write buffer that is retained
// and reused across messages. Messages larger than this use a one-off buffer
// (not retained) so a single huge message cannot pin memory on an idle conn.
const reusableWriteBufCap = 64 * 1024

type messageConnAdapter struct {
	conn         io.ReadWriteCloser
	reader       *bufio.Reader
	timeout      time.Duration
	maxFrameSize int // captured at construction; never reads the global at runtime
	writeLock    sync.Mutex
	writeBuf     []byte // reused under writeLock to avoid a per-message frame allocation
	shutdown     atomic.Bool
}

func (t *messageConnAdapter) ReadMessage() (value.Map, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(t.reader, lenBuf[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(lenBuf[:])
	if t.maxFrameSize > 0 && int64(n) > int64(t.maxFrameSize) {
		return nil, xerrors.Errorf("frame too large: %d bytes (max %d)", n, t.maxFrameSize)
	}
	payload := make([]byte, int(n))
	if _, err := io.ReadFull(t.reader, payload); err != nil {
		return nil, err
	}
	msg, err := value.Unpack(payload, true)
	if err != nil {
		return nil, xerrors.Errorf("msgpack unpack, %v", err)
	}
	if msg.Kind() != value.MAP {
		return nil, xerrors.New("expected msgpack map")
	}
	return msg.(value.Map), nil
}

func (t *messageConnAdapter) WriteMessage(msg value.Map) error {
	if t.shutdown.Load() {
		return ErrClientClosed
	}
	payload, err := value.Pack(msg)
	if err != nil {
		return xerrors.Errorf("msgpack pack, %v", err)
	}
	return t.doWriteFrame(payload)
}

func (t *messageConnAdapter) doWriteFrame(payload []byte) error {
	t.writeLock.Lock()
	defer t.writeLock.Unlock()
	if t.timeout > 0 {
		if wd, ok := t.conn.(writeDeadlineConn); ok {
			if err := wd.SetWriteDeadline(time.Now().Add(t.timeout)); err != nil {
				return err
			}
		}
	}
	// Single buffer + single Write so the length prefix and payload cannot be
	// torn apart by a concurrent writer (writeLock already serializes callers).
	// The buffer is reused across messages to avoid a per-message allocation;
	// oversized messages use a one-off buffer that is not retained.
	need := 4 + len(payload)
	var frame []byte
	if need <= reusableWriteBufCap {
		if cap(t.writeBuf) < need {
			t.writeBuf = make([]byte, need, reusableWriteBufCap)
		}
		frame = t.writeBuf[:need]
	} else {
		frame = make([]byte, need)
	}
	binary.BigEndian.PutUint32(frame[:4], uint32(len(payload)))
	copy(frame[4:], payload)
	_, err := t.conn.Write(frame)
	return err
}

func (t *messageConnAdapter) SetReadDeadline(deadline time.Time) error {
	if rd, ok := t.conn.(readDeadlineConn); ok {
		return rd.SetReadDeadline(deadline)
	}
	return nil // best-effort no-op for streams without deadlines
}

func (t *messageConnAdapter) RemoteAddr() string {
	if ra, ok := t.conn.(remoteAddrConn); ok {
		if addr := ra.RemoteAddr(); addr != nil {
			return addr.String()
		}
	}
	return ""
}

func (t *messageConnAdapter) Close() error {
	t.shutdown.Store(true)
	return t.conn.Close()
}
