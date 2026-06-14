/*
 * Copyright (c) 2025 Karagatan LLC.
 * SPDX-License-Identifier: Apache-2.0
 */

package valuerpc

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"go.arpabet.com/value"
)

var (
	ErrClientClosed = fmt.Errorf("client closed")
)

// MaxFrameSize bounds the size of a single inbound message payload. A 4-byte
// length prefix would otherwise allow a peer to request a ~4 GiB allocation
// with a few bytes (BUG-11). Set to 0 to disable the check. Default 16 MiB
// (gRPC defaults to 4 MiB for comparison).
var MaxFrameSize = 16 * 1024 * 1024

// Wire format (unchanged from the previous goframe-based implementation):
//   [4-byte big-endian length N][N bytes MessagePack payload]
// The length does not include the 4-byte length field itself.

type MsgConn interface {
	ReadMessage() (value.Map, error)

	WriteMessage(msg value.Map) error

	Close() error

	Conn() net.Conn
}

func NewMsgConn(conn net.Conn, timeout time.Duration) MsgConn {
	return &messageConnAdapter{
		conn:    conn,
		reader:  bufio.NewReader(conn),
		timeout: timeout,
	}
}

type messageConnAdapter struct {
	conn      net.Conn
	reader    *bufio.Reader
	timeout   time.Duration
	writeLock sync.Mutex
	shutdown  atomic.Bool
}

func (t *messageConnAdapter) ReadMessage() (value.Map, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(t.reader, lenBuf[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(lenBuf[:])
	if MaxFrameSize > 0 && int64(n) > int64(MaxFrameSize) {
		return nil, errors.Errorf("frame too large: %d bytes (max %d)", n, MaxFrameSize)
	}
	payload := make([]byte, int(n))
	if _, err := io.ReadFull(t.reader, payload); err != nil {
		return nil, err
	}
	msg, err := value.Unpack(payload, true)
	if err != nil {
		return nil, errors.Errorf("msgpack unpack, %v", err)
	}
	if msg.Kind() != value.MAP {
		return nil, errors.New("expected msgpack map")
	}
	return msg.(value.Map), nil
}

func (t *messageConnAdapter) WriteMessage(msg value.Map) error {
	if t.shutdown.Load() {
		return ErrClientClosed
	}
	payload, err := value.Pack(msg)
	if err != nil {
		return errors.Errorf("msgpack pack, %v", err)
	}
	return t.doWriteFrame(payload)
}

func (t *messageConnAdapter) doWriteFrame(payload []byte) error {
	t.writeLock.Lock()
	defer t.writeLock.Unlock()
	if t.timeout > 0 {
		if err := t.conn.SetWriteDeadline(time.Now().Add(t.timeout)); err != nil {
			return err
		}
	}
	// Single buffer + single Write so the length prefix and payload cannot be
	// torn apart by a concurrent writer (writeLock already serializes callers).
	frame := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(payload)))
	copy(frame[4:], payload)
	_, err := t.conn.Write(frame)
	return err
}

func (t *messageConnAdapter) Close() error {
	t.shutdown.Store(true)
	return t.conn.Close()
}

func (t *messageConnAdapter) Conn() net.Conn {
	return t.conn
}
