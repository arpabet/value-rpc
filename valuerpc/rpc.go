/*
 * Copyright (c) 2025 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valuerpc

import (
	"encoding/binary"
	"fmt"
	"go.arpabet.com/value"
	"github.com/pkg/errors"
	"github.com/smallnest/goframe"
	"go.uber.org/atomic"
	"net"
	"sync"
	"time"
)

var (
	ErrClientClosed = fmt.Errorf("client closed")
)

var encoderConfig = goframe.EncoderConfig{
	ByteOrder:                       binary.BigEndian,
	LengthFieldLength:               4,
	LengthAdjustment:                0,
	LengthIncludesLengthFieldLength: false,
}

var decoderConfig = goframe.DecoderConfig{
	ByteOrder:           binary.BigEndian,
	LengthFieldOffset:   0,
	LengthFieldLength:   4,
	LengthAdjustment:    0,
	InitialBytesToStrip: 4,
}

type MsgConn interface {
	ReadMessage() (value.Map, error)

	WriteMessage(msg value.Map) error

	Close() error

	Conn() net.Conn
}

func NewMsgConn(conn net.Conn, timeout time.Duration) MsgConn {
	framedConn := goframe.NewLengthFieldBasedFrameConn(encoderConfig, decoderConfig, conn)
	return &messageConnAdapter{conn: framedConn, timeout: timeout}
}

type messageConnAdapter struct {
	conn goframe.FrameConn
	timeout  time.Duration
	writeLock sync.Mutex
	shutdown  atomic.Bool
}

func (t *messageConnAdapter) ReadMessage() (value.Map, error) {
	frame, err := t.conn.ReadFrame()
	if err != nil {
		return nil, err
	}
	msg, err := value.Unpack(frame, true)
	if err != nil {
		return nil, errors.Errorf("msgpack unpack, %v", err)
	}
	if msg.Kind() != value.MAP {
		return nil, errors.New("expected msgpack table")
	}
	return msg.(value.Map), nil
}

func (t *messageConnAdapter) WriteMessage(msg value.Map) error {
	if t.shutdown.Load() {
		return ErrClientClosed
	}
	resp, err := value.Pack(msg)
	if err != nil {
		return errors.Errorf("msgpack pack, %v", err)
	}
	return t.doWriteFrame(resp)
}

func (t* messageConnAdapter) doWriteFrame(payload []byte) error {
	t.writeLock.Lock()
	defer t.writeLock.Unlock()
	if err := t.conn.Conn().SetWriteDeadline(time.Now().Add(t.timeout)); err != nil {
		return err
	}
	return t.conn.WriteFrame(payload)
}

func (t *messageConnAdapter) Close() error {
	t.shutdown.Store(true)
	return t.conn.Close()
}

func (t *messageConnAdapter) Conn() net.Conn {
	return t.conn.Conn()
}
