/**
    Copyright (c) 2020-2022 Arpabet, Inc.

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in
	all copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
	THE SOFTWARE.
*/

package valuerpc

import (
	"encoding/binary"
	"go.arpabet.com/value"
	"github.com/pkg/errors"
	"github.com/smallnest/goframe"
	"net"
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

func NewMsgConn(conn net.Conn) MsgConn {
	framedConn := goframe.NewLengthFieldBasedFrameConn(encoderConfig, decoderConfig, conn)
	return &messageConnAdapter{framedConn}
}

type messageConnAdapter struct {
	conn goframe.FrameConn
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
	resp, err := value.Pack(msg)
	if err != nil {
		return errors.Errorf("msgpack pack, %v", err)
	}
	return t.conn.WriteFrame(resp)
}

func (t *messageConnAdapter) Close() error {
	return t.conn.Close()
}

func (t *messageConnAdapter) Conn() net.Conn {
	return t.conn.Conn()
}
