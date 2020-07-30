/*
 *
 * Copyright 2020-present Arpabet Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package rpc

import (
	"encoding/binary"
	"github.com/consensusdb/value"
	"github.com/pkg/errors"
	"github.com/smallnest/goframe"
	"net"
)

/**
@author Alex Shvid
*/

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
