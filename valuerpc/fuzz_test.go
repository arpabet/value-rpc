/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valuerpc_test

import (
	"bytes"
	"encoding/binary"
	"testing"

	"go.arpabet.com/value"
	vrpc "go.arpabet.com/value-rpc/valuerpc"
)

// byteConn adapts a fixed byte slice to an io.ReadWriteCloser so NewMsgConn can
// read framed messages out of adversarial input. Writes are discarded.
type byteConn struct{ r *bytes.Reader }

func (c *byteConn) Read(p []byte) (int, error)  { return c.r.Read(p) }
func (c *byteConn) Write(p []byte) (int, error) { return len(p), nil }
func (c *byteConn) Close() error                { return nil }

// frame builds a length-prefixed frame around payload (the wire format
// messageConnAdapter.ReadMessage expects).
func frame(payload []byte) []byte {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(payload)))
	return append(hdr[:], payload...)
}

func pk(v value.Value) []byte { b, _ := value.Pack(v); return b }

// FuzzReadMessage feeds arbitrary bytes through the length-prefix + msgpack
// decode path. The contract under test: ReadMessage must never panic on
// malformed/adversarial input — it returns an error instead. The frame-size cap
// also bounds how much it will allocate from a hostile length prefix.
func FuzzReadMessage(f *testing.F) {
	// A valid framed message.
	valid, _ := value.Pack(value.EmptyMap(true).
		Put(vrpc.MessageTypeField, vrpc.FunctionRequest.Long()).
		Put(vrpc.RequestIdField, value.Long(1)).
		Put(vrpc.FunctionNameField, value.Utf8("ping")))
	f.Add(frame(valid))
	f.Add([]byte{0, 0, 0, 0})              // zero-length frame
	f.Add([]byte{0, 0, 0, 5, 1, 2})        // truncated payload
	f.Add([]byte{0xff, 0xff, 0xff, 0xff})  // huge declared length, no payload
	f.Add(frame([]byte{0xc1}))             // msgpack "never used" byte
	f.Add(frame([]byte{0x81, 0xc0, 0xc0})) // map with a null key
	f.Add(frame(pk(value.Long(7))))        // a valid non-map msgpack value
	f.Add([]byte{})                        // empty

	const maxFrame = 1 << 16 // bound allocation during fuzzing
	f.Fuzz(func(t *testing.T, data []byte) {
		mc := vrpc.NewMsgConn(&byteConn{r: bytes.NewReader(data)}, 0, maxFrame)
		// Drain frames until the stream errors; any panic fails the fuzz.
		for i := 0; i < 64; i++ {
			if _, err := mc.ReadMessage(); err != nil {
				break
			}
		}
	})
}

// FuzzUnpack fuzzes the raw msgpack decode used by ReadMessage, independent of
// framing — it must not panic on arbitrary payloads.
func FuzzUnpack(f *testing.F) {
	f.Add(pk(value.Long(1)))
	f.Add(pk(value.Utf8("x")))
	f.Add([]byte{0xc1})
	f.Add([]byte{0x81})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = value.Unpack(data, true)
	})
}
