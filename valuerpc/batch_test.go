/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valuerpc_test

import (
	"net"
	"testing"

	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valuerpc"
)

// A batched write must produce exactly the same frame sequence as N individual
// writes: every message readable, in order, one frame each.
func TestWriteMessagesBatchFraming(t *testing.T) {
	a, b := net.Pipe()
	sender := valuerpc.NewMsgConn(a, 0, valuerpc.MaxFrameSize)
	receiver := valuerpc.NewMsgConn(b, 0, valuerpc.MaxFrameSize)
	defer sender.Close()
	defer receiver.Close()

	const n = 17
	msgs := make([]value.Map, n)
	for i := range msgs {
		msgs[i] = valuerpc.NewStreamValue(value.Long(int64(i+1)), value.Utf8("payload"))
	}

	bc, ok := sender.(valuerpc.BatchMsgConn)
	if !ok {
		t.Fatal("stream MsgConn should implement BatchMsgConn")
	}
	errCh := make(chan error, 1)
	go func() { errCh <- bc.WriteMessages(msgs) }()

	for i := 0; i < n; i++ {
		got, err := receiver.ReadMessage()
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		id, ok := valuerpc.GetNumberField(got, valuerpc.DefaultDialect.RequestIdField)
		if !ok || id.Long() != int64(i+1) {
			t.Fatalf("message %d out of order: got rid %v", i, got.Get(valuerpc.DefaultDialect.RequestIdField))
		}
		if s := got.GetString(valuerpc.DefaultDialect.ValueField).String(); s != "payload" {
			t.Fatalf("message %d payload = %q", i, s)
		}
	}
	if err := <-errCh; err != nil {
		t.Fatalf("batched write: %v", err)
	}

	// Interleaving with single writes keeps working (shared frame buffer).
	go func() { errCh <- sender.WriteMessage(valuerpc.NewStreamReady(value.Long(99))) }()
	got, err := receiver.ReadMessage()
	if err != nil {
		t.Fatalf("read after batch: %v", err)
	}
	if id, ok := valuerpc.GetNumberField(got, valuerpc.DefaultDialect.RequestIdField); !ok || id.Long() != 99 {
		t.Fatal("single write after a batch was corrupted")
	}
	if err := <-errCh; err != nil {
		t.Fatalf("single write after batch: %v", err)
	}
}
