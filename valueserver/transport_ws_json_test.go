/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valueserver_test

import (
	"context"
	"testing"

	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valueclient"
	"go.arpabet.com/value-rpc/valuerpc"
	"go.arpabet.com/value-rpc/valueserver"
	"go.uber.org/zap"
)

// registerPatterns wires the four RPC patterns used by the JSON negotiation
// tests onto a server.
func registerPatterns(s valueserver.Server) {
	s.AddFunction("greet", valuerpc.Any, valuerpc.Any,
		func(_ context.Context, args value.Value) (value.Value, error) {
			return value.Utf8("Hello, " + args.(value.List).GetStringAt(0).String() + "!"), nil
		})
	s.AddOutgoingStream("count", valuerpc.Any,
		func(_ context.Context, args value.Value) (<-chan value.Value, error) {
			n := args.(value.List).GetNumberAt(0).Long()
			out := make(chan value.Value)
			go func() {
				defer close(out)
				for i := int64(1); i <= n; i++ {
					out <- value.Long(i)
				}
			}()
			return out, nil
		})
	s.AddIncomingStream("sum", valuerpc.Any,
		func(_ context.Context, _ value.Value, inC <-chan value.Value) error {
			// Drain; a companion unary reports the total via a closure below.
			go func() {
				for range inC {
				}
			}()
			return nil
		})
	s.AddChat("echo", valuerpc.Any,
		func(_ context.Context, _ value.Value, inC <-chan value.Value) (<-chan value.Value, error) {
			out := make(chan value.Value)
			go func() {
				defer close(out)
				for v := range inC {
					out <- value.Utf8("echo:" + v.(value.String).String())
				}
			}()
			return out, nil
		})
}

// TestWebSocketJSON_AllPatterns drives all four RPC patterns over the JSON codec
// (text frames), negotiated by the vrpc.json subprotocol against a default
// server (both codecs enabled).
func TestWebSocketJSON_AllPatterns(t *testing.T) {
	srv, url := newWSServer(t, "/rpc", registerPatterns)
	defer srv.Close()

	cli := valueclient.NewWebSocketClient(url, valueclient.WithWireCodec(valuerpc.JsonWireCodec))
	if err := cli.Connect(); err != nil {
		t.Fatalf("connect (json): %v", err)
	}
	defer cli.Close()
	cli.SetTimeout(5000)

	// unary
	res, err := cli.CallFunction(context.Background(), "greet", value.Tuple(value.Utf8("json")))
	if err != nil {
		t.Fatalf("unary: %v", err)
	}
	if got := res.(value.String).String(); got != "Hello, json!" {
		t.Fatalf("unary result = %q", got)
	}

	// server stream
	readC, _, err := cli.GetStream(context.Background(), "count", value.Tuple(value.Long(5)), 16)
	if err != nil {
		t.Fatalf("get stream: %v", err)
	}
	var sum int64
	for v := range readC {
		if v == nil || v.Kind() == value.NULL {
			continue
		}
		sum += v.(value.Number).Long()
	}
	if sum != 15 {
		t.Fatalf("stream sum = %d, want 15", sum)
	}

	// client stream
	sendC := make(chan value.Value, 4)
	go func() {
		for i := int64(0); i < 4; i++ {
			sendC <- value.Long(i)
		}
		close(sendC)
	}()
	if err := cli.PutStream(context.Background(), "sum", nil, sendC); err != nil {
		t.Fatalf("put stream: %v", err)
	}

	// bidirectional
	chatIn := make(chan value.Value, 2)
	chatOut, _, err := cli.Chat(context.Background(), "echo", nil, 16, chatIn)
	if err != nil {
		t.Fatalf("chat: %v", err)
	}
	go func() {
		chatIn <- value.Utf8("a")
		chatIn <- value.Utf8("b")
		close(chatIn)
	}()
	var echoes []string
	for v := range chatOut {
		if v == nil || v.Kind() == value.NULL {
			continue
		}
		echoes = append(echoes, v.(value.String).String())
	}
	if len(echoes) != 2 || echoes[0] != "echo:a" || echoes[1] != "echo:b" {
		t.Fatalf("chat echoes = %v", echoes)
	}
}

// TestWebSocketJSON_CoexistWithMsgpack proves a JSON browser and a msgpack Go
// peer share one listener and one handler, each getting its own codec.
func TestWebSocketJSON_CoexistWithMsgpack(t *testing.T) {
	srv, url := newWSServer(t, "/rpc", registerPatterns)
	defer srv.Close()

	msg := valueclient.NewWebSocketClient(url) // no codec -> msgpack (binary)
	if err := msg.Connect(); err != nil {
		t.Fatalf("connect (msgpack): %v", err)
	}
	defer msg.Close()
	msg.SetTimeout(5000)

	jsn := valueclient.NewWebSocketClient(url, valueclient.WithWireCodec(valuerpc.JsonWireCodec))
	if err := jsn.Connect(); err != nil {
		t.Fatalf("connect (json): %v", err)
	}
	defer jsn.Close()
	jsn.SetTimeout(5000)

	for name, cli := range map[string]valueclient.Client{"msgpack": msg, "json": jsn} {
		res, err := cli.CallFunction(context.Background(), "greet", value.Tuple(value.Utf8(name)))
		if err != nil {
			t.Fatalf("%s call: %v", name, err)
		}
		if got := res.(value.String).String(); got != "Hello, "+name+"!" {
			t.Fatalf("%s result = %q", name, got)
		}
	}
}

// TestWebSocketJSON_RejectedByMsgpackOnlyServer verifies a JSON client fails to
// connect (fast, with a clear error) against a server that only speaks msgpack —
// rather than silently sending text frames the server would choke on.
func TestWebSocketJSON_RejectedByMsgpackOnlyServer(t *testing.T) {
	srv, err := valueserver.NewWebSocketServer("127.0.0.1:0", "/rpc", zap.NewNop(),
		valueserver.WithWireCodecs(valuerpc.MsgpackWireCodec))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	defer srv.Close()
	registerPatterns(srv)
	go srv.Run()
	url := "ws://" + srv.Addr().String() + "/rpc"

	cli := valueclient.NewWebSocketClient(url, valueclient.WithWireCodec(valuerpc.JsonWireCodec))
	if err := cli.Connect(); err == nil {
		cli.Close()
		t.Fatal("json client should fail against a msgpack-only server")
	}

	// A msgpack client still works against the same server.
	msg := valueclient.NewWebSocketClient(url)
	if err := msg.Connect(); err != nil {
		t.Fatalf("msgpack client should still connect: %v", err)
	}
	defer msg.Close()
	msg.SetTimeout(5000)
	if _, err := msg.CallFunction(context.Background(), "greet", value.Tuple(value.Utf8("m"))); err != nil {
		t.Fatalf("msgpack call: %v", err)
	}
}
