/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valueserver_test

import (
	"net"
	"net/http"
	"strings"
	"testing"

	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valueclient"
	"go.arpabet.com/value-rpc/valuerpc"
	"go.arpabet.com/value-rpc/valueserver"
	"go.uber.org/zap"
)

// newWSServer starts a standalone WebSocket server on an ephemeral port and
// returns it plus the ws:// URL to dial.
func newWSServer(t testing.TB, path string, setup func(s valueserver.Server)) (valueserver.Server, string) {
	t.Helper()
	srv, err := valueserver.NewWebSocketServer("127.0.0.1:0", path, zap.NewNop())
	if err != nil {
		t.Fatalf("new websocket server: %v", err)
	}
	setup(srv)
	go srv.Run()
	return srv, "ws://" + srv.Addr().String() + path
}

func TestWebSocket_Unary(t *testing.T) {
	srv, url := newWSServer(t, "/rpc", func(s valueserver.Server) {
		s.AddFunction("echo", valuerpc.List(valuerpc.String), valuerpc.String,
			func(args value.Value) (value.Value, error) {
				return value.Utf8("ws:" + args.(value.List).GetStringAt(0).String()), nil
			})
	})
	defer srv.Close()

	cli := valueclient.NewWebSocketClient(url)
	if err := cli.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer cli.Close()
	cli.SetTimeout(5000)

	res, err := cli.CallFunction("echo", value.Tuple(value.Utf8("hi")))
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if got := res.(value.String).String(); got != "ws:hi" {
		t.Fatalf("result = %q, want %q", got, "ws:hi")
	}
}

func TestWebSocket_ServerStream(t *testing.T) {
	srv, url := newWSServer(t, "/rpc", func(s valueserver.Server) {
		s.AddOutgoingStream("count", valuerpc.List(valuerpc.Number),
			func(args value.Value) (<-chan value.Value, error) {
				n := args.(value.List).GetNumberAt(0).Long()
				out := make(chan value.Value)
				go func() {
					defer close(out)
					for i := int64(0); i < n; i++ {
						out <- value.Long(i)
					}
				}()
				return out, nil
			})
	})
	defer srv.Close()

	cli := valueclient.NewWebSocketClient(url)
	if err := cli.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer cli.Close()
	cli.SetTimeout(5000)

	readC, _, err := cli.GetStream("count", value.Tuple(value.Long(5)), 16)
	if err != nil {
		t.Fatalf("get stream: %v", err)
	}
	var got int
	for v := range readC {
		if v == nil || v.Kind() == value.NULL {
			t.Fatalf("BUG-4: phantom Null on websocket stream")
		}
		if v.(value.Number).Long() != int64(got) {
			t.Fatalf("value %d out of order: %d", got, v.(value.Number).Long())
		}
		got++
	}
	if got != 5 {
		t.Fatalf("received %d values, want 5", got)
	}
}

func TestWebSocket_Chat(t *testing.T) {
	srv, url := newWSServer(t, "/rpc", func(s valueserver.Server) {
		s.AddChat("echo", valuerpc.Any,
			func(args value.Value, inC <-chan value.Value) (<-chan value.Value, error) {
				out := make(chan value.Value)
				go func() {
					defer close(out)
					for v := range inC {
						out <- value.Utf8("echo:" + v.(value.String).String())
					}
				}()
				return out, nil
			})
	})
	defer srv.Close()

	cli := valueclient.NewWebSocketClient(url)
	if err := cli.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer cli.Close()
	cli.SetTimeout(5000)

	sendC := make(chan value.Value, 4)
	readC, _, err := cli.Chat("echo", nil, 16, sendC)
	if err != nil {
		t.Fatalf("chat: %v", err)
	}
	inputs := []string{"a", "bb", "ccc"}
	go func() {
		for _, s := range inputs {
			sendC <- value.Utf8(s)
		}
		close(sendC)
	}()
	var got []string
	for v := range readC {
		if v == nil || v.Kind() == value.NULL {
			continue
		}
		got = append(got, v.(value.String).String())
	}
	if len(got) != len(inputs) {
		t.Fatalf("received %d echoes %v, want %d", len(got), got, len(inputs))
	}
	for i, s := range inputs {
		if want := "echo:" + s; got[i] != want {
			t.Fatalf("echo[%d] = %q, want %q", i, got[i], want)
		}
	}
}

// TestWebSocket_SchemeViaNewServer drives the ws:// scheme through the plain
// NewServer / NewClient constructors.
func TestWebSocket_SchemeViaNewServer(t *testing.T) {
	srv, err := valueserver.NewServer("ws://127.0.0.1:0/rpc", zap.NewNop())
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	defer srv.Close()
	srv.AddFunction("ping", valuerpc.Void, valuerpc.String,
		func(args value.Value) (value.Value, error) {
			return value.Utf8("pong"), nil
		})
	go srv.Run()

	cli := valueclient.NewClient("ws://"+srv.Addr().String()+"/rpc", "")
	if err := cli.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer cli.Close()
	cli.SetTimeout(5000)

	res, err := cli.CallFunction("ping", nil)
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if got := res.(value.String).String(); got != "pong" {
		t.Fatalf("result = %q, want %q", got, "pong")
	}
}

// TestWebSocket_EmbeddedHandler mounts the vRPC handler on a user-owned
// http.Server alongside another route (port sharing).
func TestWebSocket_EmbeddedHandler(t *testing.T) {
	srv, h, err := valueserver.NewWebSocketHandler(zap.NewNop())
	if err != nil {
		t.Fatalf("new ws handler: %v", err)
	}
	defer srv.Close()
	srv.AddFunction("echo", valuerpc.List(valuerpc.String), valuerpc.String,
		func(args value.Value) (value.Value, error) {
			return value.Utf8(args.(value.List).GetStringAt(0).String()), nil
		})
	go srv.Run()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })
	mux.Handle("/rpc", h)

	httpLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	httpSrv := &http.Server{Handler: mux}
	go httpSrv.Serve(httpLis)
	defer httpSrv.Close()

	cli := valueclient.NewWebSocketClient("ws://" + httpLis.Addr().String() + "/rpc")
	if err := cli.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer cli.Close()
	cli.SetTimeout(5000)

	res, err := cli.CallFunction("echo", value.Tuple(value.Utf8("shared-port")))
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if got := res.(value.String).String(); got != "shared-port" {
		t.Fatalf("result = %q, want %q", got, "shared-port")
	}
}

// TestWebSocket_MaxFrameSize verifies that MaxFrameSize is enforced on the
// WebSocket read limit: a payload over the limit is rejected by the server.
func TestWebSocket_MaxFrameSize(t *testing.T) {
	old := valuerpc.MaxFrameSize
	valuerpc.MaxFrameSize = 512
	defer func() { valuerpc.MaxFrameSize = old }()

	srv, url := newWSServer(t, "/rpc", func(s valueserver.Server) {
		s.AddFunction("recv", valuerpc.Any, valuerpc.String,
			func(args value.Value) (value.Value, error) {
				return value.Utf8("ok"), nil
			})
	})
	defer srv.Close()

	cli := valueclient.NewWebSocketClient(url) // SetReadLimit(512) applied on connect
	if err := cli.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer cli.Close()
	cli.SetTimeout(600)

	// A small message round-trips.
	if _, err := cli.CallFunction("recv", value.Tuple(value.Utf8("small"))); err != nil {
		t.Fatalf("small call should succeed: %v", err)
	}
	// A message over the read limit must be rejected (server drops the frame).
	big := strings.Repeat("x", 4096)
	if _, err := cli.CallFunction("recv", value.Tuple(value.Utf8(big))); err == nil {
		t.Fatal("expected an over-limit websocket message to be rejected")
	}
}

func BenchmarkWebSocketUnary(b *testing.B) {
	srv, url := newWSServer(b, "/rpc", func(s valueserver.Server) {
		s.AddFunction("noop", valuerpc.List(valuerpc.Number), valuerpc.Number,
			func(args value.Value) (value.Value, error) {
				return args.(value.List).GetNumberAt(0), nil
			})
	})
	defer srv.Close()

	cli := valueclient.NewWebSocketClient(url)
	if err := cli.Connect(); err != nil {
		b.Fatalf("connect: %v", err)
	}
	defer cli.Close()
	cli.SetTimeout(10000)

	arg := value.Tuple(value.Long(1))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := cli.CallFunction("noop", arg); err != nil {
			b.Fatal(err)
		}
	}
}
