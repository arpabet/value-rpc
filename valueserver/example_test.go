/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valueserver_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valueclient"
	"go.arpabet.com/value-rpc/valuerpc"
	"go.arpabet.com/value-rpc/valueserver"
	"go.uber.org/zap"
)

// Example_unary shows a request/response call (the gRPC "unary" equivalent).
func Example_unary() {
	srv, err := valueserver.NewServer("127.0.0.1:0", zap.NewNop())
	if err != nil {
		panic(err)
	}
	defer srv.Close()

	srv.AddFunction("greet",
		valuerpc.List(valuerpc.String), // args: [name]
		valuerpc.String,                // result: string
		func(_ context.Context, args value.Value) (value.Value, error) {
			name := args.(value.List).GetStringAt(0).String()
			return value.Utf8("Hello, " + name + "!"), nil
		})
	go srv.Run()

	cli := valueclient.NewClient(srv.Addr().String(), "")
	if err := cli.Connect(); err != nil {
		panic(err)
	}
	defer cli.Close()
	cli.SetTimeout(5000)

	res, err := cli.CallFunction(context.Background(), "greet", value.Tuple(value.Utf8("world")))
	if err != nil {
		panic(err)
	}
	fmt.Println(res.(value.String).String())

	// Output:
	// Hello, world!
}

// Example_serverStreaming shows a server-streaming call (client gets a stream).
func Example_serverStreaming() {
	srv, err := valueserver.NewServer("127.0.0.1:0", zap.NewNop())
	if err != nil {
		panic(err)
	}
	defer srv.Close()

	srv.AddOutgoingStream("count", valuerpc.List(valuerpc.Number),
		func(_ context.Context, args value.Value) (<-chan value.Value, error) {
			n := args.(value.List).GetNumberAt(0).Long()
			out := make(chan value.Value)
			go func() {
				defer close(out) // closing the channel ends the stream
				for i := int64(1); i <= n; i++ {
					out <- value.Long(i)
				}
			}()
			return out, nil
		})
	go srv.Run()

	cli := valueclient.NewClient(srv.Addr().String(), "")
	if err := cli.Connect(); err != nil {
		panic(err)
	}
	defer cli.Close()
	cli.SetTimeout(5000)

	readC, _, err := cli.GetStream(context.Background(), "count", value.Tuple(value.Long(3)), 8)
	if err != nil {
		panic(err)
	}
	for v := range readC {
		if v == nil || v.Kind() == value.NULL {
			continue
		}
		fmt.Println(v.(value.Number).Long())
	}

	// Output:
	// 1
	// 2
	// 3
}

// Example_chat shows a bidirectional stream (the gRPC "bidi" equivalent).
func Example_chat() {
	srv, err := valueserver.NewServer("127.0.0.1:0", zap.NewNop())
	if err != nil {
		panic(err)
	}
	defer srv.Close()

	srv.AddChat("echo", valuerpc.Any,
		func(_ context.Context, args value.Value, inC <-chan value.Value) (<-chan value.Value, error) {
			out := make(chan value.Value)
			go func() {
				defer close(out)
				for msg := range inC {
					out <- value.Utf8("echo: " + msg.(value.String).String())
				}
			}()
			return out, nil
		})
	go srv.Run()

	cli := valueclient.NewClient(srv.Addr().String(), "")
	if err := cli.Connect(); err != nil {
		panic(err)
	}
	defer cli.Close()
	cli.SetTimeout(5000)

	sendC := make(chan value.Value, 2)
	readC, _, err := cli.Chat(context.Background(), "echo", nil, 8, sendC)
	if err != nil {
		panic(err)
	}

	go func() {
		sendC <- value.Utf8("one")
		sendC <- value.Utf8("two")
		close(sendC) // half-close the send side; we still receive every echo
	}()

	for v := range readC {
		if v == nil || v.Kind() == value.NULL {
			continue
		}
		fmt.Println(v.(value.String).String())
	}

	// Output:
	// echo: one
	// echo: two
}

// Example_unixSocket runs a unary call over a Unix domain socket — the same API,
// only the address changes.
func Example_unixSocket() {
	sock := filepath.Join(os.TempDir(), "vrpc-example.sock")

	srv, err := valueserver.NewUnixServer(sock, zap.NewNop())
	if err != nil {
		panic(err)
	}
	defer srv.Close()
	defer os.Remove(sock)

	srv.AddFunction("greet", valuerpc.List(valuerpc.String), valuerpc.String,
		func(_ context.Context, args value.Value) (value.Value, error) {
			return value.Utf8("Hello, " + args.(value.List).GetStringAt(0).String() + "!"), nil
		})
	go srv.Run()

	cli := valueclient.NewUnixClient(sock)
	if err := cli.Connect(); err != nil {
		panic(err)
	}
	defer cli.Close()
	cli.SetTimeout(5000)

	res, err := cli.CallFunction(context.Background(), "greet", value.Tuple(value.Utf8("unix")))
	if err != nil {
		panic(err)
	}
	fmt.Println(res.(value.String).String())

	// Output:
	// Hello, unix!
}

// Example_webSocket runs a unary call over WebSocket (one MessagePack message
// per binary frame). Use NewWebSocketHandler instead to share a port with other
// HTTP routes or to serve wss:// from your own TLS server.
func Example_webSocket() {
	srv, err := valueserver.NewWebSocketServer("127.0.0.1:0", "/rpc", zap.NewNop())
	if err != nil {
		panic(err)
	}
	defer srv.Close()

	srv.AddFunction("greet", valuerpc.List(valuerpc.String), valuerpc.String,
		func(_ context.Context, args value.Value) (value.Value, error) {
			return value.Utf8("Hello, " + args.(value.List).GetStringAt(0).String() + "!"), nil
		})
	go srv.Run()

	cli := valueclient.NewWebSocketClient("ws://" + srv.Addr().String() + "/rpc")
	if err := cli.Connect(); err != nil {
		panic(err)
	}
	defer cli.Close()
	cli.SetTimeout(5000)

	res, err := cli.CallFunction(context.Background(), "greet", value.Tuple(value.Utf8("websocket")))
	if err != nil {
		panic(err)
	}
	fmt.Println(res.(value.String).String())

	// Output:
	// Hello, websocket!
}

// Example_inMemory wires a client and server together in one process over the
// "mem://" transport (no sockets, no serialization). Switching to "tcp://host:port"
// later needs no other code changes.
func Example_inMemory() {
	srv, err := valueserver.NewMemServer("billing", zap.NewNop())
	if err != nil {
		panic(err)
	}
	defer srv.Close()

	srv.AddFunction("greet", valuerpc.List(valuerpc.String), valuerpc.String,
		func(_ context.Context, args value.Value) (value.Value, error) {
			return value.Utf8("Hello, " + args.(value.List).GetStringAt(0).String() + "!"), nil
		})
	go srv.Run()

	cli := valueclient.NewMemClient("billing")
	if err := cli.Connect(); err != nil {
		panic(err)
	}
	defer cli.Close()
	cli.SetTimeout(5000)

	res, err := cli.CallFunction(context.Background(), "greet", value.Tuple(value.Utf8("in-process")))
	if err != nil {
		panic(err)
	}
	fmt.Println(res.(value.String).String())

	// Output:
	// Hello, in-process!
}
