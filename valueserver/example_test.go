/*
 * Copyright (c) 2025 Karagatan LLC.
 * SPDX-License-Identifier: Apache-2.0
 */

package valueserver_test

import (
	"fmt"

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
		func(args value.Value) (value.Value, error) {
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

	res, err := cli.CallFunction("greet", value.Tuple(value.Utf8("world")))
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
		func(args value.Value) (<-chan value.Value, error) {
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

	readC, _, err := cli.GetStream("count", value.Tuple(value.Long(3)), 8)
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
		func(args value.Value, inC <-chan value.Value) (<-chan value.Value, error) {
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
	readC, _, err := cli.Chat("echo", nil, 8, sendC)
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
