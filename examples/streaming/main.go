/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

// Command streaming demonstrates the four interaction patterns — unary,
// server-streaming, client-streaming, and bidirectional chat — over one
// connection, including credit-based flow control on a large server stream
// (every value is delivered losslessly, buffering stays bounded).
package main

import (
	"context"
	"fmt"
	"log"

	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valueclient"
	"go.arpabet.com/value-rpc/valuerpc"
	"go.arpabet.com/value-rpc/valueserver"
	"go.uber.org/zap"
)

func main() {
	srv, err := valueserver.NewMemServer("streaming-demo", zap.NewNop())
	if err != nil {
		log.Fatal(err)
	}
	defer srv.Close()

	// unary
	srv.AddFunction("upper", valuerpc.Any, valuerpc.Any,
		func(_ context.Context, a value.Value) (value.Value, error) {
			return value.Utf8("UPPER:" + a.(value.String).String()), nil
		})
	// server-stream: emit n values (drives credit-based flow control when n is large)
	srv.AddOutgoingStream("count", valuerpc.Any,
		func(_ context.Context, a value.Value) (<-chan value.Value, error) {
			n := int(a.(value.Number).Long())
			out := make(chan value.Value)
			go func() {
				defer close(out)
				for i := 0; i < n; i++ {
					out <- value.Long(int64(i))
				}
			}()
			return out, nil
		})
	// client-stream: sum everything the client sends
	sums := make(chan int64, 1)
	srv.AddIncomingStream("sum", valuerpc.Any,
		func(_ context.Context, _ value.Value, in <-chan value.Value) error {
			go func() {
				var total int64
				for v := range in {
					if v != nil {
						total += v.(value.Number).Long()
					}
				}
				sums <- total
			}()
			return nil
		})
	// chat: echo each message back, upper-cased
	srv.AddChat("shout", valuerpc.Any,
		func(_ context.Context, _ value.Value, in <-chan value.Value) (<-chan value.Value, error) {
			out := make(chan value.Value)
			go func() {
				defer close(out)
				for v := range in {
					out <- value.Utf8(v.(value.String).String() + "!")
				}
			}()
			return out, nil
		})
	go srv.Run()

	cli := valueclient.NewMemClient("streaming-demo", valueclient.WithStreamMaxPending(32))
	if err := cli.Connect(); err != nil {
		log.Fatal(err)
	}
	defer cli.Close()
	ctx := context.Background()

	// unary
	r, _ := cli.CallFunction(ctx, "upper", value.Utf8("hello"))
	fmt.Printf("unary:  %s\n", r.(value.String).String())

	// server-stream — 10,000 values, all delivered (lossless under flow control)
	const n = 10000
	readC, _, err := cli.GetStream(ctx, "count", value.Long(n), 16)
	if err != nil {
		log.Fatal(err)
	}
	got := 0
	for range readC {
		got++
	}
	fmt.Printf("server-stream: received %d of %d values\n", got, n)

	// client-stream
	putC := make(chan value.Value, 4)
	if err := cli.PutStream(ctx, "sum", nil, putC); err != nil {
		log.Fatal(err)
	}
	for i := int64(1); i <= 100; i++ {
		putC <- value.Long(i)
	}
	close(putC)
	fmt.Printf("client-stream: server summed 1..100 = %d\n", <-sums)

	// chat
	chatIn := make(chan value.Value, 4)
	chatOut, _, err := cli.Chat(ctx, "shout", nil, 8, chatIn)
	if err != nil {
		log.Fatal(err)
	}
	for _, m := range []string{"a", "b", "c"} {
		chatIn <- value.Utf8(m)
	}
	close(chatIn)
	fmt.Print("chat:  ")
	for v := range chatOut {
		fmt.Printf("%s ", v.(value.String).String())
	}
	fmt.Println()
}
