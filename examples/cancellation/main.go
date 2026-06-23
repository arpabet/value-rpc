/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

// Command cancellation demonstrates context propagation: the client's context
// deadline is sent as the request SLA, so a slow server handler observes the
// deadline (and an explicit cancel) on its own ctx and can abandon work early.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valueclient"
	"go.arpabet.com/value-rpc/valuerpc"
	"go.arpabet.com/value-rpc/valueserver"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
)

// The deadline is sent as the server SLA, so both ends expire together: the
// client's local ctx.Done usually wins (a raw context error), but the server's
// SLA response can arrive first (a coded *valuerpc.Error). Either is a deadline.
func isDeadline(err error) bool {
	return xerrors.Is(err, context.DeadlineExceeded) || valuerpc.CodeOf(err) == valuerpc.CodeDeadlineExceeded
}

func main() {
	srv, err := valueserver.NewMemServer("cancel-demo", zap.NewNop())
	if err != nil {
		log.Fatal(err)
	}
	defer srv.Close()

	// A handler that does slow work, but bails as soon as its context is done.
	srv.AddFunction("slow", valuerpc.Any, valuerpc.Any,
		func(ctx context.Context, _ value.Value) (value.Value, error) {
			select {
			case <-time.After(5 * time.Second):
				return value.Utf8("finished"), nil
			case <-ctx.Done():
				fmt.Printf("  server: handler abandoned work (%v)\n", ctx.Err())
				return nil, ctx.Err()
			}
		})
	go srv.Run()

	cli := valueclient.NewMemClient("cancel-demo")
	if err := cli.Connect(); err != nil {
		log.Fatal(err)
	}
	defer cli.Close()

	// 1) Deadline: the call carries a 200ms deadline; both ends give up at ~200ms.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = cli.CallFunction(ctx, "slow", nil)
	fmt.Printf("deadline call returned after %v, deadlineExceeded=%v\n",
		time.Since(start).Round(10*time.Millisecond), isDeadline(err))

	// 2) Explicit cancel from another goroutine.
	ctx2, cancel2 := context.WithCancel(context.Background())
	go func() { time.Sleep(150 * time.Millisecond); cancel2() }()
	start = time.Now()
	_, err = cli.CallFunction(ctx2, "slow", nil)
	fmt.Printf("cancelled call returned after %v (err: %v)\n",
		time.Since(start).Round(10*time.Millisecond), err != nil)
}
