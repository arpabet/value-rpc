/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

// Command reconnect demonstrates the client reconnect policy: in-flight requests
// fail fast with ErrConnectionLost (CodeUnavailable) when the connection drops,
// and the client re-establishes the connection on its own with exponential
// backoff after an outage.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valueclient"
	"go.arpabet.com/value-rpc/valuerpc"
	"go.arpabet.com/value-rpc/valueserver"
	"go.uber.org/zap"
)

func main() {
	sock := filepath.Join(os.TempDir(), fmt.Sprintf("vrpc-reconnect-%d.sock", time.Now().UnixNano()))
	defer os.Remove(sock)

	hold := make(chan struct{}) // blocks the "hold" handler until released
	defer close(hold)

	newSrv := func() valueserver.Server {
		s, err := valueserver.NewUnixServer(sock, zap.NewNop())
		if err != nil {
			log.Fatal(err)
		}
		s.AddFunction("ping", valuerpc.Any, valuerpc.Any,
			func(_ context.Context, _ value.Value) (value.Value, error) { return value.Utf8("pong"), nil })
		s.AddFunction("hold", valuerpc.Any, valuerpc.Any,
			func(ctx context.Context, _ value.Value) (value.Value, error) {
				select {
				case <-hold:
				case <-ctx.Done():
				}
				return value.Utf8("late"), nil
			})
		go s.Run()
		return s
	}

	srvA := newSrv()

	cli := valueclient.NewUnixClient(sock, valueclient.WithReconnectPolicy(valueclient.ReconnectPolicy{
		// Auto-reconnect with exponential backoff after a drop.
		InitialBackoff: 20 * time.Millisecond,
		MaxBackoff:     200 * time.Millisecond,
		MaxAttempts:    -1, // retry until the server returns
		Jitter:         true,
		// "ping" is idempotent, so replay it across a reconnect instead of failing it.
		ReplayUnary: func(method string) bool { return method == "ping" },
	}))
	if err := cli.Connect(); err != nil {
		log.Fatal(err)
	}
	defer cli.Close()
	ctx := context.Background()

	r, _ := cli.CallFunction(ctx, "ping", nil)
	fmt.Printf("1) normal call: ping -> %s\n", r.(value.String).String())

	// --- fail-fast: an in-flight non-idempotent call is failed on a drop ---------
	fmt.Println("2) fail-fast: starting a long 'hold' call, then forcing a reconnect")
	errc := make(chan error, 1)
	go func() {
		_, err := cli.CallFunction(ctx, "hold", nil)
		errc <- err
	}()
	time.Sleep(100 * time.Millisecond) // let the call reach the server
	cli.Reconnect()                    // drop + re-establish
	err := <-errc
	fmt.Printf("   in-flight 'hold' returned code=%v (%v)\n", valuerpc.CodeOf(err), err)

	// --- backoff: survive a server outage and reconnect automatically -----------
	fmt.Println("3) backoff: stopping the server, then bringing it back up")
	srvA.Close()
	time.Sleep(150 * time.Millisecond) // outage
	srvB := newSrv()
	defer srvB.Close()

	deadline := time.After(3 * time.Second)
	for !cli.IsActive() {
		select {
		case <-deadline:
			log.Fatal("client did not auto-reconnect")
		case <-time.After(10 * time.Millisecond):
		}
	}
	fmt.Println("   client auto-reconnected via backoff")
	r, _ = cli.CallFunction(ctx, "ping", nil)
	fmt.Printf("   call after reconnect: ping -> %s\n", r.(value.String).String())
}
