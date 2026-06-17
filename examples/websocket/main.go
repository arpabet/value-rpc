/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

// Command websocket demonstrates the WebSocket transport as an embeddable
// http.Handler: a single http.Server serves a normal HTTP route and vRPC over
// WebSocket on the same port (so vRPC can ride existing HTTP infrastructure —
// TLS termination, reverse proxies, browser clients).
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"

	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valueclient"
	"go.arpabet.com/value-rpc/valuerpc"
	"go.arpabet.com/value-rpc/valueserver"
	"go.uber.org/zap"
)

func main() {
	// vRPC server exposed as an http.Handler (no listener of its own).
	rpc, handler, err := valueserver.NewWebSocketHandler(zap.NewNop())
	if err != nil {
		log.Fatal(err)
	}
	defer rpc.Close()
	rpc.AddFunction("echo", valuerpc.Any, valuerpc.Any,
		func(_ context.Context, a value.Value) (value.Value, error) { return a, nil })
	go rpc.Run()

	// Mount it alongside a plain HTTP route on one mux / one port.
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	mux.Handle("/rpc", handler)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	httpSrv := &http.Server{Handler: mux}
	go httpSrv.Serve(lis)
	defer httpSrv.Close()
	addr := lis.Addr().String()

	// The same port serves plain HTTP ...
	resp, err := http.Get("http://" + addr + "/healthz")
	if err != nil {
		log.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	fmt.Printf("GET /healthz -> %s\n", body)

	// ... and vRPC over WebSocket.
	cli := valueclient.NewWebSocketClient("ws://" + addr + "/rpc")
	if err := cli.Connect(); err != nil {
		log.Fatal(err)
	}
	defer cli.Close()
	r, err := cli.CallFunction(context.Background(), "echo", value.Utf8("over websocket"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("RPC /rpc echo -> %s\n", r.(value.String).String())
}
