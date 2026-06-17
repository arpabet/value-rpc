/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

// Command customtransport demonstrates the "bring your own connection" seam:
// NewFuncDialer / NewAcceptListener let vRPC run over any io.ReadWriteCloser, so
// you can interpose your own byte-stream layer (compression, obfuscation,
// tunnels, WebRTC data channels) without changing the RPC code. Here the
// interposed layer is a trivial byte counter wrapping a TCP connection.
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"sync/atomic"

	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valueclient"
	"go.arpabet.com/value-rpc/valuerpc"
	"go.arpabet.com/value-rpc/valueserver"
	"go.uber.org/zap"
)

// countingConn wraps a net.Conn and tallies bytes — a stand-in for whatever
// custom stream processing you want to interpose (this is where an obfuscator or
// compressor would live).
type countingConn struct {
	net.Conn
	in, out *atomic.Int64
}

func (c countingConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.in.Add(int64(n))
	return n, err
}

func (c countingConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	c.out.Add(int64(n))
	return n, err
}

func main() {
	var clientIn, clientOut atomic.Int64

	// --- server over a custom Listener built from a raw net.Listener -----------
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	addr := lis.Addr().String()

	transport := valuerpc.NewAcceptListener(
		func() (io.ReadWriteCloser, error) { return lis.Accept() }, // raw conns; vRPC frames them
		lis.Addr(),
		lis.Close,
		valueserver.DefaultTimeout,
	)
	srv, err := valueserver.NewServerWithListener(transport, zap.NewNop())
	if err != nil {
		log.Fatal(err)
	}
	defer srv.Close()
	srv.AddFunction("echo", valuerpc.Any, valuerpc.Any,
		func(_ context.Context, a value.Value) (value.Value, error) { return a, nil })
	go srv.Run()

	// --- client over a custom Dialer that wraps each dialed conn ---------------
	dialer := valuerpc.NewFuncDialer(func(ctx context.Context) (io.ReadWriteCloser, error) {
		var d net.Dialer
		c, err := d.DialContext(ctx, "tcp", addr)
		if err != nil {
			return nil, err
		}
		return countingConn{Conn: c, in: &clientIn, out: &clientOut}, nil
	}, valueclient.DefaultTimeout)

	cli := valueclient.NewClientWithDialer(dialer)
	if err := cli.Connect(); err != nil {
		log.Fatal(err)
	}
	defer cli.Close()

	r, err := cli.CallFunction(context.Background(), "echo", value.Utf8("through a custom transport"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("echo -> %s\n", r.(value.String).String())
	fmt.Printf("custom transport observed: client sent %d bytes, received %d bytes\n",
		clientOut.Load(), clientIn.Load())
}
