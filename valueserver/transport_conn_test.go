/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valueserver_test

import (
	"net"
	"testing"

	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valueclient"
	"go.arpabet.com/value-rpc/valuerpc"
	"go.arpabet.com/value-rpc/valueserver"
	"go.uber.org/zap"
)

// outOfBandPair returns two connected net.Conns established independently of
// value-rpc, standing in for a connection produced by an obfuscation layer or a
// rendezvous broker (a real socket, not a synchronous net.Pipe).
func outOfBandPair(t *testing.T) (serverConn, clientConn net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	type res struct {
		c   net.Conn
		err error
	}
	accepted := make(chan res, 1)
	go func() {
		c, err := ln.Accept()
		accepted <- res{c, err}
	}()

	clientConn, err = net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	r := <-accepted
	if r.err != nil {
		t.Fatalf("accept: %v", r.err)
	}
	return r.c, clientConn
}

// TestSeam_SingleConn_RoundTrip proves the bring-your-own-connection seam end to
// end: a connection established out of band carries a full server<->client RPC
// (handshake + unary call) without value-rpc dialing or accepting anything itself.
// This is the pattern servion uses to hand value-rpc an obfuscated or broker conn.
func TestSeam_SingleConn_RoundTrip(t *testing.T) {
	serverConn, clientConn := outOfBandPair(t)

	lis := valuerpc.NewSingleConnListener(serverConn, nil, valueserver.DefaultTimeout)
	srv, err := valueserver.NewServerWithListener(lis, zap.NewNop())
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	defer srv.Close()
	srv.AddFunction("echo", valuerpc.List(valuerpc.String), valuerpc.String,
		func(args value.Value) (value.Value, error) {
			return value.Utf8("t:" + args.(value.List).GetStringAt(0).String()), nil
		})
	go srv.Run()

	dialer := valuerpc.NewSingleConnDialer(clientConn, valueclient.DefaultTimeout)
	cli := valueclient.NewClientWithDialer(dialer)
	if err := cli.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer cli.Close()
	cli.SetTimeout(5000)

	res, err := cli.CallFunction("echo", value.Tuple(value.Utf8("hi")))
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if got := res.(value.String).String(); got != "t:hi" {
		t.Fatalf("result = %q, want %q", got, "t:hi")
	}
}
