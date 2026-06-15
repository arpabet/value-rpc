/*
 * Copyright (c) 2025 Karagatan LLC.
 * SPDX-License-Identifier: Apache-2.0
 */

package valueserver_test

import (
	"crypto/tls"
	"fmt"
	"testing"
	"time"

	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valueclient"
	"go.arpabet.com/value-rpc/valuerpc"
	"go.arpabet.com/value-rpc/valueserver"
	"go.uber.org/zap"
)

func TestQUIC_RoundTrip(t *testing.T) {
	caPool, serverCert, _ := genCertPair(t)

	srv, err := valueserver.NewQUICServer("127.0.0.1:0",
		&tls.Config{Certificates: []tls.Certificate{serverCert}}, zap.NewNop())
	if err != nil {
		t.Fatalf("quic server: %v", err)
	}
	defer srv.Close()
	srv.AddFunction("echo", valuerpc.List(valuerpc.String), valuerpc.String,
		func(args value.Value) (value.Value, error) {
			return value.Utf8("q:" + args.(value.List).GetStringAt(0).String()), nil
		})
	go srv.Run()

	cli := valueclient.NewQUICClient(srv.Addr().String(), &tls.Config{RootCAs: caPool})
	if err := cli.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer cli.Close()
	cli.SetTimeout(5000)

	res, err := cli.CallFunction("echo", value.Tuple(value.Utf8("hi")))
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if got := res.(value.String).String(); got != "q:hi" {
		t.Fatalf("result = %q, want %q", got, "q:hi")
	}
}

// TestQUIC_MutualAuth verifies mTLS over QUIC and that the verified client
// certificate is reachable from a connect-authorizer via PeerCertificates.
func TestQUIC_MutualAuth(t *testing.T) {
	caPool, serverCert, clientCert := genCertPair(t)

	srv, err := valueserver.NewQUICServer("127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientCAs:    caPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
	}, zap.NewNop())
	if err != nil {
		t.Fatalf("quic server: %v", err)
	}
	defer srv.Close()

	cnCh := make(chan string, 1)
	srv.SetConnectAuthorizer(func(conn valuerpc.MsgConn) error {
		certs, ok := valuerpc.PeerCertificates(conn)
		if !ok {
			return fmt.Errorf("no client certificate")
		}
		select {
		case cnCh <- certs[0].Subject.CommonName:
		default:
		}
		return nil
	})
	srv.AddFunction("ping", valuerpc.Void, valuerpc.String,
		func(args value.Value) (value.Value, error) { return value.Utf8("pong"), nil })
	go srv.Run()

	cli := valueclient.NewQUICClient(srv.Addr().String(), &tls.Config{
		RootCAs:      caPool,
		Certificates: []tls.Certificate{clientCert},
	})
	if err := cli.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer cli.Close()
	cli.SetTimeout(5000)

	if _, err := cli.CallFunction("ping", nil); err != nil {
		t.Fatalf("call: %v", err)
	}
	select {
	case cn := <-cnCh:
		if cn != "vrpc-test-client" {
			t.Errorf("client cert CN = %q, want %q", cn, "vrpc-test-client")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("authorizer never observed the client certificate")
	}
}

// TestQUIC_StreamsAreFreed runs many sequential calls with a tiny per-connection
// stream cap: if finished requests' streams were not freed, the cap would be hit
// and calls would block/time out.
func TestQUIC_StreamsAreFreed(t *testing.T) {
	oldMax := valuerpc.QUICMaxStreams
	valuerpc.QUICMaxStreams = 8
	defer func() { valuerpc.QUICMaxStreams = oldMax }()

	caPool, serverCert, _ := genCertPair(t)
	srv, err := valueserver.NewQUICServer("127.0.0.1:0",
		&tls.Config{Certificates: []tls.Certificate{serverCert}}, zap.NewNop())
	if err != nil {
		t.Fatalf("quic server: %v", err)
	}
	defer srv.Close()
	srv.AddFunction("inc", valuerpc.List(valuerpc.Number), valuerpc.Number,
		func(args value.Value) (value.Value, error) {
			return value.Long(args.(value.List).GetNumberAt(0).Long() + 1), nil
		})
	go srv.Run()

	cli := valueclient.NewQUICClient(srv.Addr().String(), &tls.Config{RootCAs: caPool})
	if err := cli.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer cli.Close()
	cli.SetTimeout(3000)

	const calls = 50 // far more than the 8-stream cap
	for i := int64(0); i < calls; i++ {
		res, err := cli.CallFunction("inc", value.Tuple(value.Long(i)))
		if err != nil {
			t.Fatalf("call %d failed (stream leak would block here): %v", i, err)
		}
		if got := res.(value.Number).Long(); got != i+1 {
			t.Fatalf("inc(%d) = %d, want %d", i, got, i+1)
		}
	}
}
