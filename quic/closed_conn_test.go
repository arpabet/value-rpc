/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valuequic_test

import (
	"context"
	"crypto/tls"
	"io"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"go.arpabet.com/value"
	valuequic "go.arpabet.com/value-rpc/quic"
	"go.arpabet.com/value-rpc/valueclient"
	"go.arpabet.com/value-rpc/valuerpc"
	"go.arpabet.com/value-rpc/valueserver"
	"go.uber.org/zap"
)

// These tests are about a QUIC connection that ends while a reader waits on
// it. The stream readers fail when the connection goes, but ReadMessage waits
// on the funnel they feed, not on the connection: it has to be told, or a peer
// that only waits never learns that the other end is gone.

// quicEnds returns the two ends of one QUIC connection, as the transport gives
// them to the client and to the server: the dialed one and the accepted one.
func quicEnds(t *testing.T) (srv, cli valuerpc.MsgConn) {
	t.Helper()
	caPool, serverCert, _ := genCertPair(t)
	lis, err := valuequic.NewListener("127.0.0.1:0",
		&tls.Config{Certificates: []tls.Certificate{serverCert}}, time.Second)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { lis.Close() })

	accepted := make(chan valuerpc.MsgConn, 1)
	go func() {
		if c, err := lis.Accept(); err == nil {
			accepted <- c
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cli, err = valuequic.NewDialer(lis.Addr().String(), &tls.Config{RootCAs: caPool}, time.Second).Dial(ctx)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { cli.Close() })

	select {
	case srv = <-accepted:
	case <-time.After(5 * time.Second):
		t.Fatal("the listener accepted no connection")
	}
	t.Cleanup(func() { srv.Close() })
	return srv, cli
}

// readResult is what one ReadMessage came back with.
type readResult struct {
	msg value.Map
	err error
}

// readInBackground calls ReadMessage on its own goroutine, as the read loop of
// a client or of a server does.
func readInBackground(conn valuerpc.MsgConn) <-chan readResult {
	out := make(chan readResult, 1)
	go func() {
		m, err := conn.ReadMessage()
		out <- readResult{m, err}
	}()
	return out
}

// TestQUIC_ReadMessageEndsWhenThePeerClosesTheConnection: a reader that waits
// when the peer closes the connection is told, at once and with io.EOF, as a
// reader on a stream transport is. It used to wait for good.
func TestQUIC_ReadMessageEndsWhenThePeerClosesTheConnection(t *testing.T) {
	for _, tc := range []struct {
		name   string
		reader func(srv, cli valuerpc.MsgConn) (reads, closes valuerpc.MsgConn)
	}{
		{"the server closes", func(srv, cli valuerpc.MsgConn) (valuerpc.MsgConn, valuerpc.MsgConn) { return cli, srv }},
		{"the client closes", func(srv, cli valuerpc.MsgConn) (valuerpc.MsgConn, valuerpc.MsgConn) { return srv, cli }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, cli := quicEnds(t)
			reads, closes := tc.reader(srv, cli)

			got := readInBackground(reads)
			time.Sleep(50 * time.Millisecond) // the reader waits
			select {
			case r := <-got:
				t.Fatalf("ReadMessage returned before anything happened: %v, %v", r.msg, r.err)
			default:
			}

			start := time.Now()
			if err := closes.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}
			select {
			case r := <-got:
				if r.err != io.EOF {
					t.Fatalf("ReadMessage = %v, %v; want io.EOF for a peer that closed in good order", r.msg, r.err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("ReadMessage did not return when the peer closed the connection")
			}
			if d := time.Since(start); d > time.Second {
				t.Errorf("ReadMessage took %v to learn of the close", d)
			}

			// And it goes on saying so.
			select {
			case r := <-readInBackground(reads):
				if r.err != io.EOF {
					t.Fatalf("the next ReadMessage = %v, %v; want io.EOF", r.msg, r.err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("the next ReadMessage did not return")
			}
		})
	}
}

// TestQUIC_ReadMessageEndsWhenItsOwnConnectionIsClosed: closing a connection
// here ends its reader with io.EOF, as it always did.
func TestQUIC_ReadMessageEndsWhenItsOwnConnectionIsClosed(t *testing.T) {
	_, cli := quicEnds(t)
	got := readInBackground(cli)
	time.Sleep(20 * time.Millisecond)
	cli.Close()
	select {
	case r := <-got:
		if r.err != io.EOF {
			t.Fatalf("ReadMessage = %v, %v; want io.EOF", r.msg, r.err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("ReadMessage did not return when its connection was closed")
	}
	if _, err := cli.ReadMessage(); err != io.EOF {
		t.Fatalf("ReadMessage after Close = %v; want io.EOF", err)
	}
}

// TestQUIC_WhatWasReadBeforeTheEndIsHandedOverFirst: the end of the connection
// is reported after the messages its streams had read, not in place of them. A
// client has to see the refusal a server sends before it closes, and not an
// end of file that says nothing.
func TestQUIC_WhatWasReadBeforeTheEndIsHandedOverFirst(t *testing.T) {
	srv, cli := quicEnds(t)

	// Three requests of the server to its client, each on a stream of its own.
	// Nobody reads on the client yet: each stream reader holds its message.
	for rid := int64(-1); rid >= -3; rid-- {
		req := value.ImmutableMapOf(map[string]value.Value{
			valuerpc.DefaultDialect.MessageTypeField:  valuerpc.FunctionRequest.Long(),
			valuerpc.DefaultDialect.RequestIdField:    value.Long(rid),
			valuerpc.DefaultDialect.FunctionNameField: value.Utf8("notify"),
		})
		if err := srv.WriteMessage(req); err != nil {
			t.Fatalf("write %d: %v", rid, err)
		}
	}
	time.Sleep(200 * time.Millisecond) // they arrive and are read
	srv.Close()
	time.Sleep(100 * time.Millisecond) // the client's connection is gone

	var rids []int
	for i := 0; i < 3; i++ {
		select {
		case r := <-readInBackground(cli):
			if r.err != nil {
				t.Fatalf("message %d of 3: %v; the end came before what was read", i+1, r.err)
			}
			n, _ := valuerpc.GetNumberField(r.msg, valuerpc.DefaultDialect.RequestIdField)
			rids = append(rids, int(n.Long()))
		case <-time.After(3 * time.Second):
			t.Fatalf("message %d of 3 did not come", i+1)
		}
	}
	sort.Ints(rids)
	if len(rids) != 3 || rids[0] != -3 || rids[1] != -2 || rids[2] != -1 {
		t.Fatalf("the messages were of requests %v, want -3, -2, -1", rids)
	}
	select {
	case r := <-readInBackground(cli):
		if r.err != io.EOF {
			t.Fatalf("after the last message: %v, %v; want io.EOF", r.msg, r.err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the end was not reported after the last message")
	}
}

// TestQUIC_ClientWithoutCertificateIsRefusedAtOnce: a server that wants a
// certificate closes the connection of a client that shows none, right after
// the QUIC handshake, which the client has finished by then. The client used
// to wait for an answer to its vRPC handshake until its dial timeout was over,
// thirty seconds by default; it now fails when the connection goes.
func TestQUIC_ClientWithoutCertificateIsRefusedAtOnce(t *testing.T) {
	caPool, serverCert, _ := genCertPair(t)
	srv, err := valuequic.NewServer("127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientCAs:    caPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
	}, zap.NewNop())
	if err != nil {
		t.Fatalf("quic server: %v", err)
	}
	defer srv.Close()
	go srv.Run()

	cli := valuequic.NewClient(srv.Addr().String(), &tls.Config{RootCAs: caPool}) // no client certificate
	defer cli.Close()

	start := time.Now()
	err = cli.Connect() // the default dial timeout: thirty seconds
	took := time.Since(start)
	if err == nil {
		t.Fatal("a client without a certificate connected to a server that requires one")
	}
	if took > 5*time.Second {
		t.Fatalf("the refusal took %v to reach the client: %v", took, err)
	}
	t.Logf("refused after %v: %v", took, err)
}

// TestQUIC_IdleClientNoticesALostConnectionAndReconnects: a client that makes
// no call, as one that only serves what its server asks of it, learns that its
// connection was closed and connects again. Before, it would not have found
// out until it next wrote.
func TestQUIC_IdleClientNoticesALostConnectionAndReconnects(t *testing.T) {
	var conns atomic.Int32
	var last atomic.Value // valuerpc.MsgConn
	srv, cli := serverAuth(t, func(s valueserver.Server) {
		s.SetConnectAuthorizer(func(conn valuerpc.MsgConn) error {
			last.Store(conn)
			conns.Add(1)
			return nil
		})
		s.AddFunction("ping", valuerpc.Any, valuerpc.Any,
			func(_ context.Context, _ value.Value) (value.Value, error) { return value.Utf8("pong"), nil })
	})
	defer srv.Close()

	var handshakes atomic.Int32
	cli.SetConnectionHandler(func(value.Map) { handshakes.Add(1) })
	mustConnect(t, cli)
	defer cli.Close()
	if n := conns.Load(); n != 1 {
		t.Fatalf("the server has seen %d connections, want 1", n)
	}

	// The server closes the connection. The client makes no call.
	last.Load().(valuerpc.MsgConn).Close()

	deadline := time.Now().Add(5 * time.Second)
	for conns.Load() < 2 || handshakes.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("the idle client did not connect again: the server has seen %d connections, the client %d handshakes",
				conns.Load(), handshakes.Load())
		}
		time.Sleep(10 * time.Millisecond)
	}

	// And the connection it made is one to call on.
	callWhenReconnected(t, cli)
}

// callWhenReconnected makes a call, again while a reconnect is under way.
func callWhenReconnected(t *testing.T, cli valueclient.Client) {
	t.Helper()
	var err error
	for i := 0; i < 50; i++ {
		var res value.Value
		if res, err = cli.CallFunction(context.Background(), "ping", nil); err == nil {
			if got := res.(value.String).String(); got != "pong" {
				t.Fatalf("ping = %q", got)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("no call got through after the reconnect: %v", err)
}
