/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valueserver_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valueclient"
	"go.arpabet.com/value-rpc/valuerpc"
	"go.arpabet.com/value-rpc/valueserver"
)

// TestAuthenticatorGatesHandshake verifies the handshake Authenticator: a client
// with the wrong/absent credential is rejected, and one with the correct
// credential is accepted and can call.
func TestAuthenticatorGatesHandshake(t *testing.T) {
	const secret = "s3cret"
	addr, stop := newServer(t, func(s valueserver.Server) {
		s.SetAuthenticator(func(conn valuerpc.MsgConn, cred value.Value) error {
			if cred == nil || cred.Kind() != value.STRING || cred.(value.String).String() != secret {
				return fmt.Errorf("bad credential")
			}
			return nil
		})
		s.AddFunction("ping", valuerpc.Any, valuerpc.Any,
			func(_ context.Context, args value.Value) (value.Value, error) {
				return value.Utf8("pong"), nil
			})
	})
	defer stop()

	// No credential -> the handshake is rejected, so calls fail.
	bad := valueclient.NewClient(addr, "")
	bad.SetTimeout(500)
	defer bad.Close()
	_ = bad.Connect()
	if _, err := bad.CallFunction(context.Background(), "ping", nil); err == nil {
		t.Fatal("call without the required credential should fail")
	}

	// Correct credential -> accepted.
	good := valueclient.NewClient(addr, "")
	good.SetCredential(value.Utf8(secret))
	defer good.Close()
	if err := good.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	res, err := good.CallFunction(context.Background(), "ping", nil)
	if err != nil {
		t.Fatalf("authenticated call failed: %v", err)
	}
	if got := res.(value.String).String(); got != "pong" {
		t.Fatalf("got %q, want pong", got)
	}
}

// rawHandshake dials a bare connection, sends a handshake for clientId carrying
// token, and returns the server's reply (or an error if the server rejected and
// closed the connection). It lets a test impersonate a peer at the wire level.
func rawHandshake(t *testing.T, addr string, clientId int64, token string) (valuerpc.MsgConn, value.Map, error) {
	t.Helper()
	dialer := valuerpc.NewDialer(addr, "", valueclient.KeepAlivePeriod, valueclient.DefaultTimeout, valuerpc.MaxFrameSize)
	conn, err := dialer.Dial()
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if err := conn.WriteMessage(valuerpc.NewHandshakeRequest(clientId, token)); err != nil {
		conn.Close()
		t.Fatalf("write handshake: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	resp, err := conn.ReadMessage()
	return conn, resp, err
}

// TestSessionResumptionRequiresToken is the regression test for the cid
// session-hijack fix. A first handshake (no token) is accepted and the server
// issues a session token; resuming the same clientId is allowed only with that
// exact token. A peer that reuses the clientId with a wrong token, or with no
// token, is rejected — so it cannot take over (and close) another client's
// session by guessing its id.
func TestSessionResumptionRequiresToken(t *testing.T) {
	addr, stop := newServer(t, func(s valueserver.Server) {
		s.AddFunction("ping", valuerpc.Any, valuerpc.Any,
			func(_ context.Context, args value.Value) (value.Value, error) { return value.Utf8("pong"), nil })
	})
	defer stop()

	const cid = int64(0xABCDEF)

	// First connect: no token yet -> accepted, server mints one.
	c1, resp, err := rawHandshake(t, addr, cid, "")
	if err != nil {
		t.Fatalf("first handshake rejected: %v", err)
	}
	defer c1.Close()
	tok, ok := valuerpc.GetStringField(resp, valuerpc.SessionTokenField)
	if !ok || tok.String() == "" {
		t.Fatal("server did not issue a session token on first handshake")
	}

	// Resume with the correct token -> accepted (legitimate reconnect).
	c2, resp2, err := rawHandshake(t, addr, cid, tok.String())
	if err != nil {
		t.Fatalf("resume with correct token was rejected: %v", err)
	}
	defer c2.Close()
	if mt, ok := valuerpc.GetNumberField(resp2, valuerpc.MessageTypeField); !ok || valuerpc.MessageType(mt.Long()) != valuerpc.HandshakeResponse {
		t.Fatal("resume with correct token did not get a handshake response")
	}

	// Hijack attempt: same cid, wrong token -> rejected (server closes the conn).
	if c3, _, err := rawHandshake(t, addr, cid, "deadbeefdeadbeefdeadbeefdeadbeef"); err == nil {
		c3.Close()
		t.Fatal("server accepted a handshake with the wrong session token (hijack)")
	}

	// Hijack attempt: same cid, no token -> rejected.
	if c4, _, err := rawHandshake(t, addr, cid, ""); err == nil {
		c4.Close()
		t.Fatal("server accepted a handshake reusing an existing client id with no token (hijack)")
	}
}

// TestHijackAttemptLeavesVictimIntact checks that a rejected impersonation does
// not disturb the real client's live session: the victim keeps working.
func TestHijackAttemptLeavesVictimIntact(t *testing.T) {
	addr, stop := newServer(t, func(s valueserver.Server) {
		s.AddFunction("ping", valuerpc.Any, valuerpc.Any,
			func(_ context.Context, args value.Value) (value.Value, error) { return value.Utf8("pong"), nil })
	})
	defer stop()

	victim := dialClient(t, addr)
	defer victim.Close()
	if _, err := victim.CallFunction(context.Background(), "ping", nil); err != nil {
		t.Fatalf("victim call: %v", err)
	}

	// Attacker reuses the victim's clientId with no token: must be rejected.
	if c, _, err := rawHandshake(t, addr, victim.ClientId(), ""); err == nil {
		c.Close()
		t.Fatal("server accepted an impersonating handshake without the session token")
	}

	// The victim's session must be unaffected and still usable.
	res, err := victim.CallFunction(context.Background(), "ping", nil)
	if err != nil {
		t.Fatalf("victim call after hijack attempt failed (session disturbed): %v", err)
	}
	if got := res.(value.String).String(); got != "pong" {
		t.Fatalf("victim got %q, want pong", got)
	}
}
