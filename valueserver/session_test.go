/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valueserver_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valueclient"
	"go.arpabet.com/value-rpc/valuerpc"
	"go.arpabet.com/value-rpc/valueserver"
	"golang.org/x/xerrors"
)

// TestAuthenticatorGatesHandshake verifies the handshake Authenticator: a client
// with the wrong/absent credential is rejected, and one with the correct
// credential is accepted and can call.
func TestAuthenticatorGatesHandshake(t *testing.T) {
	const secret = "s3cret"
	addr, stop := newServer(t, func(s valueserver.Server) {
		s.SetAuthenticator(func(conn valuerpc.MsgConn, cred value.Value) (string, error) {
			if cred == nil || cred.Kind() != value.STRING || cred.(value.String).String() != secret {
				return "", xerrors.New("bad credential")
			}
			return "user", nil
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
	conn, err := dialer.Dial(context.Background())
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
// session-hijack fix, updated for reverse-hash-chain resumption. A first handshake
// presents the chain anchor and is accepted; resuming the same clientId is allowed
// only by revealing the next pre-image of that chain. A peer that reuses the
// clientId with a bogus link, or with no link, is rejected — so it cannot take
// over (and close) another client's session by guessing its id.
func TestSessionResumptionRequiresToken(t *testing.T) {
	addr, stop := newServer(t, func(s valueserver.Server) {
		s.AddFunction("ping", valuerpc.Any, valuerpc.Any,
			func(_ context.Context, args value.Value) (value.Value, error) { return value.Utf8("pong"), nil })
	})
	defer stop()

	const cid = int64(0xABCDEF)

	chain, err := valuerpc.NewHashChain(100)
	if err != nil {
		t.Fatalf("new hash chain: %v", err)
	}

	// First connect: present the chain anchor -> accepted.
	c1, resp, err := rawHandshake(t, addr, cid, chain.Anchor())
	if err != nil {
		t.Fatalf("first handshake (anchor) rejected: %v", err)
	}
	defer c1.Close()
	if mt, ok := valuerpc.GetNumberField(resp, valuerpc.DefaultDialect.MessageTypeField); !ok || valuerpc.MessageType(mt.Long()) != valuerpc.HandshakeResponse {
		t.Fatal("first handshake (anchor) was not acknowledged")
	}

	// Resume by revealing the next pre-image -> accepted (legitimate reconnect).
	c2, resp2, err := rawHandshake(t, addr, cid, chain.Next())
	if err != nil {
		t.Fatalf("resume with the next pre-image was rejected: %v", err)
	}
	defer c2.Close()
	if mt, ok := valuerpc.GetNumberField(resp2, valuerpc.DefaultDialect.MessageTypeField); !ok || valuerpc.MessageType(mt.Long()) != valuerpc.HandshakeResponse {
		t.Fatal("resume with the correct pre-image did not get a handshake response")
	}

	// Hijack attempt: same cid, a well-formed but bogus link that hashes to nothing
	// in the chain -> rejected (server closes the conn).
	if c3, _, err := rawHandshake(t, addr, cid, strings.Repeat("ab", 32)); err == nil {
		c3.Close()
		t.Fatal("server accepted a handshake with a bogus resumption link (hijack)")
	}

	// Hijack attempt: same cid, no link -> rejected.
	if c4, _, err := rawHandshake(t, addr, cid, ""); err == nil {
		c4.Close()
		t.Fatal("server accepted a handshake reusing an existing client id with no resumption link (hijack)")
	}
}

// TestSessionResumptionToleratesDroppedHandshakes verifies the self-healing
// property the design requires: the client advances its hash chain on every
// reconnect attempt, even when a handshake is dropped, so the server can receive a
// pre-image several links past the last one it accepted. The server must hash the
// presented value forward to re-sync rather than demanding an exact one-step
// match — and a now-stale earlier link must still be rejected.
func TestSessionResumptionToleratesDroppedHandshakes(t *testing.T) {
	addr, stop := newServer(t, func(s valueserver.Server) {
		s.AddFunction("ping", valuerpc.Any, valuerpc.Any,
			func(_ context.Context, _ value.Value) (value.Value, error) { return value.Utf8("pong"), nil })
	})
	defer stop()

	const cid = int64(0xD0D0)

	chain, err := valuerpc.NewHashChain(100)
	if err != nil {
		t.Fatalf("new hash chain: %v", err)
	}

	// First connect: anchor accepted; server's last-verified link is now h[100].
	c1, _, err := rawHandshake(t, addr, cid, chain.Anchor())
	if err != nil {
		t.Fatalf("first handshake rejected: %v", err)
	}
	defer c1.Close()

	// Three reconnect handshakes the network dropped: the client advanced its chain
	// three times but the server saw none of them.
	_ = chain.Next() // h[99] dropped
	_ = chain.Next() // h[98] dropped
	_ = chain.Next() // h[97] dropped

	// The fourth reconnect reaches the server with h[96]; the server must hash it
	// forward four times (-> h[97], h[98], h[99], h[100]) to match and accept.
	c2, resp, err := rawHandshake(t, addr, cid, chain.Next())
	if err != nil {
		t.Fatalf("resume after dropped handshakes was rejected (no self-heal): %v", err)
	}
	defer c2.Close()
	if mt, ok := valuerpc.GetNumberField(resp, valuerpc.DefaultDialect.MessageTypeField); !ok || valuerpc.MessageType(mt.Long()) != valuerpc.HandshakeResponse {
		t.Fatal("resume after dropped handshakes was not acknowledged")
	}

	// A stale earlier link (the anchor) must now be rejected: the chain has moved
	// past it, so a sniffed-then-replayed link is useless.
	if c3, _, err := rawHandshake(t, addr, cid, chain.Anchor()); err == nil {
		c3.Close()
		t.Fatal("server accepted a stale earlier link after the chain advanced (replay)")
	}
}

// rawHandshakeAuth is rawHandshake with a credential attached, so a test can
// impersonate a peer presenting a specific clientId, token, and credential.
func rawHandshakeAuth(t *testing.T, addr string, clientId int64, token string, cred value.Value) (valuerpc.MsgConn, value.Map, error) {
	t.Helper()
	dialer := valuerpc.NewDialer(addr, "", valueclient.KeepAlivePeriod, valueclient.DefaultTimeout, valuerpc.MaxFrameSize)
	conn, err := dialer.Dial(context.Background())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	req := valuerpc.NewHandshakeRequest(clientId, token).Put(valuerpc.DefaultDialect.AuthField, cred)
	if err := conn.WriteMessage(req); err != nil {
		conn.Close()
		t.Fatalf("write handshake: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	resp, err := conn.ReadMessage()
	return conn, resp, err
}

// TestResumptionBoundToPrincipal verifies that session resumption is bound to the
// authenticated principal: a valid session token is not enough — a peer that
// authenticates as a *different* principal cannot resume someone else's session
// even with their (leaked) token.
func TestResumptionBoundToPrincipal(t *testing.T) {
	addr, stop := newServer(t, func(s valueserver.Server) {
		s.SetAuthenticator(func(_ valuerpc.MsgConn, cred value.Value) (string, error) {
			if cred == nil || cred.Kind() != value.STRING {
				return "", xerrors.New("no credential")
			}
			switch cred.(value.String).String() {
			case "alice-key":
				return "alice", nil
			case "bob-key":
				return "bob", nil
			}
			return "", xerrors.New("unknown credential")
		})
		s.AddFunction("ping", valuerpc.Any, valuerpc.Any,
			func(_ context.Context, _ value.Value) (value.Value, error) { return value.Utf8("pong"), nil })
	})
	defer stop()

	const cid = int64(0x1234)

	chain, err := valuerpc.NewHashChain(100)
	if err != nil {
		t.Fatalf("new hash chain: %v", err)
	}

	// Alice's first connect: present the anchor bound to principal "alice".
	c1, resp, err := rawHandshakeAuth(t, addr, cid, chain.Anchor(), value.Utf8("alice-key"))
	if err != nil {
		t.Fatalf("alice connect rejected: %v", err)
	}
	defer c1.Close()
	if mt, ok := valuerpc.GetNumberField(resp, valuerpc.DefaultDialect.MessageTypeField); !ok || valuerpc.MessageType(mt.Long()) != valuerpc.HandshakeResponse {
		t.Fatal("alice's first handshake was not acknowledged")
	}

	// Alice resumes with her next pre-image and credential -> accepted.
	c2, _, err := rawHandshakeAuth(t, addr, cid, chain.Next(), value.Utf8("alice-key"))
	if err != nil {
		t.Fatalf("alice's own resume was rejected: %v", err)
	}
	c2.Close()

	// Bob authenticates fine but is a different principal; even with a *valid*
	// leaked next pre-image of Alice's chain he must NOT be able to resume her
	// session — the principal binding is checked before the chain is even advanced.
	leaked := chain.Next()
	if c3, _, err := rawHandshakeAuth(t, addr, cid, leaked, value.Utf8("bob-key")); err == nil {
		c3.Close()
		t.Fatal("a different principal resumed the session using a leaked pre-image")
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
