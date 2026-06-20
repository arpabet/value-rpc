/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

// Command peer demonstrates vRPC's bidirectional (peer-symmetric) calls: not
// only does a client call a function on the server, the server can call a
// function the *client* registered and use its result. Once connected, "client"
// and "server" are just who dialed — both ends speak the same Peer surface.
//
// It builds the canonical relay pattern: client A asks the server to reach
// client B, the server calls B back, and returns B's answer to A.
//
//	alice ──call{to:bob}──▶ server ──onMessage──▶ bob
//	  ▲                        │                    │
//	  └──────── reply ─────────┴──── bob's answer ──┘
//
// The pieces: the client registers handlers with Client.AddFunction (so it can
// be called); the server reaches a specific client with the caller handle from
// valueserver.ClientFromContext; identities come from the handshake
// Authenticator (valuerpc.PrincipalFromContext).
package main

import (
	"context"
	"fmt"
	"log"
	"sync"

	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valueclient"
	"go.arpabet.com/value-rpc/valuerpc"
	"go.arpabet.com/value-rpc/valueserver"
	"go.uber.org/zap"
)

func main() {
	srv, err := valueserver.NewServer("127.0.0.1:0", zap.NewNop())
	if err != nil {
		log.Fatal(err)
	}
	defer srv.Close()

	// Authenticate each connection to a principal (its name) so the server can
	// address clients by identity. The credential is the client's name here; in
	// a real system verify a token/signature and derive the identity from it.
	srv.SetAuthenticator(func(_ valuerpc.MsgConn, cred value.Value) (string, error) {
		if cred == nil || cred.Kind() != value.STRING {
			return "", fmt.Errorf("missing name credential")
		}
		return cred.(value.String).String(), nil
	})

	// Registry: principal -> caller handle, so a handler serving one client can
	// call a function on another connected client.
	var mu sync.RWMutex
	clients := map[string]valuerpc.Caller{}

	// register: a client announces itself. We capture its caller handle from the
	// request context (ClientFromContext) and remember it under the principal the
	// Authenticator bound to the connection.
	srv.AddFunction("register", valuerpc.Any, valuerpc.String,
		func(ctx context.Context, _ value.Value) (value.Value, error) {
			who := valuerpc.PrincipalFromContext(ctx)
			caller, ok := valueserver.ClientFromContext(ctx)
			if !ok {
				return nil, valuerpc.NewError(valuerpc.CodeInternal, "no client handle on connection")
			}
			mu.Lock()
			clients[who] = caller
			mu.Unlock()
			return value.Utf8("registered as " + who), nil
		})

	// call: A asks the server to reach B. The server calls B's onMessage (a
	// server->client call, on B's connection) and relays B's reply back to A.
	srv.AddFunction("call", valuerpc.Any, valuerpc.Any,
		func(ctx context.Context, args value.Value) (value.Value, error) {
			req := args.(value.Map)
			to := req.GetString("to").String()

			mu.RLock()
			peer, ok := clients[to]
			mu.RUnlock()
			if !ok {
				return nil, valuerpc.NewError(valuerpc.CodeNotFound, "%q is not connected", to)
			}

			// Forward, stamping the *authenticated* sender — not a value the
			// caller put in the payload.
			from := valuerpc.PrincipalFromContext(ctx)
			fwd := value.EmptyMap(true).
				Put("from", value.Utf8(from)).
				Put("text", req.GetString("text"))
			return peer.CallFunction(ctx, "onMessage", fwd)
		})

	go srv.Run()
	addr := srv.Addr().String()

	// connect spins up a named client that serves "onMessage" (a function the
	// server can call) and registers itself.
	connect := func(name string) valueclient.Client {
		cli := valueclient.NewClient(addr, "")
		cli.SetCredential(value.Utf8(name))
		cli.AddFunction("onMessage", valuerpc.Any, valuerpc.String,
			func(_ context.Context, args value.Value) (value.Value, error) {
				m := args.(value.Map)
				fmt.Printf("   [%s received] %q from %s\n",
					name, m.GetString("text").String(), m.GetString("from").String())
				return value.Utf8(name + " got it"), nil
			})
		if err := cli.Connect(); err != nil {
			log.Fatalf("%s connect: %v", name, err)
		}
		if _, err := cli.CallFunction(context.Background(), "register", nil); err != nil {
			log.Fatalf("%s register: %v", name, err)
		}
		return cli
	}

	alice := connect("alice")
	defer alice.Close()
	bob := connect("bob")
	defer bob.Close()

	// alice -> server -> bob -> back to alice.
	reply, err := alice.CallFunction(context.Background(), "call",
		value.EmptyMap(true).Put("to", value.Utf8("bob")).Put("text", value.Utf8("hi bob")))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("alice <- %q\n", reply.(value.String).String())

	// and the reverse direction: bob -> server -> alice -> back to bob.
	reply, err = bob.CallFunction(context.Background(), "call",
		value.EmptyMap(true).Put("to", value.Utf8("alice")).Put("text", value.Utf8("hi alice")))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("bob   <- %q\n", reply.(value.String).String())
}
