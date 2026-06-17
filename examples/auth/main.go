/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

// Command auth demonstrates the handshake Authenticator: the server validates a
// credential the client attaches with SetCredential and derives a principal
// identity. The credential is re-sent on every reconnect, and session resumption
// is bound to both the server-issued token and that principal (so a leaked token
// alone cannot let a different principal take over the session).
package main

import (
	"context"
	"fmt"
	"log"

	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valueclient"
	"go.arpabet.com/value-rpc/valuerpc"
	"go.arpabet.com/value-rpc/valueserver"
	"go.uber.org/zap"
)

func main() {
	tokens := map[string]string{ // bearer token -> principal
		"alice-token": "alice",
		"bob-token":   "bob",
	}

	srv, err := valueserver.NewServer("127.0.0.1:0", zap.NewNop())
	if err != nil {
		log.Fatal(err)
	}
	defer srv.Close()

	srv.SetAuthenticator(func(_ valuerpc.MsgConn, cred value.Value) (string, error) {
		if cred == nil || cred.Kind() != value.STRING {
			return "", fmt.Errorf("missing credential")
		}
		principal, ok := tokens[cred.(value.String).String()]
		if !ok {
			return "", fmt.Errorf("invalid token")
		}
		return principal, nil
	})
	srv.AddFunction("ping", valuerpc.Any, valuerpc.Any,
		func(_ context.Context, _ value.Value) (value.Value, error) { return value.Utf8("pong"), nil })
	go srv.Run()
	addr := srv.Addr().String()

	// 1) No credential -> handshake rejected, so the call fails.
	anon := valueclient.NewClient(addr, "")
	anon.SetTimeout(500)
	_ = anon.Connect()
	_, err = anon.CallFunction(context.Background(), "ping", nil)
	fmt.Printf("anonymous call -> error: %v\n", err != nil)
	anon.Close()

	// 2) Valid credential -> authenticated as "alice".
	alice := valueclient.NewClient(addr, "")
	alice.SetCredential(value.Utf8("alice-token"))
	if err := alice.Connect(); err != nil {
		log.Fatal(err)
	}
	defer alice.Close()
	r, err := alice.CallFunction(context.Background(), "ping", nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("alice call -> %s\n", r.(value.String).String())

	// 3) Reconnect re-sends the credential + session token and resumes the session.
	if err := alice.Reconnect(); err != nil {
		log.Fatal(err)
	}
	r, err = alice.CallFunction(context.Background(), "ping", nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("alice after reconnect -> %s (session resumed)\n", r.(value.String).String())
}
