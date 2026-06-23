/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

// Command unix demonstrates the Unix-domain-socket transport and kernel peer
// authentication: a connect-authorizer reads the connecting process's
// credentials (uid/gid/pid) via valuerpc.PeerCredOf — useful for local-only
// services that authorize by OS identity rather than a network credential.
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
	"golang.org/x/xerrors"
)

func main() {
	sock := filepath.Join(os.TempDir(), fmt.Sprintf("vrpc-unix-%d.sock", time.Now().UnixNano()))
	defer os.Remove(sock)

	srv, err := valueserver.NewUnixServer(sock, zap.NewNop())
	if err != nil {
		log.Fatal(err)
	}
	defer srv.Close()

	// Authorize by OS peer identity. Here we just log it; a real service might
	// require a specific uid or group.
	srv.SetConnectAuthorizer(func(conn valuerpc.MsgConn) error {
		cred, ok := valuerpc.PeerCredOf(conn)
		if !ok {
			return xerrors.New("peer credentials unavailable")
		}
		fmt.Printf("  server: connection from uid=%d gid=%d pid=%d\n", cred.UID, cred.GID, cred.PID)
		if cred.UID != uint32(os.Getuid()) {
			return xerrors.New("only the owning user may connect")
		}
		return nil
	})
	srv.AddFunction("whoami", valuerpc.Any, valuerpc.Any,
		func(_ context.Context, _ value.Value) (value.Value, error) {
			return value.Utf8(fmt.Sprintf("served to uid %d", os.Getuid())), nil
		})
	go srv.Run()

	cli := valueclient.NewUnixClient(sock)
	if err := cli.Connect(); err != nil {
		log.Fatal(err)
	}
	defer cli.Close()

	r, err := cli.CallFunction(context.Background(), "whoami", nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("client: %s\n", r.(value.String).String())
}
