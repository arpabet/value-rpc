/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valueserver_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valueclient"
	"go.arpabet.com/value-rpc/valuerpc"
	"go.arpabet.com/value-rpc/valueserver"
	"go.uber.org/zap"
)

// TestSeam_NewServerWithListener_TCP exercises the explicit transport
// constructors (NewServerWithListener / NewClientWithDialer) over a TCP stream
// listener built directly, proving the seam is wired end to end.
func TestSeam_NewServerWithListener_TCP(t *testing.T) {
	lis, err := valuerpc.NewStreamListener("tcp", "127.0.0.1:0",
		valueserver.KeepAlivePeriod, valueserver.DefaultTimeout)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	srv, err := valueserver.NewServerWithListener(lis, zap.NewNop())
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	defer srv.Close()
	srv.AddFunction("echo", valuerpc.List(valuerpc.String), valuerpc.String,
		func(_ context.Context, args value.Value) (value.Value, error) {
			return value.Utf8("t:" + args.(value.List).GetStringAt(0).String()), nil
		})
	go srv.Run()

	dialer := valuerpc.NewStreamDialer("tcp", srv.Addr().String(), "",
		valueclient.KeepAlivePeriod, valueclient.DefaultTimeout)
	cli := valueclient.NewClientWithDialer(dialer)
	if err := cli.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer cli.Close()
	cli.SetTimeout(5000)

	res, err := cli.CallFunction(context.Background(), "echo", value.Tuple(value.Utf8("hi")))
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if got := res.(value.String).String(); got != "t:hi" {
		t.Fatalf("result = %q, want %q", got, "t:hi")
	}
}

// TestSeam_UnixSocketTransport proves the seam is genuinely transport-agnostic:
// the same generic stream transport, pointed at the "unix" network, carries a
// full RPC over a Unix domain socket with no other code changes. (The ergonomic
// unix:// scheme / NewUnixServer wrappers and stale-file handling are phase 2.)
func TestSeam_UnixSocketTransport(t *testing.T) {
	// Keep the path short — macOS caps Unix socket paths at ~104 bytes.
	sock := filepath.Join(os.TempDir(), fmt.Sprintf("vrpc-%d.sock", time.Now().UnixNano()))
	defer os.Remove(sock)

	lis, err := valuerpc.NewStreamListener("unix", sock, 0, valueserver.DefaultTimeout)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}

	srv, err := valueserver.NewServerWithListener(lis, zap.NewNop())
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	defer srv.Close()
	srv.AddFunction("echo", valuerpc.List(valuerpc.String), valuerpc.String,
		func(_ context.Context, args value.Value) (value.Value, error) {
			return value.Utf8("u:" + args.(value.List).GetStringAt(0).String()), nil
		})
	go srv.Run()

	dialer := valuerpc.NewStreamDialer("unix", sock, "", 0, valueclient.DefaultTimeout)
	cli := valueclient.NewClientWithDialer(dialer)
	if err := cli.Connect(); err != nil {
		t.Fatalf("connect unix: %v", err)
	}
	defer cli.Close()
	cli.SetTimeout(5000)

	res, err := cli.CallFunction(context.Background(), "echo", value.Tuple(value.Utf8("hi")))
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if got := res.(value.String).String(); got != "u:hi" {
		t.Fatalf("result = %q, want %q", got, "u:hi")
	}
}

// tmpSock returns a short, unique Unix socket path (macOS caps sun_path ~104).
func tmpSock(t testing.TB) string {
	t.Helper()
	return filepath.Join(os.TempDir(), fmt.Sprintf("vrpc-%d.sock", time.Now().UnixNano()))
}

// TestUnixScheme_RoundTrip drives the ergonomic API: a unix:// address through
// the plain NewServer / NewClient constructors.
func TestUnixScheme_RoundTrip(t *testing.T) {
	sock := tmpSock(t)
	defer os.Remove(sock)

	srv, err := valueserver.NewServer("unix://"+sock, zap.NewNop())
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	defer srv.Close()
	srv.AddFunction("echo", valuerpc.List(valuerpc.String), valuerpc.String,
		func(_ context.Context, args value.Value) (value.Value, error) {
			return value.Utf8(args.(value.List).GetStringAt(0).String()), nil
		})
	go srv.Run()

	cli := valueclient.NewClient("unix://"+sock, "")
	if err := cli.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer cli.Close()
	cli.SetTimeout(5000)

	res, err := cli.CallFunction(context.Background(), "echo", value.Tuple(value.Utf8("scheme")))
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if got := res.(value.String).String(); got != "scheme" {
		t.Fatalf("result = %q, want %q", got, "scheme")
	}
}

// TestNewUnixServerClient_RoundTrip drives the NewUnixServer / NewUnixClient
// convenience constructors.
func TestNewUnixServerClient_RoundTrip(t *testing.T) {
	sock := tmpSock(t)
	defer os.Remove(sock)

	srv, err := valueserver.NewUnixServer(sock, zap.NewNop())
	if err != nil {
		t.Fatalf("new unix server: %v", err)
	}
	defer srv.Close()
	srv.AddFunction("ping", valuerpc.Void, valuerpc.String,
		func(_ context.Context, args value.Value) (value.Value, error) {
			return value.Utf8("pong"), nil
		})
	go srv.Run()

	cli := valueclient.NewUnixClient(sock)
	if err := cli.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer cli.Close()
	cli.SetTimeout(5000)

	res, err := cli.CallFunction(context.Background(), "ping", nil)
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if got := res.(value.String).String(); got != "pong" {
		t.Fatalf("result = %q, want %q", got, "pong")
	}
}

// TestPeerCredOverUnix checks that a Unix connection exposes the peer's
// credentials (via the connect authorizer + valuerpc.PeerCredOf), and that they
// match the running process.
func TestPeerCredOverUnix(t *testing.T) {
	sock := tmpSock(t)
	defer os.Remove(sock)

	srv, err := valueserver.NewUnixServer(sock, zap.NewNop())
	if err != nil {
		t.Fatalf("new unix server: %v", err)
	}
	defer srv.Close()

	credCh := make(chan valuerpc.PeerCred, 1)
	srv.SetConnectAuthorizer(func(conn valuerpc.MsgConn) error {
		cred, ok := valuerpc.PeerCredOf(conn)
		if !ok {
			return fmt.Errorf("no peer credentials available")
		}
		select {
		case credCh <- cred:
		default:
		}
		return nil
	})
	srv.AddFunction("ping", valuerpc.Void, valuerpc.String,
		func(_ context.Context, args value.Value) (value.Value, error) {
			return value.Utf8("pong"), nil
		})
	go srv.Run()

	cli := valueclient.NewUnixClient(sock)
	if err := cli.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer cli.Close()
	cli.SetTimeout(5000)
	if _, err := cli.CallFunction(context.Background(), "ping", nil); err != nil {
		t.Fatalf("call: %v", err)
	}

	select {
	case cred := <-credCh:
		if cred.UID != uint32(os.Getuid()) {
			t.Errorf("peer UID = %d, want %d", cred.UID, os.Getuid())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("connect authorizer never observed the connection")
	}
}

// TestConnectAuthorizerRejects verifies a rejecting authorizer closes the
// connection so calls fail.
func TestConnectAuthorizerRejects(t *testing.T) {
	sock := tmpSock(t)
	defer os.Remove(sock)

	srv, err := valueserver.NewUnixServer(sock, zap.NewNop())
	if err != nil {
		t.Fatalf("new unix server: %v", err)
	}
	defer srv.Close()
	srv.SetConnectAuthorizer(func(conn valuerpc.MsgConn) error {
		return fmt.Errorf("denied")
	})
	srv.AddFunction("ping", valuerpc.Void, valuerpc.String,
		func(_ context.Context, args value.Value) (value.Value, error) {
			return value.Utf8("pong"), nil
		})
	go srv.Run()

	cli := valueclient.NewUnixClient(sock)
	_ = cli.Connect() // dial may succeed; the server closes after rejecting
	defer cli.Close()
	cli.SetTimeout(1000)

	if _, err := cli.CallFunction(context.Background(), "ping", nil); err == nil {
		t.Fatal("expected the call to fail when the authorizer rejects the connection")
	}
}
