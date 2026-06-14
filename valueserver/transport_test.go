/*
 * Copyright (c) 2025 Karagatan LLC.
 * SPDX-License-Identifier: Apache-2.0
 */

package valueserver_test

import (
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
		func(args value.Value) (value.Value, error) {
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

	res, err := cli.CallFunction("echo", value.Tuple(value.Utf8("hi")))
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
		func(args value.Value) (value.Value, error) {
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

	res, err := cli.CallFunction("echo", value.Tuple(value.Utf8("hi")))
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if got := res.(value.String).String(); got != "u:hi" {
		t.Fatalf("result = %q, want %q", got, "u:hi")
	}
}
