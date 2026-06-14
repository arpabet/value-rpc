/*
 * Copyright (c) 2025 Karagatan LLC.
 * SPDX-License-Identifier: Apache-2.0
 */

package valuerpc_test

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	vrpc "go.arpabet.com/value-rpc/valuerpc"
)

func TestParseAddress(t *testing.T) {
	cases := []struct {
		in, network, addr string
	}{
		{":9000", "tcp", ":9000"},
		{"host:9000", "tcp", "host:9000"},
		{"tcp://host:9000", "tcp", "host:9000"},
		{"unix:///run/vrpc.sock", "unix", "/run/vrpc.sock"},
		{"ws://host/rpc", "ws", "host/rpc"},
	}
	for _, c := range cases {
		n, a := vrpc.ParseAddress(c.in)
		if n != c.network || a != c.addr {
			t.Errorf("ParseAddress(%q) = (%q, %q), want (%q, %q)", c.in, n, a, c.network, c.addr)
		}
	}
}

func TestNewListener_UnsupportedScheme(t *testing.T) {
	if _, err := vrpc.NewListener("bogus://addr", 0, time.Second); err == nil {
		t.Fatal("expected an error for an unsupported listen scheme")
	}
}

func TestNewDialer_UnsupportedSchemeErrorsOnDial(t *testing.T) {
	d := vrpc.NewDialer("bogus://addr", "", 0, time.Second)
	if _, err := d.Dial(); err == nil {
		t.Fatal("expected Dial to fail for an unsupported scheme")
	}
}

// TestNewUnixListener_StaleSocketCleanup simulates a socket file left behind by
// a crashed process and checks that NewUnixListener removes it and binds.
func TestNewUnixListener_StaleSocketCleanup(t *testing.T) {
	path := filepath.Join(os.TempDir(), fmt.Sprintf("vrpc-stale-%d.sock", time.Now().UnixNano()))
	defer os.Remove(path)

	l, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatalf("pre-listen: %v", err)
	}
	l.SetUnlinkOnClose(false) // leave the socket file on disk
	l.Close()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected the stale socket file to remain: %v", err)
	}

	lis, err := vrpc.NewUnixListener(path, time.Second)
	if err != nil {
		t.Fatalf("NewUnixListener over a stale socket: %v", err)
	}
	defer lis.Close()
	if got := lis.Addr().Network(); got != "unix" {
		t.Errorf("Addr().Network() = %q, want unix", got)
	}
}

// TestNewUnixListener_RefusesNonSocket ensures the cleanup never clobbers a
// regular file that happens to sit at the socket path.
func TestNewUnixListener_RefusesNonSocket(t *testing.T) {
	path := filepath.Join(t.TempDir(), "not-a-socket")
	if err := os.WriteFile(path, []byte("important"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := vrpc.NewUnixListener(path, time.Second); err == nil {
		t.Fatal("NewUnixListener must refuse to remove a non-socket file")
	}
	if _, err := os.Stat(path); err != nil {
		t.Errorf("the non-socket file must be left intact, stat: %v", err)
	}
}
