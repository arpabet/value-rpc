/*
 * Copyright (c) 2025 Karagatan LLC.
 * SPDX-License-Identifier: Apache-2.0
 */

package valueserver_test

import (
	"testing"

	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valueclient"
	"go.arpabet.com/value-rpc/valuerpc"
	"go.arpabet.com/value-rpc/valueserver"
	"go.uber.org/zap"
)

func TestMem_RoundTrip(t *testing.T) {
	srv, err := valueserver.NewMemServer("mem-roundtrip", zap.NewNop())
	if err != nil {
		t.Fatalf("mem server: %v", err)
	}
	defer srv.Close()
	srv.AddFunction("echo", valuerpc.List(valuerpc.String), valuerpc.String,
		func(args value.Value) (value.Value, error) {
			return value.Utf8("m:" + args.(value.List).GetStringAt(0).String()), nil
		})
	go srv.Run()

	cli := valueclient.NewMemClient("mem-roundtrip")
	if err := cli.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer cli.Close()
	cli.SetTimeout(5000)

	res, err := cli.CallFunction("echo", value.Tuple(value.Utf8("hi")))
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if got := res.(value.String).String(); got != "m:hi" {
		t.Fatalf("result = %q, want %q", got, "m:hi")
	}
}

// TestMem_SchemeViaNewServer drives the mem:// scheme through the plain
// NewServer / NewClient constructors — the "swap the address, no other changes"
// path for splitting a monolith onto a real socket later.
func TestMem_SchemeViaNewServer(t *testing.T) {
	srv, err := valueserver.NewServer("mem://mem-scheme", zap.NewNop())
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	defer srv.Close()
	srv.AddFunction("ping", valuerpc.Void, valuerpc.String,
		func(args value.Value) (value.Value, error) { return value.Utf8("pong"), nil })
	go srv.Run()

	cli := valueclient.NewClient("mem://mem-scheme", "")
	if err := cli.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer cli.Close()
	cli.SetTimeout(5000)

	res, err := cli.CallFunction("ping", nil)
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if got := res.(value.String).String(); got != "pong" {
		t.Fatalf("result = %q, want %q", got, "pong")
	}
}

func TestMem_DuplicateNameRejected(t *testing.T) {
	srv, err := valueserver.NewMemServer("mem-dup", zap.NewNop())
	if err != nil {
		t.Fatalf("mem server: %v", err)
	}
	defer srv.Close()

	if _, err := valueserver.NewMemServer("mem-dup", zap.NewNop()); err == nil {
		t.Fatal("expected a duplicate mem name to be rejected")
	}
}

func TestMem_DialUnregistered(t *testing.T) {
	cli := valueclient.NewMemClient("mem-does-not-exist")
	defer cli.Close()
	if err := cli.Connect(); err == nil {
		t.Fatal("expected dialing an unregistered mem name to fail")
	}
}
