/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valueserver_test

import (
	"context"
	"testing"

	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valueclient"
	"go.arpabet.com/value-rpc/valuerpc"
	"go.arpabet.com/value-rpc/valueserver"
)

// TestReverseCallMetadata verifies that metadata set on the context of a
// server->client reverse CallFunction is delivered to the client's inbound
// handler (valuerpc.MetadataFromContext) — symmetric with the forward path. This
// is what lets a multiplexed node route a reverse call to the right account.
func TestReverseCallMetadata(t *testing.T) {
	const want = "vox-abc123"

	addr, stop := newServer(t, func(s valueserver.Server) {
		s.AddFunction("trigger", valuerpc.Void, valuerpc.String,
			func(ctx context.Context, _ value.Value) (value.Value, error) {
				caller, ok := valueserver.PeerFromContext(ctx)
				if !ok {
					return nil, valuerpc.NewError(valuerpc.CodeInternal, "no peer")
				}
				rctx := valuerpc.ContextWithMetadata(ctx, valuerpc.Metadata{"acct": want})
				return caller.CallFunction(rctx, "whoami", nil)
			})
	})
	defer stop()

	cli := valueclient.NewClient(addr, "")
	cli.AddFunction("whoami", valuerpc.Void, valuerpc.String,
		func(ctx context.Context, _ value.Value) (value.Value, error) {
			md := valuerpc.MetadataFromContext(ctx) // nil-map read -> "" if not propagated
			return value.Utf8(md["acct"]), nil
		})
	if err := cli.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer cli.Close()
	cli.SetTimeout(5000)

	res, err := cli.CallFunction(context.Background(), "trigger", nil)
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if got := res.(value.String).String(); got != want {
		t.Fatalf("reverse-call metadata not propagated: got %q, want %q", got, want)
	}
}
