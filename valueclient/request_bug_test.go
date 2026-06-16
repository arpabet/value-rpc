/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valueclient

import (
	"testing"

	"go.arpabet.com/value"
)

// These tests pin the BUG-7 fix: a chat request opens both the get and put
// halves, and resultCh must be closed exactly once — when the LAST half closes
// — regardless of order. Previously each half closed independently and the
// second close panicked with "close of closed channel".

func assertClosed(t *testing.T, ctx *rpcRequestCtx) {
	t.Helper()
	if _, ok := <-ctx.resultCh; ok {
		t.Fatal("resultCh should be closed")
	}
}

func TestRequestCtx_ChatClosesOnce_GetThenPut(t *testing.T) {
	ctx := NewRequestCtx(1, chatKind, value.EmptyMap(true), 4)

	if done := ctx.TryGetClose(); done {
		t.Fatal("chat is not finished until the put side also closes")
	}
	// Must not panic (this is the BUG-7 regression guard).
	if done := ctx.TryPutClose(); !done {
		t.Fatal("chat must be finished once both halves close")
	}
	assertClosed(t, ctx)
}

func TestRequestCtx_ChatClosesOnce_PutThenGet(t *testing.T) {
	ctx := NewRequestCtx(2, chatKind, value.EmptyMap(true), 4)

	if done := ctx.TryPutClose(); done {
		t.Fatal("chat is not finished until the get side also closes")
	}
	if done := ctx.TryGetClose(); !done {
		t.Fatal("chat must be finished once both halves close")
	}
	assertClosed(t, ctx)
}

func TestRequestCtx_GetStreamClosesOnGet(t *testing.T) {
	ctx := NewRequestCtx(3, getStreamKind, value.EmptyMap(true), 1)
	if done := ctx.TryGetClose(); !done {
		t.Fatal("get-stream is finished when the get side closes")
	}
	assertClosed(t, ctx)
}

func TestRequestCtx_PutStreamClosesOnPut(t *testing.T) {
	ctx := NewRequestCtx(4, putStreamKind, value.EmptyMap(true), 1)
	if done := ctx.TryPutClose(); !done {
		t.Fatal("put-stream is finished when the put side closes")
	}
	assertClosed(t, ctx)
}

func TestRequestCtx_UnaryCloseIsIdempotent(t *testing.T) {
	ctx := NewRequestCtx(5, unaryKind, value.EmptyMap(true), 1)
	ctx.Close()
	ctx.Close() // must not panic (closeOnce)
	assertClosed(t, ctx)
}
