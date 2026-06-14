/*
 * Copyright (c) 2025 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valueclient

import (
	"testing"

	"go.arpabet.com/value"
)

// TestRequestCtx_GetThenPutClose_DoubleClose deterministically reproduces the
// Chat double-close panic without any networking.
//
// A Chat request opens BOTH the get and put halves (state = getStreamFlag +
// putStreamFlag). On a normal chat shutdown the server's StreamEnd drives
// TryGetClose() on the response loop while the client's drained putCh drives
// TryPutClose() in streamOut. Each method unconditionally calls
// close(t.resultCh) when it clears *its* flag — so the second one panics with
// "close of closed channel". See FINDINGS.md (BUG-7).
//
// The intended contract is "close the channel once, when the LAST half closes".
func TestRequestCtx_GetThenPutClose_DoubleClose(t *testing.T) {
	ctx := NewRequestCtx(1, value.EmptyMap(true), 4) // opens get+put, like Chat

	ctx.TryGetClose() // closes resultCh once (state 3 -> 2)

	defer func() {
		if r := recover(); r != nil {
			t.Logf("BUG-7 confirmed: double close panic: %v", r)
			return
		}
		t.Skip("no panic: double-close appears to be FIXED — update this test")
	}()

	ctx.TryPutClose() // clears put flag (state 2 -> 0) and closes resultCh AGAIN -> panic
	t.Fatalf("expected a double-close panic, got none")
}
