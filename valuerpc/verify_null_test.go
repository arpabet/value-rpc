/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valuerpc_test

import (
	"testing"

	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valuerpc"
)

// A List cannot have holes, so an explicit Null is the only way to skip an
// optional positional argument that precedes a supplied one. Verify must accept
// Null for an optional arg and still reject it for a required one.
func TestVerifyOptionalArgAcceptsNull(t *testing.T) {
	def := valuerpc.List(valuerpc.StringOpt, valuerpc.Number)

	args := value.Tuple(value.Null, value.Long(42))
	if !valuerpc.Verify(args, def) {
		t.Fatal("Null for an optional positional arg should verify")
	}

	required := valuerpc.List(valuerpc.String, valuerpc.Number)
	if valuerpc.Verify(args, required) {
		t.Fatal("Null for a required positional arg should be rejected")
	}
}

func TestVerifyOptionalParamAcceptsNull(t *testing.T) {
	def := valuerpc.Map(
		valuerpc.Param("name", value.STRING, true),
		valuerpc.Param("note", value.STRING, false),
	)
	args := value.EmptyMap(true).
		Put("name", value.Utf8("a")).
		Put("note", value.Null)
	if !valuerpc.Verify(args, def) {
		t.Fatal("Null for an optional param should verify")
	}
}
