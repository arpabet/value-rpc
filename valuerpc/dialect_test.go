/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valuerpc_test

import (
	"testing"

	vrpc "go.arpabet.com/value-rpc/valuerpc"
)

// TestCustomDialectIncompatibleByDesign verifies that a custom dialect produces a
// wire format the default dialect does not recognise — the intended use for
// shedding the cleartext protocol fingerprint or forking the protocol.
func TestCustomDialectIncompatibleByDesign(t *testing.T) {
	custom := vrpc.NewDialect()
	custom.Magic = "X9"
	custom.MagicField = "z"
	custom.MessageTypeField = "q"
	custom.RequestIdField = "r"
	custom.ClientIdField = "c"

	hs := custom.NewHandshakeRequest(7, "")

	// A dialect accepts its own handshake ...
	if !custom.ValidMagicAndVersion(hs) {
		t.Fatal("a dialect must accept its own handshake")
	}
	// ... but the default dialect must reject it (incompatible by design).
	if vrpc.DefaultDialect.ValidMagicAndVersion(hs) {
		t.Fatal("the default dialect must reject a custom-dialect handshake")
	}

	// The default field names carry nothing; the custom magic rides the custom key.
	if _, ok := vrpc.GetStringField(hs, vrpc.NewDialect().MagicField); ok {
		t.Errorf("custom handshake must not use the default magic field %q", vrpc.NewDialect().MagicField)
	}
	if mg, ok := vrpc.GetStringField(hs, "z"); !ok || mg.String() != "X9" {
		t.Errorf("custom magic = %v (ok=%v), want %q under field %q", mg, ok, "X9", "z")
	}
}

// TestNewDialectIsStandard pins that the default-constructed dialect equals the
// package DefaultDialect's wire markers (so NewDialect is a safe base to copy).
func TestNewDialectIsStandard(t *testing.T) {
	d := vrpc.NewDialect()
	if d.Magic != vrpc.DefaultDialect.Magic ||
		d.MessageTypeField != vrpc.DefaultDialect.MessageTypeField ||
		d.RequestIdField != vrpc.DefaultDialect.RequestIdField {
		t.Fatal("NewDialect should return the standard dialect")
	}
}
