/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valuerpc_test

import (
	"testing"

	"go.arpabet.com/value"
	vrpc "go.arpabet.com/value-rpc/valuerpc"
)

// roundTrip packs then unpacks a message the same way MsgConn does on the wire.
func roundTrip(t *testing.T, m value.Map) value.Map {
	t.Helper()
	buf, err := value.Pack(m)
	if err != nil {
		t.Fatalf("pack: %v", err)
	}
	got, err := value.Unpack(buf, true)
	if err != nil {
		t.Fatalf("unpack: %v", err)
	}
	if got.Kind() != value.MAP {
		t.Fatalf("expected MAP, got %s", got.Kind())
	}
	return got.(value.Map)
}

func TestHandshakeRequestRoundTrip(t *testing.T) {
	req := roundTrip(t, vrpc.NewHandshakeRequest(42, "sometoken"))

	if got := req.GetString(vrpc.MagicField).String(); got != vrpc.Magic {
		t.Errorf("magic = %q, want %q", got, vrpc.Magic)
	}
	if got := vrpc.MessageType(req.GetNumber(vrpc.MessageTypeField).Long()); got != vrpc.HandshakeRequest {
		t.Errorf("type = %v, want HandshakeRequest", got)
	}
	if got := req.GetNumber(vrpc.ClientIdField).Long(); got != 42 {
		t.Errorf("clientId = %d, want 42", got)
	}
	if got := req.GetNumber(vrpc.RequestIdField).Long(); got != vrpc.HandshakeRequestId {
		t.Errorf("requestId = %d, want %d", got, vrpc.HandshakeRequestId)
	}
	if got, ok := vrpc.GetStringField(req, vrpc.SessionTokenField); !ok || got.String() != "sometoken" {
		t.Errorf("session token = %q (present=%v), want %q", got, ok, "sometoken")
	}
}

// TestHandshakeRequest_OmitsEmptyToken: the first connect has no token yet, so
// the field must be absent (not an empty string the server might misread).
func TestHandshakeRequest_OmitsEmptyToken(t *testing.T) {
	req := roundTrip(t, vrpc.NewHandshakeRequest(7, ""))
	if _, ok := vrpc.GetStringField(req, vrpc.SessionTokenField); ok {
		t.Fatal("session token field must be omitted when empty")
	}
}

func TestValidMagicAndVersion_Valid(t *testing.T) {
	if !vrpc.ValidMagicAndVersion(vrpc.NewHandshakeRequest(1, "")) {
		t.Fatal("a freshly-built handshake must validate")
	}
}

func TestValidMagicAndVersion_BadMagic(t *testing.T) {
	req := value.EmptyMap(true).
		Put(vrpc.MagicField, value.Utf8("NOPE")).
		Put(vrpc.VersionField, value.Double(vrpc.Version))
	if vrpc.ValidMagicAndVersion(req) {
		t.Fatal("wrong magic must be rejected")
	}
}

// TestValidMagicAndVersion_RejectsNewerVersion pins the BUG-1 fix:
// ValidMagicAndVersion must read the version out of VersionField ("v") and
// reject a client claiming a version newer than ours.
func TestValidMagicAndVersion_RejectsNewerVersion(t *testing.T) {
	req := value.EmptyMap(true).
		Put(vrpc.MagicField, value.Utf8(vrpc.Magic)).
		Put(vrpc.VersionField, value.Double(999.0)) // pretend to be from the future

	if vrpc.ValidMagicAndVersion(req) {
		t.Fatal("BUG-1: a newer-than-supported version must be rejected")
	}
}

// TestValidMagicAndVersion_MissingVersion: a handshake with no version field
// must be rejected (GetNumber would silently return Zero).
func TestValidMagicAndVersion_MissingVersion(t *testing.T) {
	req := value.EmptyMap(true).Put(vrpc.MagicField, value.Utf8(vrpc.Magic))
	if vrpc.ValidMagicAndVersion(req) {
		t.Fatal("a handshake without a version field must be rejected")
	}
}

func TestVerify_AnyAndVoid(t *testing.T) {
	if !vrpc.Verify(value.Utf8("anything"), vrpc.Any) {
		t.Error("Any must accept any value")
	}
	if !vrpc.Verify(nil, vrpc.Void) {
		t.Error("Void must accept nil")
	}
	if !vrpc.Verify(value.EmptyList(true), vrpc.Void) {
		t.Error("Void must accept an empty list")
	}
	if vrpc.Verify(value.Tuple(value.Utf8("x")), vrpc.Void) {
		t.Error("Void must reject a non-empty list")
	}
}

func TestVerify_Args(t *testing.T) {
	def := vrpc.List(vrpc.String, vrpc.Number)

	ok := value.Tuple(value.Utf8("alex"), value.Long(7))
	if !vrpc.Verify(ok, def) {
		t.Error("matching args must verify")
	}

	wrongType := value.Tuple(value.Utf8("alex"), value.Utf8("seven"))
	if vrpc.Verify(wrongType, def) {
		t.Error("wrong-typed arg must fail")
	}

	wrongArity := value.Tuple(value.Utf8("alex"))
	if vrpc.Verify(wrongArity, def) {
		t.Error("wrong arity must fail")
	}
}

func TestVerify_Params(t *testing.T) {
	def := vrpc.Map(
		vrpc.Param("name", value.STRING, true),
		vrpc.Param("age", value.NUMBER, true),
	)

	ok := value.EmptyMap(true).
		Put("name", value.Utf8("alex")).
		Put("age", value.Long(30))
	if !vrpc.Verify(ok, def) {
		t.Error("matching params must verify")
	}

	missing := value.EmptyMap(true).Put("name", value.Utf8("alex"))
	if vrpc.Verify(missing, def) {
		t.Error("missing required param must fail")
	}
}

func TestVerify_AnyAcceptsNull(t *testing.T) {
	if !vrpc.Verify(nil, vrpc.Any) {
		t.Error("Any must accept Go nil")
	}
	if !vrpc.Verify(value.Null, vrpc.Any) {
		t.Error("Any must accept value.Null (the on-wire form of nil args)")
	}
}

// TestVerify_VoidVariants pins BUG-2: Void must accept Go nil, value.Null, and
// empty collections, and reject anything non-empty.
func TestVerify_VoidVariants(t *testing.T) {
	accept := []value.Value{
		nil,
		value.Null,
		value.EmptyList(true),
		value.EmptyMap(true),
	}
	for i, v := range accept {
		if !vrpc.Verify(v, vrpc.Void) {
			t.Errorf("Void must accept case %d (%v)", i, v)
		}
	}
	if vrpc.Verify(value.Tuple(value.Long(1)), vrpc.Void) {
		t.Error("Void must reject a non-empty list")
	}
}

// TestVerify_ArgsNull pins BUG-2 for the args path: an empty ArgsDef must accept
// value.Null (nil args on the wire), while a non-empty one must reject it.
func TestVerify_ArgsNull(t *testing.T) {
	if !vrpc.Verify(value.Null, vrpc.List()) {
		t.Error("empty ArgsDef must accept Null (no args)")
	}
	if vrpc.Verify(value.Null, vrpc.List(vrpc.String)) {
		t.Error("non-empty ArgsDef must reject Null")
	}
}

func TestVerify_OptionalParam(t *testing.T) {
	def := vrpc.Map(
		vrpc.Param("name", value.STRING, true),
		vrpc.Param("nick", value.STRING, false), // optional
	)

	// optional param absent: OK
	ok := value.EmptyMap(true).Put("name", value.Utf8("alex"))
	if !vrpc.Verify(ok, def) {
		t.Error("an absent optional param must be allowed")
	}

	// optional param present but wrong type: rejected
	wrong := value.EmptyMap(true).
		Put("name", value.Utf8("alex")).
		Put("nick", value.Long(5))
	if vrpc.Verify(wrong, def) {
		t.Error("a present optional param with the wrong type must be rejected")
	}
}

func TestMessageTypeLong(t *testing.T) {
	for _, mt := range []vrpc.MessageType{
		vrpc.HandshakeRequest, vrpc.FunctionRequest, vrpc.ChatRequest, vrpc.StreamEnd,
	} {
		if got := vrpc.MessageType(mt.Long().Long()); got != mt {
			t.Errorf("round-trip MessageType %d -> %d", mt, got)
		}
	}
}

func BenchmarkPackUnpackFunctionRequest(b *testing.B) {
	req := value.EmptyMap(true).
		Put(vrpc.MessageTypeField, vrpc.FunctionRequest.Long()).
		Put(vrpc.RequestIdField, value.Long(12345)).
		Put(vrpc.FunctionNameField, value.Utf8("computeStuff")).
		Put(vrpc.ArgumentsField, value.Tuple(value.Utf8("hello"), value.Long(42), value.Double(3.14)))

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf, err := value.Pack(req)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := value.Unpack(buf, true); err != nil {
			b.Fatal(err)
		}
	}
}
