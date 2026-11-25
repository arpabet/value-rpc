/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valuerpc

import (
	"math"
	"math/big"
	"strings"
	"testing"

	"go.arpabet.com/value"
)

// jsonRoundTrip asserts that a value survives JSON encode+decode intact (by
// canonical msgpack-byte equality, which is value's Equal contract).
func jsonRoundTrip(t *testing.T, v value.Value) value.Value {
	t.Helper()
	text := value.Jsonify(v)
	got, err := ParseJson([]byte(text))
	if err != nil {
		t.Fatalf("ParseJson(%q): %v", text, err)
	}
	return got
}

func TestParseJsonPrimitives(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want value.Value
	}{
		{"null", `null`, value.Null},
		{"true", `true`, value.Boolean(true)},
		{"false", `false`, value.Boolean(false)},
		{"zero", `0`, value.Long(0)},
		{"int", `42`, value.Long(42)},
		{"neg", `-7`, value.Long(-7)},
		{"maxint64", `9223372036854775807`, value.Long(math.MaxInt64)},
		{"double", `1.5`, value.Double(1.5)},
		{"exp", `1e3`, value.Double(1000)},
		{"string", `"hello"`, value.Utf8("hello")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseJson([]byte(tc.in))
			if err != nil {
				t.Fatalf("ParseJson(%q): %v", tc.in, err)
			}
			if !got.Equal(tc.want) {
				t.Fatalf("ParseJson(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

func TestParseJsonIntegerOverflowToBigInt(t *testing.T) {
	// Beyond int64 must widen to BIGINT rather than lose precision as a Double.
	got, err := ParseJson([]byte(`18446744073709551616`)) // 2^64
	if err != nil {
		t.Fatal(err)
	}
	n, ok := got.(value.Number)
	if !ok || n.Type() != value.BIGINT {
		t.Fatalf("2^64 decoded as %T (%v), want BIGINT", got, got)
	}
	want := new(big.Int).Lsh(big.NewInt(1), 64)
	if n.BigInt().Cmp(want) != 0 {
		t.Fatalf("BigInt = %v, want %v", n.BigInt(), want)
	}
}

func TestJsonRoundTripBytesAndExt(t *testing.T) {
	// RAW bytes round-trip via the "base64," convention.
	raw := value.Raw([]byte{0, 1, 2, 254, 255}, false)
	if got := jsonRoundTrip(t, raw); !got.Equal(raw) {
		t.Fatalf("raw round-trip = %v, want %v", got, raw)
	}
	// UNKNOWN ext round-trips via the data-uri convention (tag byte + data).
	ext := value.Unknown([]byte{7, 9, 8})
	got := jsonRoundTrip(t, ext)
	if !got.Equal(ext) {
		t.Fatalf("ext round-trip = %v, want %v", got, ext)
	}
}

func TestJsonRoundTripEnvelope(t *testing.T) {
	env := value.EmptyMap(true).
		Put("t", value.Long(2)).
		Put("rid", value.Long(1)).
		Put("fn", value.Utf8("user.get")).
		Put("args", value.ImmutableList([]value.Value{
			value.Long(42), value.Utf8("x"), value.Null, value.Boolean(true),
			value.Raw([]byte{0, 255}, false),
			value.EmptyMap(true).Put("deep", value.Double(1.25)),
		})).
		Put("sla", value.Long(5000))
	got := jsonRoundTrip(t, env)
	if !got.Equal(env) {
		t.Fatalf("envelope round-trip mismatch:\n got %s\nwant %s", value.Jsonify(got), value.Jsonify(env))
	}
}

// An envelope is JSON whatever text it carries, as a value and as a map key:
// before value v1.6.2 a control character, a quote in a key or an infinite
// double gave a frame that no JSON reader took, ParseJson and a browser's
// JSON.parse included, so one such argument broke the call.
func TestJsonEnvelopeIsJsonWhateverTheText(t *testing.T) {
	texts := []string{
		"bell \a, escape \x1b[0m, vertical tab \v, delete \x7f, null \x00",
		`a quote " and a backslash \`,
		"line\nbreak\ttab",
		"beyond the basic plane and not printable: " + string(rune(0xe0001)),
	}
	for _, text := range texts {
		env := value.EmptyMap(true).
			Put("t", value.Long(2)).
			Put("rid", value.Long(1)).
			Put("fn", value.Utf8("echo")).
			Put("args", value.EmptyMap(true).Put(text, value.Utf8(text))).
			Put("md", value.EmptyMap(true).Put("trace", value.Utf8(text)))
		frame, err := JsonWireCodec.Encode(env)
		if err != nil {
			t.Fatalf("encode %q: %v", text, err)
		}
		got, err := JsonWireCodec.Decode(frame)
		if err != nil {
			t.Fatalf("decode of %s: %v", frame, err)
		}
		if !got.Equal(env) {
			t.Fatalf("envelope round-trip mismatch for %q:\n got %s\nwant %s", text, value.Jsonify(got), value.Jsonify(env))
		}
	}

	// A double that is NaN or infinite has no JSON form and travels as null.
	frame, err := JsonWireCodec.Encode(value.EmptyMap(true).
		Put("t", value.Long(3)).
		Put("rid", value.Long(1)).
		Put("res", value.ImmutableList([]value.Value{value.Double(math.Inf(1)), value.Double(math.Inf(-1)), value.Double(math.NaN())})))
	if err != nil {
		t.Fatal(err)
	}
	got, err := JsonWireCodec.Decode(frame)
	if err != nil {
		t.Fatalf("decode of %s: %v", frame, err)
	}
	want := value.ImmutableList([]value.Value{value.Null, value.Null, value.Null})
	if !got.Get("res").Equal(want) {
		t.Fatalf("got %s, want %s", value.Jsonify(got.Get("res")), value.Jsonify(want))
	}

	// A byte that is not UTF-8 has no JSON form either: it arrives as U+FFFD.
	frame, err = JsonWireCodec.Encode(value.EmptyMap(true).Put("val", value.Utf8("a\xffb")))
	if err != nil {
		t.Fatal(err)
	}
	if got, err = JsonWireCodec.Decode(frame); err != nil {
		t.Fatalf("decode of %s: %v", frame, err)
	}
	if text := got.Get("val").String(); text != "a"+string(rune(0xfffd))+"b" {
		t.Fatalf("got %q", text)
	}
}

func TestParseJsonRejectsTrailingAndMalformed(t *testing.T) {
	for _, in := range []string{`{}{}`, `[1,2] 3`, `{`, `not json`, ``} {
		if _, err := ParseJson([]byte(in)); err == nil {
			t.Fatalf("ParseJson(%q) succeeded, want error", in)
		}
	}
}

func TestParseJsonDepthLimit(t *testing.T) {
	old := value.MaxParseDepth
	value.MaxParseDepth = 5
	defer func() { value.MaxParseDepth = old }()
	deep := strings.Repeat("[", 10) + strings.Repeat("]", 10)
	if _, err := ParseJson([]byte(deep)); err == nil {
		t.Fatal("deeply nested JSON accepted despite depth limit")
	}
}

func TestJsonCodecFrameType(t *testing.T) {
	if MsgpackWireCodec.TextFrames() {
		t.Fatal("msgpack must use binary frames")
	}
	if !JsonWireCodec.TextFrames() {
		t.Fatal("json must use text frames")
	}
	if MsgpackWireCodec.Subprotocol() != SubprotocolMsgpack || JsonWireCodec.Subprotocol() != SubprotocolJson {
		t.Fatal("unexpected subprotocol tokens")
	}
}

func TestWireCodecForNegotiation(t *testing.T) {
	// Default policy (nil set): "" -> msgpack, "vrpc.json" -> json.
	if WireCodecFor("", nil) != MsgpackWireCodec {
		t.Fatal(`"" should select msgpack`)
	}
	if WireCodecFor(SubprotocolJson, nil) != JsonWireCodec {
		t.Fatal("vrpc.json should select json")
	}
	// Restricted to JSON-only: an offered json subprotocol matches.
	jsonOnly := []WireCodec{JsonWireCodec}
	if WireCodecFor(SubprotocolJson, jsonOnly) != JsonWireCodec {
		t.Fatal("json-only endpoint should select json")
	}
}

func TestJsonCodecDecodeAssertsMap(t *testing.T) {
	if _, err := JsonWireCodec.Decode([]byte(`[1,2,3]`)); err == nil {
		t.Fatal("a JSON array must be rejected as a non-map envelope")
	}
	m, err := JsonWireCodec.Decode([]byte(`{"t":1,"rid":0}`))
	if err != nil {
		t.Fatal(err)
	}
	if got := m.GetString("t"); got != nil {
		// "t" is a number; GetString returns nil for a non-string
	}
	if n, ok := GetNumberField(m, "t"); !ok || n.Long() != 1 {
		t.Fatalf("decoded envelope missing t=1")
	}
}
