/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valuerpc

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"strconv"
	"strings"

	"go.arpabet.com/value"
	"golang.org/x/xerrors"
)

// WireCodec serializes a message envelope for one WebSocket connection. It is the
// per-connection seam that lets the WebSocket transport speak either MessagePack
// (binary frames, the default and only codec on TCP/Unix/TLS/QUIC) or JSON (text
// frames), negotiated once at the opening handshake by subprotocol. Everything
// above the transport operates on value.Map regardless of codec.
type WireCodec interface {
	// Name identifies the codec and is the subprotocol suffix: "msgpack", "json".
	Name() string
	// Subprotocol is the WebSocket subprotocol token a client offers (and the
	// server echoes) to select this codec: "vrpc.msgpack" or "vrpc.json". A
	// client that offers no subprotocol at all is served msgpack, so a client
	// that predates the negotiation keeps working unchanged.
	Subprotocol() string
	// TextFrames reports whether messages ride in WebSocket text frames (JSON)
	// rather than binary frames (msgpack).
	TextFrames() bool
	// Encode serializes an envelope. Decode parses one, asserting a top-level MAP
	// and applying the shared decode limits (limits also bound the msgpack path).
	Encode(msg value.Map) ([]byte, error)
	Decode(data []byte) (value.Map, error)
}

// Subprotocol tokens negotiated on the WebSocket handshake (RFC 6455
// Sec-WebSocket-Protocol) — the only negotiation channel a browser can reach,
// since browsers cannot set arbitrary request headers on a WebSocket. A client
// that offers neither selects msgpack (full backward compatibility).
const (
	SubprotocolMsgpack = "vrpc.msgpack"
	SubprotocolJson    = "vrpc.json"
)

var (
	// MsgpackWireCodec is the default codec: value.Pack / value.Unpack in binary
	// frames — byte-for-byte today's WebSocket behavior.
	MsgpackWireCodec WireCodec = msgpackCodec{}
	// JsonWireCodec encodes with value's JSON conventions (value.Jsonify) in text
	// frames and decodes with ParseJson.
	JsonWireCodec WireCodec = jsonCodec{}
)

// WireCodecFor returns the codec selected by a negotiated subprotocol string
// (as reported by websocket.Conn.Subprotocol): JSON for SubprotocolJson,
// msgpack for "" or SubprotocolMsgpack. codecs restricts what the server
// accepts; a nil/empty set means both are enabled.
func WireCodecFor(subprotocol string, codecs []WireCodec) WireCodec {
	for _, c := range codecs {
		if c.Subprotocol() == subprotocol {
			return c
		}
	}
	if len(codecs) == 0 {
		// Default policy: both codecs enabled.
		if subprotocol == SubprotocolJson {
			return JsonWireCodec
		}
		return MsgpackWireCodec
	}
	// Restricted set and no exact match: fall back to the first enabled codec
	// (the server's preference) when the client offered nothing, else it is a
	// mismatch the caller handles.
	return codecs[0]
}

type msgpackCodec struct{}

func (msgpackCodec) Name() string        { return "msgpack" }
func (msgpackCodec) Subprotocol() string { return SubprotocolMsgpack }
func (msgpackCodec) TextFrames() bool    { return false }

func (msgpackCodec) Encode(msg value.Map) ([]byte, error) {
	return value.Pack(msg)
}

func (msgpackCodec) Decode(data []byte) (value.Map, error) {
	msg, err := value.Unpack(data, true)
	if err != nil {
		return nil, xerrors.Errorf("msgpack unpack, %v", err)
	}
	if msg.Kind() != value.MAP {
		return nil, xerrors.New("expected msgpack map")
	}
	return msg.(value.Map), nil
}

type jsonCodec struct{}

func (jsonCodec) Name() string        { return "json" }
func (jsonCodec) Subprotocol() string { return SubprotocolJson }
func (jsonCodec) TextFrames() bool    { return true }

func (jsonCodec) Encode(msg value.Map) ([]byte, error) {
	// value.Jsonify emits JSON with sorted map keys, the "base64," and
	// "data:application/x-msgpack-ext;" string conventions, and null for a
	// double that is NaN or infinite. It is JSON for every value since value
	// v1.6.2: before, a control character in a text or a quote in a map key gave
	// a frame no JSON reader took. It returns a string; the extra copy is
	// negligible at browser scale.
	return []byte(value.Jsonify(msg)), nil
}

func (jsonCodec) Decode(data []byte) (value.Map, error) {
	v, err := ParseJson(data)
	if err != nil {
		return nil, err
	}
	if v == nil || v.Kind() != value.MAP {
		return nil, xerrors.New("expected json object envelope")
	}
	return v.(value.Map), nil
}

// ParseJson decodes canonical value JSON — the form value.Jsonify / PrintJSON
// emits — back into a value.Value. It is the decode half missing from the value
// library (which ships only the encode direction); the conventions are value's:
//
//   - null/true/false            -> Null / Bool
//   - integer number             -> Long, or BigInt when it overflows int64
//   - decimal/exponent number    -> Double
//   - "base64,<rawstd>"          -> RAW bytes (value.ParseString)
//   - "data:application/x-msgpack-ext;base64,<tag+data>" -> UNKNOWN ext
//   - any other string           -> UTF8
//   - array / object             -> List / Map (object keys sorted on pack;
//     duplicate key -> last wins)
//
// It enforces the same decode limits as the msgpack path (MaxParseDepth,
// MaxParseCollectionLen, MaxParseByteLen) so a hostile browser cannot force an
// oversized allocation or deep-nesting stack blow-up; MaxFrameSize already
// bounds the total input at the transport.
//
// Asymmetry (documented, matches value's encode side): BIGINT and DECIMAL encode
// as "0x…" strings and TIMESTAMP (value v1.5.0) as an RFC 3339 string, and none
// of them round-trips back — they decode as UTF8 strings, because a string is
// never re-typed by its content. Model big/precise numbers and instants as
// strings at the app level for JSON APIs, or use the msgpack codec where they
// round-trip natively.
func ParseJson(data []byte) (value.Value, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var raw interface{}
	if err := dec.Decode(&raw); err != nil {
		return nil, xerrors.Errorf("value: json decode: %w", err)
	}
	if dec.More() {
		return nil, xerrors.New("value: trailing data after JSON value")
	}
	return jsonToValue(raw, 0)
}

func jsonToValue(raw interface{}, depth int) (value.Value, error) {
	if value.MaxParseDepth > 0 && depth > value.MaxParseDepth {
		return nil, xerrors.Errorf("value: max parse depth %d exceeded", value.MaxParseDepth)
	}
	switch t := raw.(type) {
	case nil:
		return value.Null, nil
	case bool:
		return value.Boolean(t), nil
	case json.Number:
		return jsonNumberToValue(t)
	case string:
		return jsonStringToValue(t)
	case []interface{}:
		if value.MaxParseCollectionLen > 0 && len(t) > value.MaxParseCollectionLen {
			return nil, xerrors.Errorf("value: json array length %d exceeds limit %d", len(t), value.MaxParseCollectionLen)
		}
		items := make([]value.Value, len(t))
		for i, e := range t {
			ev, err := jsonToValue(e, depth+1)
			if err != nil {
				return nil, err
			}
			items[i] = ev
		}
		return value.ImmutableList(items), nil
	case map[string]interface{}:
		if value.MaxParseCollectionLen > 0 && len(t) > value.MaxParseCollectionLen {
			return nil, xerrors.Errorf("value: json object size %d exceeds limit %d", len(t), value.MaxParseCollectionLen)
		}
		m := make(map[string]value.Value, len(t))
		for k, e := range t {
			ev, err := jsonToValue(e, depth+1)
			if err != nil {
				return nil, err
			}
			m[k] = ev
		}
		return value.ImmutableMapOf(m), nil
	default:
		return nil, xerrors.Errorf("value: unsupported json type %T", raw)
	}
}

func jsonNumberToValue(n json.Number) (value.Value, error) {
	s := n.String()
	// Integer syntax (no fraction or exponent) decodes as LONG, widening to
	// BIGINT past the int64 range so no precision is silently lost.
	if !strings.ContainsAny(s, ".eE") {
		if i, err := strconv.ParseInt(s, 10, 64); err == nil {
			return value.Long(i), nil
		}
		if bi, ok := new(big.Int).SetString(s, 10); ok {
			return value.BigInt(bi), nil
		}
	}
	f, err := n.Float64()
	if err != nil {
		return nil, xerrors.Errorf("value: invalid json number %q: %w", s, err)
	}
	return value.Double(f), nil
}

func jsonStringToValue(s string) (value.Value, error) {
	if value.MaxParseByteLen > 0 && len(s) > value.MaxParseByteLen {
		return nil, xerrors.Errorf("value: json string length %d exceeds limit %d", len(s), value.MaxParseByteLen)
	}
	// A msgpack extension value: "data:application/x-msgpack-ext;base64,<tag+data>".
	if strings.HasPrefix(s, value.UnknownPrefix) {
		rest := s[len(value.UnknownPrefix):]
		if strings.HasPrefix(rest, value.Base64Prefix) {
			payload, err := base64.RawStdEncoding.DecodeString(rest[len(value.Base64Prefix):])
			if err == nil && len(payload) >= 1 {
				return value.Unknown(payload), nil
			}
		}
		return value.Utf8(s), nil
	}
	// value.ParseString reverses the "base64,<rawstd>" convention to RAW bytes,
	// otherwise returns a UTF8 string.
	return value.ParseString(s), nil
}
