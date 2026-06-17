/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valuerpc

import (
	"go.arpabet.com/value"
)

type MessageType int64

const (
	HandshakeRequest MessageType = iota
	HandshakeResponse
	FunctionRequest
	FunctionResponse
	GetStreamRequest
	PutStreamRequest
	ChatRequest
	ErrorResponse
	StreamReady
	StreamValue
	StreamEnd
	CancelRequest
	_ // reserved (was ThrottleIncrease, removed); kept to preserve wire numbering
	_ // reserved (was ThrottleDecrease, removed); kept to preserve wire numbering
	// StreamCredit grants the data sender permission to send N more stream values
	// (credit-based flow control). The receiver sends an initial window when the
	// stream opens and replenishes credit as it delivers values to the consumer,
	// so a fast producer can never overrun a slow consumer (lossless), buffering
	// stays bounded, and the shared connection loop is never blocked.
	StreamCredit
)

func (t MessageType) Long() value.Number {
	return value.Long(int64(t))
}

// Dialect names the wire-level fields and markers of the vRPC envelope. It lets an
// application change the on-the-wire field names (and the magic/version markers)
// to make its protocol incompatible-by-design with the default — e.g. to shed the
// obvious cleartext fingerprint that keyword-based DPI looks for, or to fork the
// protocol. Both peers MUST share the same Dialect to interoperate.
//
// A Dialect is a *protocol identity*, not a per-call tunable: it is read by the
// transport layer (the QUIC transport inspects the message-type and request-id
// fields to multiplex per-request streams), so it is a process-wide setting.
// Build one with NewDialect, override the fields you want, and install it via
// DefaultDialect at startup — before any connection is opened. Do not mutate it
// once connections exist.
//
// Caveat: renaming fields is "look-like-nothing" obfuscation. It removes the
// cleartext keyword fingerprint but does NOT hide message structure, size, or
// timing — statistical/ML DPI can still classify the traffic. Treat a Dialect as
// one cheap layer to combine with TLS and a traffic-shaping/mimicry transport, not
// as a substitute for encryption.
type Dialect struct {
	Magic              string
	Version            float64
	HandshakeRequestId int64

	MessageTypeField  string
	MagicField        string
	VersionField      string
	RequestIdField    string
	TimeoutField      string
	ClientIdField     string
	SessionTokenField string // server-issued session secret; gates resumption
	AuthField         string // client-supplied credential, validated by the server Authenticator
	FunctionNameField string
	ArgumentsField    string // allow multiple args if List value in function call
	ResultField       string // allow multiple results if List in function call
	ErrorField        string
	CodeField         string // ErrorResponse: machine-readable Code
	CreditField       string // StreamCredit: number of additional stream values granted
	MetadataField     string // request metadata (trace context, baggage): string->string map
	ValueField        string // streaming value field
}

// NewDialect returns the standard vRPC dialect (the default wire format). Copy it
// and override fields to define a custom, incompatible-by-design dialect, then
// install it via DefaultDialect.
func NewDialect() *Dialect {
	return &Dialect{
		Magic:              "vRPC",
		Version:            1.0,
		HandshakeRequestId: -1,
		MessageTypeField:   "t",
		MagicField:         "m",
		VersionField:       "v",
		RequestIdField:     "rid",
		TimeoutField:       "sla",
		ClientIdField:      "cid",
		SessionTokenField:  "tok",
		AuthField:          "auth",
		FunctionNameField:  "fn",
		ArgumentsField:     "args",
		ResultField:        "res",
		ErrorField:         "err",
		CodeField:          "code",
		CreditField:        "cr",
		MetadataField:      "md",
		ValueField:         "val",
	}
}

// DefaultDialect is the active wire dialect used by every transport, client, and
// server in this process. It defaults to the standard vRPC format; assign a custom
// *Dialect at startup (before opening connections) to change the wire format
// process-wide.
var DefaultDialect = NewDialect()

// NewHandshakeRequest builds the client handshake using dialect d. token is the
// session secret the server issued on the first handshake; it is empty on the
// first connect and resent on reconnect so the server can authorize resuming this
// clientId.
func (d *Dialect) NewHandshakeRequest(clientId int64, token string) value.Map {
	req := value.EmptyMap(true).
		Put(d.MagicField, value.Utf8(d.Magic)).
		Put(d.VersionField, value.Double(d.Version)).
		Put(d.MessageTypeField, HandshakeRequest.Long()).
		Put(d.RequestIdField, value.Long(d.HandshakeRequestId)).
		Put(d.ClientIdField, value.Long(clientId))
	if token != "" {
		req = req.Put(d.SessionTokenField, value.Utf8(token))
	}
	return req
}

// NewHandshakeResponse builds the server handshake reply using dialect d, carrying
// the session token the client must present to resume this session on a reconnect.
func (d *Dialect) NewHandshakeResponse(token string) value.Map {
	resp := value.EmptyMap(true).
		Put(d.MagicField, value.Utf8(d.Magic)).
		Put(d.VersionField, value.Double(d.Version)).
		Put(d.MessageTypeField, HandshakeResponse.Long()).
		Put(d.RequestIdField, value.Long(d.HandshakeRequestId))
	if token != "" {
		resp = resp.Put(d.SessionTokenField, value.Utf8(token))
	}
	return resp
}

// NewStreamCredit builds a StreamCredit message (dialect d) granting the data
// sender credit additional stream values for the given request.
func (d *Dialect) NewStreamCredit(requestId value.Number, credit int64) value.Map {
	return value.EmptyMap(true).
		Put(d.MessageTypeField, StreamCredit.Long()).
		Put(d.RequestIdField, requestId).
		Put(d.CreditField, value.Long(credit))
}

// ValidMagicAndVersion reports whether req carries dialect d's magic and a
// supported version.
func (d *Dialect) ValidMagicAndVersion(req value.Map) bool {
	magic, ok := GetStringField(req, d.MagicField)
	if !ok || magic.String() != d.Magic {
		return false
	}
	// BUG-1 fix: the version lives in VersionField ("v"), not MagicField ("m").
	version, ok := GetNumberField(req, d.VersionField)
	if !ok || version.Double() > d.Version {
		return false
	}
	return true
}

// The package-level helpers below operate on the active DefaultDialect.

func NewHandshakeRequest(clientId int64, token string) value.Map {
	return DefaultDialect.NewHandshakeRequest(clientId, token)
}

func NewHandshakeResponse(token string) value.Map {
	return DefaultDialect.NewHandshakeResponse(token)
}

func NewStreamCredit(requestId value.Number, credit int64) value.Map {
	return DefaultDialect.NewStreamCredit(requestId, credit)
}

func ValidMagicAndVersion(req value.Map) bool {
	return DefaultDialect.ValidMagicAndVersion(req)
}

// GetNumberField returns the NUMBER at field and true, or (nil, false) when the
// field is absent or not a number. Use this instead of Map.GetNumber when you
// need to distinguish "absent" from "zero": GetNumber returns value.Zero for a
// missing key, so `GetNumber(k) == nil` is always false (BUG-5).
func GetNumberField(m value.Map, field string) (value.Number, bool) {
	v := m.Get(field)
	if v == nil || v.Kind() != value.NUMBER {
		return nil, false
	}
	return v.(value.Number), true
}

// GetStringField returns the STRING at field and true, or (nil, false) when the
// field is absent or not a string. See GetNumberField for the rationale.
func GetStringField(m value.Map, field string) (value.String, bool) {
	v := m.Get(field)
	if v == nil || v.Kind() != value.STRING {
		return nil, false
	}
	return v.(value.String), true
}
