/*
 * Copyright (c) 2025 Karagatan LLC.
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
	ThrottleIncrease
	ThrottleDecrease
)

func (t MessageType) Long() value.Number {
	return value.Long(int64(t))
}

var Magic = "vRPC"
var Version = 1.0

var MessageTypeField = "t"
var MagicField = "m"
var VersionField = "v"
var RequestIdField = "rid"
var TimeoutField = "sla"
var ClientIdField = "cid"
var FunctionNameField = "fn"
var ArgumentsField = "args" // allow multiple args if List value in function call
var ResultField = "res"     // allow multiple results if List in function call
var ErrorField = "err"
var ValueField = "val" // streaming value field

var HandshakeRequestId = int64(-1)

func NewHandshakeRequest(clientId int64) value.Map {
	return value.EmptyMap(true).
		Put(MagicField, value.Utf8(Magic)).
		Put(VersionField, value.Double(Version)).
		Put(MessageTypeField, HandshakeRequest.Long()).
		Put(RequestIdField, value.Long(HandshakeRequestId)).
		Put(ClientIdField, value.Long(clientId))
}

func NewHandshakeResponse() value.Map {
	return value.EmptyMap(true).
		Put(MagicField, value.Utf8(Magic)).
		Put(VersionField, value.Double(Version)).
		Put(MessageTypeField, HandshakeResponse.Long()).
		Put(RequestIdField, value.Long(HandshakeRequestId))
}

func ValidMagicAndVersion(req value.Map) bool {
	magic, ok := GetStringField(req, MagicField)
	if !ok || magic.String() != Magic {
		return false
	}
	// BUG-1 fix: the version lives in VersionField ("v"), not MagicField ("m").
	version, ok := GetNumberField(req, VersionField)
	if !ok || version.Double() > Version {
		return false
	}
	return true
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
