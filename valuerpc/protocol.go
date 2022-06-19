/**
    Copyright (c) 2020-2022 Arpabet, Inc.

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in
	all copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
	THE SOFTWARE.
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
	return value.EmptyMap().
		Put(MagicField, value.Utf8(Magic)).
		Put(VersionField, value.Double(Version)).
		Put(MessageTypeField, HandshakeRequest.Long()).
		Put(RequestIdField, value.Long(HandshakeRequestId)).
		Put(ClientIdField, value.Long(clientId))
}

func NewHandshakeResponse() value.Map {
	return value.EmptyMap().
		Put(MagicField, value.Utf8(Magic)).
		Put(VersionField, value.Double(Version)).
		Put(MessageTypeField, HandshakeResponse.Long()).
		Put(RequestIdField, value.Long(HandshakeRequestId))
}

func ValidMagicAndVersion(req value.Map) bool {
	magic := req.GetString(MagicField)
	if magic == nil || magic.String() != Magic {
		return false
	}
	version := req.GetNumber(MagicField)
	if version == nil || version.Double() > Version {
		return false
	}
	return true
}
