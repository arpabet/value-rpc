/*
 *
 * Copyright 2020-present Arpabet Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package rpc

import (
	"github.com/consensusdb/value"
	"github.com/pkg/errors"
)

/**
Alex Shvid
*/

type MessageType int64

const (
	HandshakeRequest MessageType = iota
	HandshakeResponse
	FunctionRequest
	FunctionResponse
	GetStreamRequest
	PutStreamRequest
	ChatRequest
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
var ValueField = "val"
var ErrorField = "err"

var HandshakeRequestId = int64(-1)

func NewClientRequest(clientId int64) value.Table {

	msg := value.Map()
	msg.Put(MagicField, value.Utf8(Magic))
	msg.Put(VersionField, value.Double(Version))
	msg.Put(MessageTypeField, HandshakeRequest.Long())
	msg.Put(RequestIdField, value.Long(HandshakeRequestId))
	msg.Put(ClientIdField, value.Long(clientId))

	return msg
}

func NewClientResponse() value.Table {

	msg := value.Map()
	msg.Put(MagicField, value.Utf8(Magic))
	msg.Put(VersionField, value.Double(Version))
	msg.Put(MessageTypeField, HandshakeResponse.Long())
	msg.Put(RequestIdField, value.Long(HandshakeRequestId))

	return msg

}

func ValidMagicAndVersion(req value.Table) bool {
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

func ServerResult(resp value.Table) (value.Value, error) {
	err := resp.Get(ErrorField)
	if err != nil {
		return nil, errors.Errorf("SERVER_ERROR %v", err)
	}
	return resp.Get(ValueField), nil
}
