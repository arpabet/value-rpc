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

package valueclient

import (
	"errors"
	"arpabet.pkg.is/value"
)

/**
@author Alex Shvid
*/

var ErrNoResponse = errors.New("no response")
var ErrNoMessageType = errors.New("message type not found")
var ErrIdFieldNotFound = errors.New("request id not found")
var ErrTimeoutError = errors.New("timeout error")
var ErrRequestNotFound = errors.New("request not found")
var ErrUnsupportedMessageType = errors.New("message type not supported")

type ErrorHandler interface {
	BadConnection(err error)

	ProtocolError(resp value.Map, err error)

	StreamError(requestId int64, err error)
}
