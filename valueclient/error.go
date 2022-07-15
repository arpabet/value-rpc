/**
  Copyright (c) 2022 Arpabet, LLC. All rights reserved.
*/

package valueclient

import (
	"errors"
	"go.arpabet.com/value"
)


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
