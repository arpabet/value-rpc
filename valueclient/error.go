/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valueclient

import (
	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valuerpc"
	"golang.org/x/xerrors"
)

var ErrNoResponse = xerrors.New("no response")
var ErrNoMessageType = xerrors.New("message type not found")
var ErrIdFieldNotFound = xerrors.New("request id not found")
var ErrTimeoutError = xerrors.New("timeout error")
var ErrRequestNotFound = xerrors.New("request not found")
var ErrUnsupportedMessageType = xerrors.New("message type not supported")

// ErrConnectionLost fails an in-flight request when the connection drops and is
// re-established (the reconnect policy's fail-fast outcome). It carries
// CodeUnavailable, so callers can detect a retryable failure with
// valuerpc.CodeOf(err) == valuerpc.CodeUnavailable.
var ErrConnectionLost = valuerpc.NewError(valuerpc.CodeUnavailable, "connection lost during reconnect")

type ErrorHandler interface {
	BadConnection(err error)

	ProtocolError(resp value.Map, err error)

	StreamError(requestId int64, err error)
}
