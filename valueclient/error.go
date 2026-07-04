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
var ErrReverseIdNotNegative = xerrors.New("server-initiated request must use a negative request id")

// ErrConnectionLost fails an in-flight request when the connection drops and is
// re-established (the reconnect policy's fail-fast outcome). It carries
// CodeUnavailable, so callers can detect a retryable failure with
// valuerpc.CodeOf(err) == valuerpc.CodeUnavailable.
var ErrConnectionLost = valuerpc.NewError(valuerpc.CodeUnavailable, "connection lost during reconnect")

// ErrClientClosed fails an in-flight request when the client is closed
// (Close()). It carries CodeUnavailable so a caller blocked on a call or
// draining a stream unblocks with a clear, retryable-classified error instead
// of hanging or timing out.
var ErrClientClosed = valuerpc.NewError(valuerpc.CodeUnavailable, "client closed")

type ErrorHandler interface {
	BadConnection(err error)

	ProtocolError(resp value.Map, err error)

	StreamError(requestId int64, err error)
}
