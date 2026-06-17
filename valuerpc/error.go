/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valuerpc

import (
	"errors"
	"fmt"
)

// Code is a machine-readable RPC status, carried on the wire so callers can
// branch on the kind of failure instead of string-matching the message. The set
// mirrors a useful subset of gRPC status codes.
type Code int32

const (
	CodeOK                Code = iota // not an error
	CodeUnknown                       // unclassified failure (default for a bare handler error)
	CodeCanceled                      // the request was cancelled
	CodeInvalidArgument               // malformed request / args failed verification
	CodeDeadlineExceeded              // the deadline elapsed before completion
	CodeNotFound                      // no such function
	CodeResourceExhausted             // a limit was hit (busy, too many streams, flow-control overrun)
	CodeUnavailable                   // transport/stream failure; safe to retry
	CodeUnauthenticated               // authentication failed
	CodeInternal                      // handler or server-side internal error
)

func (c Code) String() string {
	switch c {
	case CodeOK:
		return "OK"
	case CodeCanceled:
		return "Canceled"
	case CodeInvalidArgument:
		return "InvalidArgument"
	case CodeDeadlineExceeded:
		return "DeadlineExceeded"
	case CodeNotFound:
		return "NotFound"
	case CodeResourceExhausted:
		return "ResourceExhausted"
	case CodeUnavailable:
		return "Unavailable"
	case CodeUnauthenticated:
		return "Unauthenticated"
	case CodeInternal:
		return "Internal"
	default:
		return "Unknown"
	}
}

// Error is a coded RPC error. Handlers may return one to control the code sent to
// the client (valuerpc.NewError(...)); the client returns one from a failed call,
// so callers can branch with errors.As / CodeOf instead of matching strings.
type Error struct {
	Code    Code
	Message string
}

func (e *Error) Error() string {
	return fmt.Sprintf("vrpc %s: %s", e.Code, e.Message)
}

// NewError builds a coded error.
func NewError(code Code, format string, args ...interface{}) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

// CodeOf reports the Code carried by err: the code of a *valuerpc.Error
// (anywhere in the chain), CodeOK for a nil error, or CodeUnknown otherwise.
func CodeOf(err error) Code {
	if err == nil {
		return CodeOK
	}
	var e *Error
	if errors.As(err, &e) {
		return e.Code
	}
	return CodeUnknown
}
