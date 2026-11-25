/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valuequic

import (
	"fmt"
	"testing"

	"go.arpabet.com/value-rpc/valuerpc"
)

// StreamsHeld reports how many streams a connection of this transport holds at
// this end: one for each request that is not over at both of its halves. It is
// here for the tests of the other package, which have no other way to see that
// a stream was freed, or that one was opened that should not have been.
func StreamsHeld(conn valuerpc.MsgConn) int {
	t := conn.(*quicMsgConn)
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.streams)
}

// MessageName names a message type, for what the tests of both packages report.
func MessageName(mt valuerpc.MessageType) string {
	switch mt {
	case valuerpc.HandshakeRequest:
		return "HandshakeRequest"
	case valuerpc.HandshakeResponse:
		return "HandshakeResponse"
	case valuerpc.FunctionRequest:
		return "FunctionRequest"
	case valuerpc.FunctionResponse:
		return "FunctionResponse"
	case valuerpc.GetStreamRequest:
		return "GetStreamRequest"
	case valuerpc.PutStreamRequest:
		return "PutStreamRequest"
	case valuerpc.ChatRequest:
		return "ChatRequest"
	case valuerpc.ErrorResponse:
		return "ErrorResponse"
	case valuerpc.StreamReady:
		return "StreamReady"
	case valuerpc.StreamValue:
		return "StreamValue"
	case valuerpc.StreamEnd:
		return "StreamEnd"
	case valuerpc.CancelRequest:
		return "CancelRequest"
	case valuerpc.StreamCredit:
		return "StreamCredit"
	}
	return fmt.Sprintf("message type %d", int64(mt))
}

// TestQUIC_FinRules states, for every kind of request, which message is the last a
// side sends on the stream of the request: the one it ends its half with when
// it writes it (finOnWrite), or the one it ends its half on reading
// (finOnRead). Both ends of a connection have to go by the same table, in any
// implementation, or streams are not freed.
func TestQUIC_FinRules(t *testing.T) {
	const (
		hsReq   = valuerpc.HandshakeRequest
		hsRes   = valuerpc.HandshakeResponse
		fnReq   = valuerpc.FunctionRequest
		fnRes   = valuerpc.FunctionResponse
		getReq  = valuerpc.GetStreamRequest
		putReq  = valuerpc.PutStreamRequest
		chatReq = valuerpc.ChatRequest
		errRes  = valuerpc.ErrorResponse
		ready   = valuerpc.StreamReady
		val     = valuerpc.StreamValue
		end     = valuerpc.StreamEnd
		cancel  = valuerpc.CancelRequest
		credit  = valuerpc.StreamCredit
	)
	all := []valuerpc.MessageType{hsReq, hsRes, fnReq, fnRes, getReq, putReq, chatReq, errRes, ready, val, end, cancel, credit}
	set := func(mts ...valuerpc.MessageType) map[valuerpc.MessageType]bool {
		m := make(map[valuerpc.MessageType]bool, len(mts))
		for _, mt := range mts {
			m[mt] = true
		}
		return m
	}

	for _, tc := range []struct {
		side      string
		initiator bool
		request   valuerpc.MessageType
		onWrite   map[valuerpc.MessageType]bool // ends its half when it writes one of these
		onRead    map[valuerpc.MessageType]bool // ends its half when it reads one of these
	}{
		{"the initiator of a handshake", true, hsReq, set(end, cancel), set(hsRes)},
		{"the responder of a handshake", false, hsReq, set(hsRes, fnRes, errRes, end), set()},

		{"the initiator of a call", true, fnReq, set(end, cancel), set(fnRes, errRes)},
		{"the responder of a call", false, fnReq, set(hsRes, fnRes, errRes, end), set()},

		// The initiator goes on granting credit until the stream ends.
		{"the initiator of a get-stream", true, getReq, set(end, cancel), set(end, errRes)},
		{"the responder of a get-stream", false, getReq, set(hsRes, fnRes, errRes, end), set()},

		// The responder goes on granting credit until the stream ends.
		{"the initiator of a put-stream", true, putReq, set(end, cancel), set()},
		{"the responder of a put-stream", false, putReq, set(hsRes, fnRes, errRes, end), set(end, errRes, cancel)},

		// A chat is a put-stream and a get-stream in one. Its initiator goes on
		// granting credit after its own StreamEnd, until the responder ends the
		// chat: with its StreamEnd, or with the ErrorResponse it refuses it with.
		{"the initiator of a chat", true, chatReq, set(cancel), set(end, errRes)},
		{"the responder of a chat", false, chatReq, set(hsRes, fnRes, errRes, end), set()},
	} {
		for _, mt := range all {
			if got, want := finOnWrite(tc.initiator, mt, tc.request), tc.onWrite[mt]; got != want {
				t.Errorf("%s writes %s: ends its half = %v, want %v", tc.side, MessageName(mt), got, want)
			}
			if got, want := finOnRead(mt, tc.request, tc.initiator), tc.onRead[mt]; got != want {
				t.Errorf("%s reads %s: ends its half = %v, want %v", tc.side, MessageName(mt), got, want)
			}
		}
	}

	// A stream is opened by the message that opens a request, and by no other.
	opens := set(hsReq, fnReq, getReq, putReq, chatReq)
	for _, mt := range all {
		if got, want := opensRequest(mt), opens[mt]; got != want {
			t.Errorf("%s opens a stream = %v, want %v", MessageName(mt), got, want)
		}
	}
}
