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

package valueserver

import (
	"github.com/consensusdb/value"
	vrpc "github.com/consensusdb/value-rpc/valuerpc"
	"github.com/pkg/errors"
	"go.uber.org/atomic"
	"time"
)

/**
@author Alex Shvid
*/

var IncomingQueueCap = 4096

type servingRequest struct {
	ft               functionType
	requestId        value.Number
	inC              chan value.Value
	throttleOutgoing atomic.Int64

	closed           atomic.Bool
}

func NewServingRequest(ft functionType, requestId value.Number) *servingRequest {

	sr := &servingRequest{
		ft:        ft,
		requestId: requestId,
	}

	if ft == incomingStream || ft == chat {
		sr.inC = make(chan value.Value, IncomingQueueCap)
	}

	return sr
}

func (t *servingRequest) Close() {
	if t.closed.CAS(false, true) {
		if t.inC != nil {
			close(t.inC)
		}
	}
}

func (t *servingRequest) serveRunningRequest(msgType vrpc.MessageType, req value.Map, cli *servingClient) error {

	switch msgType {

	case vrpc.CancelRequest:
		return t.closeRequest(cli)

	case vrpc.StreamValue:
		return t.incomingStreamValue(req)

	case vrpc.StreamEnd:
		return t.incomingStreamEnd(req, cli)

	case vrpc.ThrottleIncrease:
		t.throttleOutgoing.Inc()

	case vrpc.ThrottleDecrease:
		t.throttleOutgoing.Dec()

	default:
		return errors.Errorf("unknown message type in %s", req.String())

	}

	return nil

}

func (t *servingRequest) incomingStreamValue(req value.Map) error {

	if t.inC == nil {
		return errors.Errorf("incoming value stream not found in serving request for %d", t.requestId)
	}

	if value, ok := req.Get(vrpc.ValueField); ok {
		t.inC <- value
	}

	return nil
}

func (t *servingRequest) incomingStreamEnd(req value.Map, cli *servingClient) error {

	if t.inC == nil {
		return errors.Errorf("incoming end stream not found in serving request for %d", t.requestId)
	}

	if value, ok := req.Get(vrpc.ValueField); ok {
		t.inC <- value
	}

	return t.closeRequest(cli)
}

func (t *servingRequest) closeRequest(cli *servingClient) error {
	cli.deleteRequest(t.requestId)
	t.Close()
	cli.canceledRequests.Delete(t.requestId)
	return nil
}

func (t *servingRequest) outgoingStreamer(outC <-chan value.Value, cli *servingClient) {

	cli.send(StreamReady(t.requestId))

	for {

		val, ok := <-outC
		if !ok || t.closed.Load() {
			cli.send(StreamEnd(t.requestId, val))
			if t.ft == outgoingStream {
				t.closeRequest(cli)
			}
			break
		}

		cli.send(StreamValue(t.requestId, val))

		th := t.throttleOutgoing.Load()
		if th > 0 {
			time.Sleep(time.Millisecond * time.Duration(th))
		}

	}

}
