/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valueserver

import (
	"context"

	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valuerpc"
)

// AddUnary registers a typed unary handler: the wire request is decoded with
// reqCodec into Req, the handler runs, and its Resp is encoded with respCodec
// back onto the wire. A decode failure is reported to the caller as a
// CodeInvalidArgument error. Pair with valueclient.CallUnary.
func AddUnary[Req, Resp any](
	s Server, name string,
	reqCodec valuerpc.Codec[Req], respCodec valuerpc.Codec[Resp],
	fn func(ctx context.Context, req Req) (Resp, error),
) error {
	return s.AddFunction(name, valuerpc.Any, valuerpc.Any,
		func(ctx context.Context, args value.Value) (value.Value, error) {
			req, err := reqCodec.Decode(args)
			if err != nil {
				return nil, valuerpc.NewError(valuerpc.CodeInvalidArgument, "decode request: %v", err)
			}
			resp, err := fn(ctx, req)
			if err != nil {
				return nil, err
			}
			return respCodec.Encode(resp), nil
		})
}

// AddOutgoingStreamTyped registers a typed server-streaming handler: the request
// is decoded with reqCodec, and each typed value the handler emits is encoded
// with respCodec. Pair with valueclient.GetStreamTyped.
func AddOutgoingStreamTyped[Req, Resp any](
	s Server, name string,
	reqCodec valuerpc.Codec[Req], respCodec valuerpc.Codec[Resp],
	fn func(ctx context.Context, req Req) (<-chan Resp, error),
) error {
	return s.AddOutgoingStream(name, valuerpc.Any,
		func(ctx context.Context, args value.Value) (<-chan value.Value, error) {
			req, err := reqCodec.Decode(args)
			if err != nil {
				return nil, valuerpc.NewError(valuerpc.CodeInvalidArgument, "decode request: %v", err)
			}
			typed, err := fn(ctx, req)
			if err != nil {
				return nil, err
			}
			out := make(chan value.Value)
			go func() {
				defer close(out)
				for v := range typed {
					out <- respCodec.Encode(v)
				}
			}()
			return out, nil
		})
}
