/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valueclient

import (
	"context"

	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valuerpc"
)

// CallUnary is a typed wrapper over Client.CallFunction: it encodes a typed
// request with reqCodec, invokes the unary function name, and decodes the result
// with respCodec. Define the codecs once per message type (see valuerpc.Codec)
// to get statically-typed calls over the dynamic wire.
func CallUnary[Req, Resp any](
	ctx context.Context, cli Client, name string,
	req Req, reqCodec valuerpc.Codec[Req], respCodec valuerpc.Codec[Resp],
) (Resp, error) {
	var zero Resp
	res, err := cli.CallFunction(ctx, name, reqCodec.Encode(req))
	if err != nil {
		return zero, err
	}
	return respCodec.Decode(res)
}

// GetStreamTyped is a typed wrapper over Client.GetStream: it encodes a typed
// request and returns a channel of decoded typed values. A decode error stops
// the stream and is reported on errp (read it after the channel closes). receiveCap
// is the underlying receive buffer.
func GetStreamTyped[Req, Resp any](
	ctx context.Context, cli Client, name string,
	req Req, receiveCap int, reqCodec valuerpc.Codec[Req], respCodec valuerpc.Codec[Resp],
	errp *error,
) (<-chan Resp, int64, error) {
	raw, id, err := cli.GetStream(ctx, name, reqCodec.Encode(req), receiveCap)
	if err != nil {
		return nil, 0, err
	}
	out := make(chan Resp, receiveCap)
	go func() {
		defer close(out)
		for v := range raw {
			decoded, derr := respCodec.Decode(v)
			if derr != nil {
				if errp != nil {
					*errp = derr
				}
				// Tell the server to stop and retire the request locally (which
				// closes raw), then drain so the pump goroutine is released.
				cli.CancelRequest(id)
				for range raw {
				}
				return
			}
			select {
			case out <- decoded:
			case <-ctx.Done():
				// The stream is torn down with ctx (watchContext); do not stay
				// blocked on a consumer that stopped reading.
				return
			}
		}
	}()
	return out, id, nil
}

// PutStreamTyped is a typed wrapper over Client.PutStream: it encodes each typed
// value from in onto the client stream. It returns once the stream is established
// (the encode+send pump runs until in is closed or ctx is cancelled — the same
// ctx also bounds the underlying stream, so cancelling it tears both down).
func PutStreamTyped[Req, Val any](
	ctx context.Context, cli Client, name string,
	req Req, reqCodec valuerpc.Codec[Req], valCodec valuerpc.Codec[Val], in <-chan Val,
) error {
	raw := make(chan value.Value)
	// Establish first: if PutStream fails nobody would ever read raw and the
	// encoder goroutine would block forever on its first send.
	if err := cli.PutStream(ctx, name, reqCodec.Encode(req), raw); err != nil {
		return err
	}
	go func() {
		defer close(raw)
		for {
			select {
			case v, ok := <-in:
				if !ok {
					return
				}
				select {
				case raw <- valCodec.Encode(v):
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return nil
}
