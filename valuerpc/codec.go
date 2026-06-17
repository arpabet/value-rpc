/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valuerpc

import "go.arpabet.com/value"

// Codec bridges a typed Go value T and the dynamic value.Value wire
// representation vRPC transmits. Define one per message type (encode + decode)
// to get statically-typed calls and handlers over the schemaless transport — see
// valueclient.CallUnary / valueserver.AddUnary.
//
// Because vRPC is schemaless, the mapping is explicit and yours: Encode builds a
// value.Map/value.List/scalar from T; Decode reads it back. This keeps the wire
// flexible (fields can be added without codegen) while call sites stay typed.
type Codec[T any] struct {
	Encode func(T) value.Value
	Decode func(value.Value) (T, error)
}

// ValueCodec is the identity Codec for callers that already work in value.Value
// (no conversion). Useful as the Req or Resp codec when one side is dynamic.
func ValueCodec() Codec[value.Value] {
	return Codec[value.Value]{
		Encode: func(v value.Value) value.Value { return v },
		Decode: func(v value.Value) (value.Value, error) { return v, nil },
	}
}
