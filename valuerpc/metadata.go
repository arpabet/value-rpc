/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valuerpc

import (
	"context"

	"go.arpabet.com/value"
)

// Metadata is a set of string key/value pairs carried with a request — the
// transport for distributed-trace context (W3C traceparent/tracestate), baggage,
// or any cross-cutting header. The client injects it from the call's context and
// the server attaches it to the handler's context, so OpenTelemetry (or any
// propagator) rides across the RPC boundary without value-rpc depending on it.
type Metadata = map[string]string

type metadataKey struct{}

// ContextWithMetadata returns a copy of ctx carrying md. MetadataFromContext
// reads it back; the server uses this to expose incoming request metadata to
// handlers.
func ContextWithMetadata(ctx context.Context, md Metadata) context.Context {
	return context.WithValue(ctx, metadataKey{}, md)
}

// MetadataFromContext returns the metadata attached to ctx, or nil.
func MetadataFromContext(ctx context.Context) Metadata {
	md, _ := ctx.Value(metadataKey{}).(Metadata)
	return md
}

// EncodeMetadata builds the envelope value for md (string->string). Returns nil
// for empty metadata so no field is added.
func EncodeMetadata(md Metadata) value.Map {
	if len(md) == 0 {
		return nil
	}
	m := make(map[string]value.Value, len(md))
	for k, v := range md {
		m[k] = value.Utf8(v)
	}
	return value.ImmutableMapOf(m)
}

// DecodeMetadata reads the metadata map carried in a request envelope, or nil
// when absent/empty.
func DecodeMetadata(req value.Map) Metadata {
	v := req.Get(MetadataField)
	if v == nil || v.Kind() != value.MAP {
		return nil
	}
	hm := v.(value.Map).HashMap()
	if len(hm) == 0 {
		return nil
	}
	md := make(Metadata, len(hm))
	for k, val := range hm {
		if s, ok := val.(value.String); ok {
			md[k] = s.String()
		}
	}
	return md
}
