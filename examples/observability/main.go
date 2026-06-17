/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

// Command observability demonstrates the pluggable metrics sink (valuerpc.Metrics)
// and metadata / trace-context propagation: the client injects a trace id from the
// call context, the server surfaces it on the handler context, and both ends feed
// a metrics implementation (here a simple in-memory counter).
package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valueclient"
	"go.arpabet.com/value-rpc/valuerpc"
	"go.arpabet.com/value-rpc/valueserver"
	"go.uber.org/zap"
)

// counters is a minimal valuerpc.Metrics implementation. A real one would feed
// Prometheus or OpenTelemetry instruments.
type counters struct {
	mu       sync.Mutex
	requests int
	errors   int
	inflight int
}

func (c *counters) RequestBegin(string) { c.mu.Lock(); c.inflight++; c.mu.Unlock() }
func (c *counters) RequestEnd(_ string, code valuerpc.Code, _ time.Duration) {
	c.mu.Lock()
	c.requests++
	c.inflight--
	if code != valuerpc.CodeOK {
		c.errors++
	}
	c.mu.Unlock()
}
func (c *counters) StreamValue(string) {}
func (c *counters) Reconnect()         {}

// traceKey carries a trace id in the client's call context.
type traceKey struct{}

func main() {
	clientM, serverM := &counters{}, &counters{}

	srv, err := valueserver.NewMemServer("obs-demo", zap.NewNop(),
		valueserver.WithMetrics(serverM),
		// Turn incoming metadata into a real context value (an OTel propagator
		// would Extract a span context here instead).
		valueserver.WithMetadataExtractor(func(ctx context.Context, md valuerpc.Metadata) context.Context {
			return context.WithValue(ctx, traceKey{}, md["traceparent"])
		}))
	if err != nil {
		log.Fatal(err)
	}
	defer srv.Close()

	srv.AddFunction("work", valuerpc.Any, valuerpc.Any,
		func(ctx context.Context, a value.Value) (value.Value, error) {
			tp, _ := ctx.Value(traceKey{}).(string)
			fmt.Printf("  server handling request, traceparent=%q\n", tp)
			if a != nil && a.(value.String).String() == "bad" {
				return nil, valuerpc.NewError(valuerpc.CodeInvalidArgument, "bad input")
			}
			return value.Utf8("done"), nil
		})
	go srv.Run()

	cli := valueclient.NewMemClient("obs-demo",
		valueclient.WithMetrics(clientM),
		// Inject a trace id from the call context into every request's metadata
		// (an OTel propagator would Inject the W3C traceparent here).
		valueclient.WithMetadata(func(ctx context.Context) valuerpc.Metadata {
			if tp, ok := ctx.Value(traceKey{}).(string); ok {
				return valuerpc.Metadata{"traceparent": tp}
			}
			return nil
		}))
	if err := cli.Connect(); err != nil {
		log.Fatal(err)
	}
	defer cli.Close()

	ctx := context.WithValue(context.Background(), traceKey{}, "00-trace-abc-01")
	fmt.Println("calling work x3 (one fails):")
	cli.CallFunction(ctx, "work", value.Utf8("ok"))
	cli.CallFunction(ctx, "work", value.Utf8("ok"))
	cli.CallFunction(ctx, "work", value.Utf8("bad"))

	report := func(name string, c *counters) {
		c.mu.Lock()
		defer c.mu.Unlock()
		fmt.Printf("%s metrics: requests=%d errors=%d inflight=%d\n", name, c.requests, c.errors, c.inflight)
	}
	report("client", clientM)
	report("server", serverM)
}
