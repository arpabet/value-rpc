/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valueclient

import (
	"context"
	"testing"
	"time"

	"go.arpabet.com/value-rpc/valuerpc"
	"go.uber.org/zap"
)

func TestDefaultClientConfig(t *testing.T) {
	cfg := newClientConfig(nil)
	if cfg.sendingCap != DefaultSendingCap {
		t.Errorf("sendingCap = %d, want %d", cfg.sendingCap, DefaultSendingCap)
	}
	if cfg.timeoutMls != DefaultTimeoutMls {
		t.Errorf("timeoutMls = %d, want %d", cfg.timeoutMls, DefaultTimeoutMls)
	}
	if cfg.maxPending != valuerpc.DefaultMaxPending {
		t.Errorf("maxPending = %d, want %d", cfg.maxPending, valuerpc.DefaultMaxPending)
	}
	if cfg.maxFrameSize != valuerpc.MaxFrameSize {
		t.Errorf("maxFrameSize = %d, want %d", cfg.maxFrameSize, valuerpc.MaxFrameSize)
	}
	if cfg.dialTimeout != DefaultDialTimeout {
		t.Errorf("dialTimeout = %v, want %v", cfg.dialTimeout, DefaultDialTimeout)
	}
	if cfg.keepAlive != KeepAlivePeriod {
		t.Errorf("keepAlive = %v, want %v", cfg.keepAlive, KeepAlivePeriod)
	}
	if cfg.writeTimeout != DefaultTimeout {
		t.Errorf("writeTimeout = %v, want %v", cfg.writeTimeout, DefaultTimeout)
	}
	if cfg.logger == nil {
		t.Error("logger should default to a non-nil (no-op) logger")
	}
	if cfg.metrics == nil {
		t.Error("metrics should default to NopMetrics")
	}
	if cfg.metadata != nil {
		t.Error("metadata injector should default to nil")
	}
	if cfg.reconnect.ReplayUnary != nil || cfg.reconnect.MaxAttempts != 0 {
		t.Error("reconnect policy should default to the zero value (fail-fast, no retry)")
	}
}

func TestClientOptionsApplied(t *testing.T) {
	logger := zap.NewNop()
	var m valuerpc.Metrics = valuerpc.NopMetrics{}
	md := func(context.Context) valuerpc.Metadata { return valuerpc.Metadata{"k": "v"} }
	pol := ReconnectPolicy{
		ReplayUnary:    func(string) bool { return true },
		InitialBackoff: 7 * time.Millisecond,
		MaxBackoff:     8 * time.Second,
		MaxAttempts:    5,
		Jitter:         true,
	}

	cfg := newClientConfig([]ClientOption{
		WithSendingCap(7),
		WithTimeout(123),
		WithStreamMaxPending(9),
		WithKeepAlivePeriod(2 * time.Second),
		WithWriteTimeout(3 * time.Second),
		WithMaxFrameSize(4096),
		WithDialTimeout(5 * time.Second),
		WithMetrics(m),
		WithMetadata(md),
		WithLogger(logger),
		WithReconnectPolicy(pol),
	})

	if cfg.sendingCap != 7 {
		t.Errorf("sendingCap = %d, want 7", cfg.sendingCap)
	}
	if cfg.timeoutMls != 123 {
		t.Errorf("timeoutMls = %d, want 123", cfg.timeoutMls)
	}
	if cfg.maxPending != 9 {
		t.Errorf("maxPending = %d, want 9", cfg.maxPending)
	}
	if cfg.keepAlive != 2*time.Second {
		t.Errorf("keepAlive = %v", cfg.keepAlive)
	}
	if cfg.writeTimeout != 3*time.Second {
		t.Errorf("writeTimeout = %v", cfg.writeTimeout)
	}
	if cfg.maxFrameSize != 4096 {
		t.Errorf("maxFrameSize = %d", cfg.maxFrameSize)
	}
	if cfg.dialTimeout != 5*time.Second {
		t.Errorf("dialTimeout = %v", cfg.dialTimeout)
	}
	if cfg.metadata == nil || cfg.metadata(context.Background())["k"] != "v" {
		t.Error("metadata injector not applied")
	}
	if cfg.reconnect.MaxAttempts != 5 || cfg.reconnect.ReplayUnary == nil || !cfg.reconnect.Jitter {
		t.Error("reconnect policy not applied")
	}
}

func TestNilLoggerAndMetricsCoerced(t *testing.T) {
	cfg := newClientConfig([]ClientOption{WithLogger(nil), WithMetrics(nil)})
	if cfg.logger == nil {
		t.Error("a nil logger must be coerced to the no-op logger")
	}
	if cfg.metrics == nil {
		t.Error("a nil metrics must be coerced to NopMetrics")
	}
}
