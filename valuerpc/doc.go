/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

// Package valuerpc defines vRPC's wire protocol and pluggable transport layer.
//
// vRPC is a small, schemaless RPC framework for Go: there is no IDL and no code
// generation. Every message is a MessagePack [go.arpabet.com/value.Map] envelope,
// and the four interaction patterns (unary, server-streaming, client-streaming,
// and bidirectional chat) are multiplexed over a single connection keyed by
// request id.
//
// This package holds the pieces shared by the client and server:
//
//   - the message envelope, message types, and field names;
//   - MsgConn and NewMsgConn — the length-prefix framing codec over any
//     io.ReadWriteCloser;
//   - the Listener / Dialer transport seam, with built-in stream (TCP/Unix),
//     WebSocket, TLS/mTLS, and in-memory transports, plus a bring-your-own-
//     connection seam (NewFuncDialer / NewAcceptListener);
//   - Code, Error, and CodeOf — machine-readable error codes;
//   - Metadata and the context helpers for trace-context propagation;
//   - the Metrics interface; and
//   - Codec[T] for optional statically-typed call sites.
//
// Most applications use [go.arpabet.com/value-rpc/valueserver] and
// [go.arpabet.com/value-rpc/valueclient] rather than this package directly; reach
// for valuerpc to implement a custom transport or to share codecs, codes, and
// metadata across both ends. The QUIC transport lives in the separate module
// [go.arpabet.com/value-rpc/quic].
package valuerpc
