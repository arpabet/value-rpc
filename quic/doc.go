/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

// Package valuequic provides a QUIC transport for vRPC.
//
// It is a separate module (go.arpabet.com/value-rpc/quic) so that the quic-go
// dependency only enters builds that actually use QUIC — the core
// go.arpabet.com/value-rpc module stays free of it.
//
// QUIC is always encrypted, so a *tls.Config is required; mutual TLS and
// valuerpc.PeerCertificates work over QUIC as they do over the TLS transport.
// Use NewServer / NewClient for the common case, or NewListener / NewDialer with
// [go.arpabet.com/value-rpc/valueserver.NewServerWithListener] and
// [go.arpabet.com/value-rpc/valueclient.NewClientWithDialer] for full control.
package valuequic
