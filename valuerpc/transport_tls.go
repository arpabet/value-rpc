/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valuerpc

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"time"
)

// TLS transport: TCP wrapped in crypto/tls. It reuses the length-prefix framing
// (a *tls.Conn is a net.Conn), so only the dial/listen differ. Set
// tls.Config.ClientAuth (e.g. tls.RequireAndVerifyClientCert) with ClientCAs on
// the server for mutual TLS; the verified client certificate is then available
// to a connect-authorizer via PeerCertificates.

// NewTLSListener listens on TCP with TLS. config must carry a server certificate.
func NewTLSListener(addr string, config *tls.Config, keepAlive, writeTimeout time.Duration) (Listener, error) {
	if config == nil {
		return nil, fmt.Errorf("tls listener requires a non-nil *tls.Config with a server certificate")
	}
	rawLis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &streamListener{
		lis:          tls.NewListener(rawLis, config),
		keepAlive:    keepAlive,
		writeTimeout: writeTimeout,
	}, nil
}

type tlsDialer struct {
	address      string
	config       *tls.Config
	keepAlive    time.Duration
	writeTimeout time.Duration
}

// NewTLSDialer dials a TLS server over TCP. A nil config verifies against the
// system root CAs and derives the server name from the address; supply a config
// for custom CAs, a client certificate (mTLS), or test options.
func NewTLSDialer(address string, config *tls.Config, keepAlive, writeTimeout time.Duration) Dialer {
	return &tlsDialer{address: address, config: config, keepAlive: keepAlive, writeTimeout: writeTimeout}
}

func (d *tlsDialer) Dial() (MsgConn, error) {
	conn, err := tls.Dial("tcp", d.address, d.config) // performs the TLS handshake
	if err != nil {
		return nil, err
	}
	enableKeepAlive(conn, d.keepAlive) // unwraps *tls.Conn -> *net.TCPConn
	return NewMsgConn(conn, d.writeTimeout), nil
}

// TLSConnectionState reports the TLS connection state when the underlying
// connection is a *tls.Conn, completing the handshake first. Handshake errors
// (e.g. an mTLS client with no valid certificate) surface on the subsequent
// read. It is exported so transports in other packages (e.g. the QUIC submodule)
// can implement the same hook for PeerCertificates.
func (t *messageConnAdapter) TLSConnectionState() (tls.ConnectionState, bool) {
	tc, ok := t.conn.(*tls.Conn)
	if !ok {
		return tls.ConnectionState{}, false
	}
	_ = tc.Handshake()
	return tc.ConnectionState(), true
}

// TLSStateConn is implemented by MsgConns whose transport is TLS-backed (TLS over
// TCP, or QUIC). PeerCertificates uses it to expose the verified peer chain.
type TLSStateConn interface {
	TLSConnectionState() (tls.ConnectionState, bool)
}

// PeerCertificates returns the verified peer (client) certificate chain for a
// TLS-backed MsgConn (TLS or QUIC), completing the handshake if needed. ok is
// false for non-TLS transports and when the peer presented no certificate. Use
// it inside a valueserver connect-authorizer for certificate-based authorization
// — the TLS analogue of PeerCredOf for Unix sockets.
func PeerCertificates(conn MsgConn) (certs []*x509.Certificate, ok bool) {
	ts, isTLS := conn.(TLSStateConn)
	if !isTLS {
		return nil, false
	}
	st, has := ts.TLSConnectionState()
	if !has || len(st.PeerCertificates) == 0 {
		return nil, false
	}
	return st.PeerCertificates, true
}
