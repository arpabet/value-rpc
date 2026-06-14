/*
 * Copyright (c) 2025 Karagatan LLC.
 * SPDX-License-Identifier: Apache-2.0
 */

package valuerpc

import (
	"errors"
	"net"
)

// PeerCred identifies the peer process of a Unix-domain-socket connection,
// enabling local authorization (e.g. "only this uid may connect"). It is read
// from the kernel (SO_PEERCRED on Linux, LOCAL_PEERCRED on macOS/BSD) and cannot
// be forged by the peer.
type PeerCred struct {
	UID uint32
	GID uint32
	PID int32 // 0 when the platform does not report a pid
}

// PeerCredOf returns the peer credentials for a MsgConn backed by a Unix-domain
// socket. ok is false for non-Unix transports (e.g. TCP, WebSocket) and on
// platforms without peer-credential support.
func PeerCredOf(conn MsgConn) (cred PeerCred, ok bool) {
	pc, isPC := conn.(interface{ peerCred() (PeerCred, error) })
	if !isPC {
		return PeerCred{}, false
	}
	c, err := pc.peerCred()
	if err != nil {
		return PeerCred{}, false
	}
	return c, true
}

var errNotUnixConn = errors.New("not a unix-domain-socket connection")

// peerCred reports the peer credentials when the underlying connection is a Unix
// socket; readPeerCred is implemented per-platform.
func (t *messageConnAdapter) peerCred() (PeerCred, error) {
	uc, ok := t.conn.(*net.UnixConn)
	if !ok {
		return PeerCred{}, errNotUnixConn
	}
	return readPeerCred(uc)
}
