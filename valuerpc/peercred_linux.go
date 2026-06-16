//go:build linux

/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valuerpc

import (
	"net"

	"golang.org/x/sys/unix"
)

func readPeerCred(uc *net.UnixConn) (PeerCred, error) {
	raw, err := uc.SyscallConn()
	if err != nil {
		return PeerCred{}, err
	}
	var (
		cred  *unix.Ucred
		operr error
	)
	if err := raw.Control(func(fd uintptr) {
		cred, operr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
	}); err != nil {
		return PeerCred{}, err
	}
	if operr != nil {
		return PeerCred{}, operr
	}
	return PeerCred{UID: cred.Uid, GID: cred.Gid, PID: cred.Pid}, nil
}
