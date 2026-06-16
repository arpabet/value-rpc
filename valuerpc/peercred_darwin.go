//go:build darwin

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
		xc    *unix.Xucred
		pid   int
		operr error
	)
	if err := raw.Control(func(fd uintptr) {
		xc, operr = unix.GetsockoptXucred(int(fd), unix.SOL_LOCAL, unix.LOCAL_PEERCRED)
		if operr != nil {
			return
		}
		pid, operr = unix.GetsockoptInt(int(fd), unix.SOL_LOCAL, unix.LOCAL_PEERPID)
	}); err != nil {
		return PeerCred{}, err
	}
	if operr != nil {
		return PeerCred{}, operr
	}
	cred := PeerCred{UID: xc.Uid, PID: int32(pid)}
	if xc.Ngroups > 0 {
		cred.GID = xc.Groups[0] // primary group
	}
	return cred, nil
}
