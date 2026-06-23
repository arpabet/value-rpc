//go:build !linux && !darwin

/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valuerpc

import (
	"net"

	"golang.org/x/xerrors"
)

func readPeerCred(*net.UnixConn) (PeerCred, error) {
	return PeerCred{}, xerrors.New("peer credentials are not supported on this platform")
}
