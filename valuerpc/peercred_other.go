//go:build !linux && !darwin

/*
 * Copyright (c) 2025 Karagatan LLC.
 * SPDX-License-Identifier: Apache-2.0
 */

package valuerpc

import (
	"errors"
	"net"
)

func readPeerCred(*net.UnixConn) (PeerCred, error) {
	return PeerCred{}, errors.New("peer credentials are not supported on this platform")
}
