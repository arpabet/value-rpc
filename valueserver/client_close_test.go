package valueserver

import (
	"errors"
	"io"
	"net"
	"testing"

	"golang.org/x/xerrors"
)

/*
TestIsClientClose pins the classification behind "connection closed" vs
"connection failed": every ordinary way a peer hangs up — the WebSocket close
frame a navigating browser sends (StatusGoingAway), resets, broken pipes,
EOF, a socket closed under the reader — logs as an Info close, and anything
unrecognized stays an error so real failures cannot hide.
*/
func TestIsClientClose(t *testing.T) {
	closes := []error{
		io.EOF,
		xerrors.Errorf("read: %w", io.EOF),
		net.ErrClosed,
		errors.New(`failed to get reader: received close frame: status = StatusGoingAway and reason = ""`),
		errors.New("read tcp 1.2.3.4:443->5.6.7.8:50000: connection reset by peer"),
		errors.New("write: broken pipe"),
		errors.New("accept: use of closed network connection"),
	}
	for _, err := range closes {
		if !isClientClose(err) {
			t.Fatalf("%v must classify as a client close", err)
		}
	}
	for _, err := range []error{
		nil,
		errors.New("handshake: wrong magic"),
		errors.New("frame too large"),
	} {
		if isClientClose(err) {
			t.Fatalf("%v must NOT classify as a client close", err)
		}
	}
}
