/*
 * Copyright (c) 2025 Karagatan LLC.
 * SPDX-License-Identifier: Apache-2.0
 */

package valueserver

import (
	"io"
	"net"
	"sync"
	"time"

	"github.com/pkg/errors"
	"go.arpabet.com/value-rpc/valuerpc"
	"go.uber.org/zap"
)

var DefaultTimeout = 10 * time.Second

// HandshakeTimeout bounds how long a freshly accepted connection has to send a
// valid handshake before it is dropped (slowloris protection, BUG-10). It does
// not apply to the steady-state read loop, so long-lived idle streams are not
// affected. Set to 0 to disable.
var HandshakeTimeout = 10 * time.Second

// KeepAlivePeriod enables TCP keepalive on accepted connections so dead peers
// are reclaimed without killing idle streams (BUG-10). Ignored for non-TCP
// transports (e.g. Unix sockets).
var KeepAlivePeriod = 15 * time.Second

type rpcServer struct {
	listener valuerpc.Listener
	shutdown chan struct{}
	wg       sync.WaitGroup
	logger   *zap.Logger

	clientMap   sync.Map // key is clientId, value *servingClient
	functionMap sync.Map // key is function name, value *function
	connMap     sync.Map // key is valuerpc.MsgConn, tracks live conns for shutdown (BUG-14)

	closeOnce sync.Once
}

func NewDevelopmentServer(address string) (Server, error) {
	logger, _ := zap.NewDevelopment()
	return NewServer(address, logger)
}

// NewServer creates a TCP server bound to address ("host:port"). For other
// transports use NewServerWithListener.
func NewServer(address string, logger *zap.Logger) (Server, error) {
	lis, err := valuerpc.NewStreamListener("tcp", address, KeepAlivePeriod, DefaultTimeout)
	if err != nil {
		logger.Error("bind the server port",
			zap.String("addr", address),
			zap.Error(err))
		return nil, err
	}
	return NewServerWithListener(lis, logger)
}

// NewServerWithListener creates a server over any transport (TCP, Unix socket,
// WebSocket, …) supplied as a valuerpc.Listener.
func NewServerWithListener(lis valuerpc.Listener, logger *zap.Logger) (Server, error) {
	t := &rpcServer{
		shutdown: make(chan struct{}),
		logger:   logger,
		listener: lis,
	}
	t.wg.Add(1)
	logger.Info("start vRPC server", zap.String("addr", lis.Addr().String()))
	return t, nil
}

func (t *rpcServer) Addr() net.Addr {
	return t.listener.Addr()
}

func (t *rpcServer) Close() error {
	var err error
	t.closeOnce.Do(func() {
		t.logger.Info("shutdown vRPC server")

		// Stop accepting and unblock Run().
		close(t.shutdown)
		err = t.listener.Close()

		// Unblock every live connection's read loop (pre- and post-handshake)
		// so handleConnection goroutines can exit (BUG-14).
		t.connMap.Range(func(key, _ interface{}) bool {
			key.(valuerpc.MsgConn).Close()
			return true
		})

		// Stop serving clients (senders, in-flight requests).
		t.clientMap.Range(func(_, value interface{}) bool {
			value.(*servingClient).Close()
			return true
		})

		// Wait for Run() and all connection goroutines to finish.
		t.wg.Wait()
	})
	return err
}

func (t *rpcServer) Run() error {

	defer t.wg.Done()

	var backoff time.Duration
	for {
		msgConn, err := t.listener.Accept()
		if err != nil {
			select {
			case <-t.shutdown:
				return nil
			default:
			}
			// BUG-12 fix: back off instead of spinning at 100% CPU on a
			// persistent accept error (e.g. EMFILE).
			if backoff == 0 {
				backoff = 5 * time.Millisecond
			} else {
				backoff *= 2
			}
			if backoff > time.Second {
				backoff = time.Second
			}
			t.logger.Warn("error on accept connection; retrying",
				zap.Duration("backoff", backoff), zap.Error(err))
			time.Sleep(backoff)
			continue
		}
		backoff = 0

		// The Listener has already applied transport framing and keepalive.
		t.connMap.Store(msgConn, struct{}{})

		t.wg.Add(1)
		go func() {
			defer t.wg.Done()
			defer t.connMap.Delete(msgConn)
			t.logger.Info("new connection", zap.String("from", msgConn.RemoteAddr()))
			if err := t.handleConnection(msgConn); err != nil {
				select {
				case <-t.shutdown:
					// expected: the read loop was unblocked by graceful shutdown
					t.logger.Debug("connection closed on shutdown", zap.Error(err))
				default:
					t.logger.Error("handle connection",
						zap.String("from", msgConn.RemoteAddr()),
						zap.Error(err),
					)
				}
			}
		}()
	}

}

func (t *rpcServer) handshake(conn valuerpc.MsgConn) (*servingClient, error) {

	// Bound the time to receive a valid handshake (BUG-10), then clear the
	// deadline so steady-state reads (which may idle on long streams) are not
	// affected.
	if HandshakeTimeout > 0 {
		_ = conn.SetReadDeadline(time.Now().Add(HandshakeTimeout))
	}

	req, err := conn.ReadMessage()
	if err != nil {
		return nil, err
	}

	if HandshakeTimeout > 0 {
		_ = conn.SetReadDeadline(time.Time{})
	}

	mt, ok := valuerpc.GetNumberField(req, valuerpc.MessageTypeField)
	if !ok {
		return nil, errors.Errorf("on handshake, empty message type in %s", req.String())
	}

	msgType := valuerpc.MessageType(mt.Long())

	if msgType != valuerpc.HandshakeRequest {
		return nil, errors.Errorf("on handshake, wrong message type in %s", req.String())
	}

	if !valuerpc.ValidMagicAndVersion(req) {
		return nil, errors.Errorf("on handshake, unsupported client version in %s", req.String())
	}
	cid, ok := valuerpc.GetNumberField(req, valuerpc.ClientIdField)
	if !ok {
		return nil, errors.Errorf("on handshake, no client id in %s", req.String())
	}
	clientId := cid.Long()
	cli := t.createOrUpdateServingClient(clientId, conn)

	resp := valuerpc.NewHandshakeResponse()
	err = conn.WriteMessage(resp)
	if err != nil {
		return nil, errors.Errorf("on handshake, %v", err)
	}

	return cli, nil
}

func (t *rpcServer) handleConnection(conn valuerpc.MsgConn) error {

	defer func() {
		defer conn.Close()
		if r := recover(); r != nil {
			t.logger.Error("Recovered in handleConnection", zap.Any("recover", r))
		}
	}()

	cli, err := t.handshake(conn)
	if err != nil {
		// wrong client, close connection
		return err
	}

	for {
		req, err := conn.ReadMessage()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		err = cli.processRequest(req)
		if err != nil {
			// app error, continue after logging
			t.logger.Debug("processMessage",
				zap.Stringer("req", req),
				zap.Error(err))
		}
	}
}

func (t *rpcServer) createOrUpdateServingClient(clientId int64, conn valuerpc.MsgConn) *servingClient {

	if cli, ok := t.clientMap.Load(clientId); ok {
		client := cli.(*servingClient)
		client.replaceConn(conn)
		return client
	}

	client := NewServingClient(clientId, conn, &t.functionMap, t.logger)
	t.clientMap.Store(clientId, client)

	return client
}
