/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valueserver

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"crypto/tls"
	"encoding/hex"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"go.arpabet.com/value-rpc/valuerpc"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
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

// MaxConnections caps the number of simultaneously open connections the server
// accepts; connections beyond the limit are closed immediately. Set to 0
// (default) for no limit.
var MaxConnections int64 = 0

type rpcServer struct {
	listener valuerpc.Listener
	shutdown chan struct{}
	wg       sync.WaitGroup
	// acceptMu serializes the shutdown signal with the per-connection wg.Add in
	// Run, so a connection accepted as the server is closing can never call Add
	// concurrently with Close's wg.Wait (a WaitGroup misuse / data race).
	acceptMu sync.Mutex
	logger   *zap.Logger

	ctx    context.Context // base context for handlers; cancelled on Close
	cancel context.CancelFunc

	cfg serverConfig // resolved per-server config (defaults + options)

	clientMap   sync.Map // key is clientId, value *servingClient
	functionMap sync.Map // key is function name, value *function
	connMap     sync.Map // key is valuerpc.MsgConn, tracks live conns for shutdown (BUG-14)
	liveConns   atomic.Int64

	authorizer    atomic.Value // ConnectAuthorizer, optional
	authenticator atomic.Value // Authenticator, optional

	closeOnce sync.Once
}

func (t *rpcServer) SetConnectAuthorizer(fn ConnectAuthorizer) {
	t.authorizer.Store(fn)
}

func (t *rpcServer) getConnectAuthorizer() ConnectAuthorizer {
	if v := t.authorizer.Load(); v != nil {
		return v.(ConnectAuthorizer)
	}
	return nil
}

func (t *rpcServer) SetAuthenticator(fn Authenticator) {
	t.authenticator.Store(fn)
}

func (t *rpcServer) getAuthenticator() Authenticator {
	if v := t.authenticator.Load(); v != nil {
		return v.(Authenticator)
	}
	return nil
}

func NewDevelopmentServer(address string) (Server, error) {
	logger, _ := zap.NewDevelopment()
	return NewServer(address, logger)
}

// NewServer creates a server bound to address. A bare "host:port" (or ":port")
// listens on TCP; a scheme selects the transport: "tcp://host:port" or
// "unix:///path/to.sock". For full control use NewServerWithListener.
func NewServer(address string, logger *zap.Logger, opts ...ServerOption) (Server, error) {
	cfg := newServerConfig(opts)
	lis, err := valuerpc.NewListener(address, cfg.keepAlive, cfg.writeTimeout, cfg.maxFrameSize)
	if err != nil {
		logger.Error("bind the server address",
			zap.String("addr", address),
			zap.Error(err))
		return nil, err
	}
	return NewServerWithListener(lis, logger, opts...)
}

// NewUnixServer creates a server listening on the Unix-domain socket at path. A
// stale socket file from a previous run is removed first.
func NewUnixServer(path string, logger *zap.Logger, opts ...ServerOption) (Server, error) {
	cfg := newServerConfig(opts)
	lis, err := valuerpc.NewUnixListener(path, cfg.writeTimeout, cfg.maxFrameSize)
	if err != nil {
		logger.Error("bind the unix socket",
			zap.String("path", path),
			zap.Error(err))
		return nil, err
	}
	return NewServerWithListener(lis, logger, opts...)
}

// NewWebSocketServer creates a standalone WebSocket server: it owns an HTTP
// server bound to addr and serves the vRPC endpoint at path (default "/"). Each
// message travels as one MessagePack binary frame. For wss:// (TLS) or to share
// a port with other HTTP routes, use NewWebSocketHandler instead.
func NewWebSocketServer(addr, path string, logger *zap.Logger, opts ...ServerOption) (Server, error) {
	cfg := newServerConfig(opts)
	lis, err := valuerpc.NewWebSocketListener(addr, path, cfg.writeTimeout, cfg.maxFrameSize, cfg.keepAlive)
	if err != nil {
		logger.Error("bind the websocket server",
			zap.String("addr", addr),
			zap.Error(err))
		return nil, err
	}
	return NewServerWithListener(lis, logger, opts...)
}

// NewTLSServer creates a server listening on TCP with TLS. config must carry a
// server certificate; set config.ClientAuth (e.g. tls.RequireAndVerifyClientCert)
// and config.ClientCAs for mutual TLS, then inspect the verified client
// certificate in a connect-authorizer via valuerpc.PeerCertificates.
func NewTLSServer(addr string, config *tls.Config, logger *zap.Logger, opts ...ServerOption) (Server, error) {
	cfg := newServerConfig(opts)
	lis, err := valuerpc.NewTLSListener(addr, config, cfg.keepAlive, cfg.writeTimeout, cfg.maxFrameSize)
	if err != nil {
		logger.Error("bind the tls server",
			zap.String("addr", addr),
			zap.Error(err))
		return nil, err
	}
	return NewServerWithListener(lis, logger, opts...)
}

// NewMemServer creates an in-process server registered under name. A client in
// the same process reaches it with valueclient.NewMemClient(name) (or the
// "mem://name" address). No sockets, no serialization — ideal for tests and for
// composing services in one binary before splitting them onto a real transport.
func NewMemServer(name string, logger *zap.Logger, opts ...ServerOption) (Server, error) {
	lis, err := valuerpc.NewMemListener(name)
	if err != nil {
		logger.Error("register the mem listener",
			zap.String("name", name),
			zap.Error(err))
		return nil, err
	}
	return NewServerWithListener(lis, logger, opts...)
}

// NewWebSocketHandler returns a server plus an http.Handler to mount on your own
// http.ServeMux (e.g. mux.Handle("/rpc", h)). The server does not listen on its
// own port; register functions and call Run() to serve upgraded connections.
// This is the path to wss:// (run your own TLS http.Server) and to sharing a
// port with REST/health/metrics routes.
func NewWebSocketHandler(logger *zap.Logger, opts ...ServerOption) (Server, http.Handler, error) {
	cfg := newServerConfig(opts)
	lis, h := valuerpc.NewWebSocketHandler(cfg.writeTimeout, cfg.maxFrameSize, cfg.keepAlive)
	srv, err := NewServerWithListener(lis, logger, opts...)
	if err != nil {
		return nil, nil, err
	}
	return srv, h, nil
}

// NewServerWithListener creates a server over any transport (TCP, Unix socket,
// WebSocket, …) supplied as a valuerpc.Listener.
func NewServerWithListener(lis valuerpc.Listener, logger *zap.Logger, opts ...ServerOption) (Server, error) {
	t := &rpcServer{
		shutdown: make(chan struct{}),
		logger:   logger,
		listener: lis,
		cfg:      newServerConfig(opts),
	}
	// ctx is the parent of every handler's request context; cancelling it on
	// Close signals all in-flight handlers that the server is shutting down.
	t.ctx, t.cancel = context.WithCancel(context.Background())
	// wg tracks only connection-handler goroutines, so Close() drains in-flight
	// connections and does not hang when Run() was never called.
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

		// Signal all in-flight handlers (via their request contexts) that the
		// server is going away, then stop accepting and unblock Run(). Closing
		// shutdown under acceptMu orders it against Run's wg.Add so no new handler
		// is added once we proceed to wg.Wait below.
		t.cancel()
		t.acceptMu.Lock()
		close(t.shutdown)
		t.acceptMu.Unlock()
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

		// Cap simultaneously open connections so a connection flood cannot
		// exhaust file descriptors / memory (DoS). Over the limit we close the
		// new connection immediately and keep serving existing ones.
		if max := t.cfg.maxConnections; max > 0 && t.liveConns.Load() >= max {
			t.logger.Warn("connection rejected: max connections reached",
				zap.Int64("max", max), zap.String("from", msgConn.RemoteAddr()))
			msgConn.Close()
			continue
		}

		// Register the handler under acceptMu so wg.Add is ordered against Close's
		// close(shutdown)+wg.Wait: if shutdown already fired, drop this connection
		// instead of adding to a WaitGroup that Close is about to wait on.
		t.acceptMu.Lock()
		select {
		case <-t.shutdown:
			t.acceptMu.Unlock()
			msgConn.Close()
			continue
		default:
		}
		// The Listener has already applied transport framing and keepalive.
		t.liveConns.Add(1)
		t.connMap.Store(msgConn, struct{}{})
		t.wg.Add(1)
		t.acceptMu.Unlock()
		go func() {
			defer t.wg.Done()
			defer t.liveConns.Add(-1)
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
	if t.cfg.handshakeTimeout > 0 {
		_ = conn.SetReadDeadline(time.Now().Add(t.cfg.handshakeTimeout))
	}

	req, err := conn.ReadMessage()
	if err != nil {
		return nil, err
	}

	if t.cfg.handshakeTimeout > 0 {
		_ = conn.SetReadDeadline(time.Time{})
	}

	mt, ok := valuerpc.GetNumberField(req, valuerpc.DefaultDialect.MessageTypeField)
	if !ok {
		return nil, xerrors.Errorf("on handshake, empty message type%s", reqDetail(req))
	}

	msgType := valuerpc.MessageType(mt.Long())

	if msgType != valuerpc.HandshakeRequest {
		return nil, xerrors.Errorf("on handshake, wrong message type%s", reqDetail(req))
	}

	if !valuerpc.ValidMagicAndVersion(req) {
		return nil, xerrors.Errorf("on handshake, unsupported client version%s", reqDetail(req))
	}

	// Authenticate the client's credential before touching any session state, so
	// an unauthenticated peer never reaches createOrUpdateServingClient. The
	// authenticated principal binds session resumption (below).
	principal := ""
	if authn := t.getAuthenticator(); authn != nil {
		p, err := authn(conn, req.Get(valuerpc.DefaultDialect.AuthField))
		if err != nil {
			return nil, xerrors.Errorf("on handshake, authentication failed: %v", err)
		}
		principal = p
	}

	cid, ok := valuerpc.GetNumberField(req, valuerpc.DefaultDialect.ClientIdField)
	if !ok {
		return nil, xerrors.Errorf("on handshake, no client id%s", reqDetail(req))
	}
	clientId := cid.Long()

	// The client-asserted clientId alone must not let a peer resume (and thereby
	// take over) another client's session: createOrUpdateServingClient requires
	// the server-issued session token to match before reattaching to an existing
	// servingClient. A first connect (no existing session) mints a fresh token.
	presentedToken := ""
	if tok, ok := valuerpc.GetStringField(req, valuerpc.DefaultDialect.SessionTokenField); ok {
		presentedToken = tok.String()
	}

	cli, err := t.createOrUpdateServingClient(clientId, presentedToken, principal, conn)
	if err != nil {
		return nil, err
	}

	resp := valuerpc.NewHandshakeResponse(cli.sessionToken)
	err = conn.WriteMessage(resp)
	if err != nil {
		return nil, xerrors.Errorf("on handshake, %v", err)
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

	if authz := t.getConnectAuthorizer(); authz != nil {
		// Bound the read deadline first: an authorizer that inspects TLS peer
		// certificates (valuerpc.PeerCertificates) triggers the TLS handshake,
		// which we must not let a stalled peer block indefinitely.
		if t.cfg.handshakeTimeout > 0 {
			_ = conn.SetReadDeadline(time.Now().Add(t.cfg.handshakeTimeout))
		}
		if err := authz(conn); err != nil {
			return xerrors.Errorf("connection rejected by authorizer: %v", err)
		}
	}

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

// createOrUpdateServingClient reattaches conn to an existing session only when
// presentedToken matches the token the server issued for that clientId AND the
// reconnecting peer authenticated as the same principal; otherwise it mints a
// brand-new session. The token stops a peer that guesses/replays another client's
// clientId from hijacking the session; the principal binding additionally stops a
// leaked token from letting a *different* authenticated principal take it over.
func (t *rpcServer) createOrUpdateServingClient(clientId int64, presentedToken, principal string, conn valuerpc.MsgConn) (*servingClient, error) {

	resume := func(existing *servingClient) (*servingClient, error) {
		if !validToken(presentedToken, existing.sessionToken) {
			return nil, xerrors.Errorf("handshake rejected: session token mismatch for client %d", clientId)
		}
		if existing.principal != principal {
			return nil, xerrors.Errorf("handshake rejected: principal mismatch for client %d", clientId)
		}
		existing.replaceConn(conn)
		return existing, nil
	}

	if cli, ok := t.clientMap.Load(clientId); ok {
		return resume(cli.(*servingClient))
	}

	token, err := newSessionToken()
	if err != nil {
		return nil, xerrors.Errorf("on handshake, generate session token: %v", err)
	}

	client := NewServingClient(t.ctx, clientId, token, principal, conn, &t.functionMap, t.logger, &t.cfg)
	// LoadOrStore guards a reconnect race for the same (new) clientId: if another
	// connection won the slot, validate against the winner and discard ours
	// (closing it stops the sender goroutine NewServingClient started).
	if actual, loaded := t.clientMap.LoadOrStore(clientId, client); loaded {
		client.Close()
		return resume(actual.(*servingClient))
	}

	return client, nil
}

// validToken reports whether the presented session token authorizes resuming a
// session. A non-empty presented token must equal the stored one, compared in
// constant time to avoid leaking it through timing.
func validToken(presented, stored string) bool {
	if presented == "" || stored == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(presented), []byte(stored)) == 1
}

// newSessionToken mints an unguessable 128-bit session secret.
func newSessionToken() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
