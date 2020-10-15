/*
 *
 * Copyright 2020-present Arpabet Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package valueserver

import (
	"arpabet.pkg.is/value-rpc/valuerpc"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"io"
	"net"
	"sync"
)

/**
@author Alex Shvid
*/

type rpcServer struct {
	listener net.Listener
	shutdown chan bool
	wg       sync.WaitGroup
	logger   *zap.Logger

	clientMap   sync.Map // key is clientId, value *servingClient
	functionMap sync.Map // key is function name, value *function

	closeOnce sync.Once
}

func NewDevelopmentServer(address string) (Server, error) {
	logger, _ := zap.NewDevelopment()
	return NewServer(address, logger)
}

func NewServer(address string, logger *zap.Logger) (Server, error) {

	t := &rpcServer{
		shutdown: make(chan bool, 1),
		logger:   logger,
	}
	lis, err := net.Listen("tcp", address)
	if err != nil {
		logger.Error("bind the server port",
			zap.String("addr", address),
			zap.Error(err))
		return nil, err
	}
	t.listener = lis
	t.wg.Add(1)
	logger.Info("start vRPC server", zap.String("addr", address))
	return t, nil

}

func (t *rpcServer) Close() error {
	var err error
	t.closeOnce.Do(func() {
		t.logger.Info("shutdown vRPC server")

		t.clientMap.Range(func(key, value interface{}) bool {
			cli := value.(*servingClient)
			cli.Close()
			return true
		})

		t.shutdown <- true
		err = t.listener.Close()
	})
	return err
}

func (t *rpcServer) Run() error {

	defer t.wg.Done()

	for {
		conn, err := t.listener.Accept()
		if err != nil {
			select {
			case <-t.shutdown:
				return nil
			default:
				t.logger.Warn("error on accept connection", zap.Error(err))
			}
		} else {
			t.wg.Add(1)
			go func() {
				defer t.wg.Done()
				t.logger.Info("new connection", zap.String("from", conn.RemoteAddr().String()))
				err := t.handleConnection(valuerpc.NewMsgConn(conn))
				if err != nil {
					t.logger.Error("handle connection",
						zap.String("from", conn.RemoteAddr().String()),
						zap.Error(err),
					)
				}
			}()
		}
	}

	return nil

}

func (t *rpcServer) handshake(conn valuerpc.MsgConn) (*servingClient, error) {
	req, err := conn.ReadMessage()
	if err != nil {
		return nil, err
	}

	mt := req.GetNumber(valuerpc.MessageTypeField)
	if mt == nil {
		return nil, errors.Errorf("on handshake, empty message type in %s", req.String())
	}

	msgType := valuerpc.MessageType(mt.Long())

	if msgType != valuerpc.HandshakeRequest {
		return nil, errors.Errorf("on handshake, wrong message type in %s", req.String())
	}

	if !valuerpc.ValidMagicAndVersion(req) {
		return nil, errors.Errorf("on handshake, unsupported client version in %s", req.String())
	}
	cid := req.GetNumber(valuerpc.ClientIdField)
	if cid == nil {
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
