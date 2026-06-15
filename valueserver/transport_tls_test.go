/*
 * Copyright (c) 2025 Karagatan LLC.
 * SPDX-License-Identifier: Apache-2.0
 */

package valueserver_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"math/big"
	"net"
	"testing"
	"time"

	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valueclient"
	"go.arpabet.com/value-rpc/valuerpc"
	"go.arpabet.com/value-rpc/valueserver"
	"go.uber.org/zap"
)

// genCertPair builds a throwaway CA and issues a loopback server certificate and
// a client certificate from it, returning a pool trusting the CA plus both
// leaf certificates (with keys) for use in tls.Config.
func genCertPair(t *testing.T) (caPool *x509.CertPool, server, client tls.Certificate) {
	t.Helper()

	issue := func(tmpl, parent *x509.Certificate, pub any, signer *ecdsa.PrivateKey) []byte {
		der, err := x509.CreateCertificate(rand.Reader, tmpl, parent, pub, signer)
		if err != nil {
			t.Fatalf("create certificate: %v", err)
		}
		return der
	}

	caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "vrpc-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	caDER := issue(caTmpl, caTmpl, caKey.Public(), caKey)
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("parse CA: %v", err)
	}
	caPool = x509.NewCertPool()
	caPool.AddCert(caCert)

	srvKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	srvTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "127.0.0.1"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:     []string{"localhost"},
	}
	srvDER := issue(srvTmpl, caCert, srvKey.Public(), caKey)
	server = tls.Certificate{Certificate: [][]byte{srvDER}, PrivateKey: srvKey}

	cliKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	cliTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject:      pkix.Name{CommonName: "vrpc-test-client"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	cliDER := issue(cliTmpl, caCert, cliKey.Public(), caKey)
	client = tls.Certificate{Certificate: [][]byte{cliDER}, PrivateKey: cliKey}

	return caPool, server, client
}

// TestTLS_ServerAuth: encrypted TCP with the client verifying the server cert.
func TestTLS_ServerAuth(t *testing.T) {
	caPool, serverCert, _ := genCertPair(t)

	srv, err := valueserver.NewTLSServer("127.0.0.1:0",
		&tls.Config{Certificates: []tls.Certificate{serverCert}}, zap.NewNop())
	if err != nil {
		t.Fatalf("tls server: %v", err)
	}
	defer srv.Close()
	srv.AddFunction("ping", valuerpc.Void, valuerpc.String,
		func(args value.Value) (value.Value, error) { return value.Utf8("pong"), nil })
	go srv.Run()

	cli := valueclient.NewTLSClient(srv.Addr().String(), &tls.Config{RootCAs: caPool})
	if err := cli.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer cli.Close()
	cli.SetTimeout(5000)

	res, err := cli.CallFunction("ping", nil)
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if got := res.(value.String).String(); got != "pong" {
		t.Fatalf("result = %q, want %q", got, "pong")
	}
}

// TestTLS_MutualAuth: the server requires and verifies a client certificate, and
// the verified client identity is read in the connect-authorizer.
func TestTLS_MutualAuth(t *testing.T) {
	caPool, serverCert, clientCert := genCertPair(t)

	srv, err := valueserver.NewTLSServer("127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientCAs:    caPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
	}, zap.NewNop())
	if err != nil {
		t.Fatalf("tls server: %v", err)
	}
	defer srv.Close()

	cnCh := make(chan string, 1)
	srv.SetConnectAuthorizer(func(conn valuerpc.MsgConn) error {
		certs, ok := valuerpc.PeerCertificates(conn)
		if !ok {
			return fmt.Errorf("no client certificate")
		}
		select {
		case cnCh <- certs[0].Subject.CommonName:
		default:
		}
		return nil
	})
	srv.AddFunction("ping", valuerpc.Void, valuerpc.String,
		func(args value.Value) (value.Value, error) { return value.Utf8("pong"), nil })
	go srv.Run()

	cli := valueclient.NewTLSClient(srv.Addr().String(), &tls.Config{
		RootCAs:      caPool,
		Certificates: []tls.Certificate{clientCert},
	})
	if err := cli.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer cli.Close()
	cli.SetTimeout(5000)

	if _, err := cli.CallFunction("ping", nil); err != nil {
		t.Fatalf("call: %v", err)
	}

	select {
	case cn := <-cnCh:
		if cn != "vrpc-test-client" {
			t.Errorf("client cert CN = %q, want %q", cn, "vrpc-test-client")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("authorizer never observed the client certificate")
	}
}

// TestTLS_RejectsClientWithoutCert: an mTLS server must reject a client that
// presents no certificate (the rejection surfaces at connect or first call,
// depending on the TLS version).
func TestTLS_RejectsClientWithoutCert(t *testing.T) {
	caPool, serverCert, _ := genCertPair(t)

	srv, err := valueserver.NewTLSServer("127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientCAs:    caPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
	}, zap.NewNop())
	if err != nil {
		t.Fatalf("tls server: %v", err)
	}
	defer srv.Close()
	srv.AddFunction("ping", valuerpc.Void, valuerpc.String,
		func(args value.Value) (value.Value, error) { return value.Utf8("pong"), nil })
	go srv.Run()

	// Trusts the server, but presents no client certificate.
	cli := valueclient.NewTLSClient(srv.Addr().String(), &tls.Config{RootCAs: caPool})
	defer cli.Close()
	cli.SetTimeout(800)

	if err := cli.Connect(); err == nil {
		// TLS 1.3 validates the client cert after the client's handshake
		// completes, so the failure may surface on the first call instead.
		if _, callErr := cli.CallFunction("ping", nil); callErr == nil {
			t.Fatal("expected mTLS to reject a client with no certificate")
		}
	}
}
