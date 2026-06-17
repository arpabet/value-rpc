/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

// Command mtls demonstrates the TLS transport with mutual authentication: the
// server requires and verifies a client certificate, and a connect-authorizer
// reads the verified client identity via valuerpc.PeerCertificates. Certificates
// are generated in-memory for the demo.
package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"log"
	"math/big"
	"time"

	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valueclient"
	"go.arpabet.com/value-rpc/valuerpc"
	"go.arpabet.com/value-rpc/valueserver"
	"go.uber.org/zap"
)

func main() {
	ca, caKey := newCA()
	caPool := x509.NewCertPool()
	caPool.AddCert(ca)

	serverTLS := &tls.Config{
		Certificates: []tls.Certificate{issue("localhost", true, ca, caKey)},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    caPool,
		MinVersion:   tls.VersionTLS12,
	}
	clientTLS := &tls.Config{
		Certificates: []tls.Certificate{issue("alice", false, ca, caKey)},
		RootCAs:      caPool,
		ServerName:   "localhost",
		MinVersion:   tls.VersionTLS12,
	}

	srv, err := valueserver.NewTLSServer("127.0.0.1:0", serverTLS, zap.NewNop())
	if err != nil {
		log.Fatal(err)
	}
	defer srv.Close()

	// The verified client certificate identifies the caller.
	srv.SetConnectAuthorizer(func(conn valuerpc.MsgConn) error {
		certs, ok := valuerpc.PeerCertificates(conn)
		if !ok || len(certs) == 0 {
			return fmt.Errorf("no client certificate")
		}
		fmt.Printf("  server: connection authorized for client CN=%q\n", certs[0].Subject.CommonName)
		return nil
	})
	srv.AddFunction("whoami", valuerpc.Any, valuerpc.Any,
		func(_ context.Context, _ value.Value) (value.Value, error) {
			return value.Utf8("hello, authenticated client"), nil
		})
	go srv.Run()

	cli := valueclient.NewTLSClient(srv.Addr().String(), clientTLS)
	if err := cli.Connect(); err != nil {
		log.Fatal(err)
	}
	defer cli.Close()

	res, err := cli.CallFunction(context.Background(), "whoami", nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("client: %s\n", res.(value.String).String())
}

func newCA() (*x509.Certificate, *ecdsa.PrivateKey) {
	key := mustKey()
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "demo-ca"},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		log.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		log.Fatal(err)
	}
	return cert, key
}

// issue returns a leaf certificate (server or client) signed by the CA.
func issue(cn string, server bool, ca *x509.Certificate, caKey *ecdsa.PrivateKey) tls.Certificate {
	key := mustKey()
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(now.UnixNano()),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    now.Add(-time.Minute),
		NotAfter:     now.Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}
	if server {
		tmpl.DNSNames = []string{cn}
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	} else {
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	if err != nil {
		log.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

func mustKey() *ecdsa.PrivateKey {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		log.Fatal(err)
	}
	return key
}
