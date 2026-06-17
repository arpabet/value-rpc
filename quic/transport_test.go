/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valuequic_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"math/big"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"go.arpabet.com/value"
	valuequic "go.arpabet.com/value-rpc/quic"
	"go.arpabet.com/value-rpc/valueclient"
	"go.arpabet.com/value-rpc/valuerpc"
	"go.arpabet.com/value-rpc/valueserver"
	"go.uber.org/zap"
)

// genCertPair builds a throwaway CA and issues a loopback server certificate and
// a client certificate from it.
func genCertPair(t testing.TB) (caPool *x509.CertPool, server, client tls.Certificate) {
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
	srvDER := issue(&x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "127.0.0.1"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:     []string{"localhost"},
	}, caCert, srvKey.Public(), caKey)
	server = tls.Certificate{Certificate: [][]byte{srvDER}, PrivateKey: srvKey}

	cliKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	cliDER := issue(&x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject:      pkix.Name{CommonName: "vrpc-test-client"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}, caCert, cliKey.Public(), caKey)
	client = tls.Certificate{Certificate: [][]byte{cliDER}, PrivateKey: cliKey}
	return caPool, server, client
}

func serverAuth(t testing.TB, setup func(valueserver.Server)) (valueserver.Server, valueclient.Client) {
	t.Helper()
	caPool, serverCert, _ := genCertPair(t)
	srv, err := valuequic.NewServer("127.0.0.1:0",
		&tls.Config{Certificates: []tls.Certificate{serverCert}}, zap.NewNop())
	if err != nil {
		t.Fatalf("quic server: %v", err)
	}
	setup(srv)
	go srv.Run()
	cli := valuequic.NewClient(srv.Addr().String(), &tls.Config{RootCAs: caPool})
	return srv, cli
}

func TestQUIC_RoundTrip(t *testing.T) {
	srv, cli := serverAuth(t, func(s valueserver.Server) {
		s.AddFunction("echo", valuerpc.List(valuerpc.String), valuerpc.String,
			func(_ context.Context, args value.Value) (value.Value, error) {
				return value.Utf8("q:" + args.(value.List).GetStringAt(0).String()), nil
			})
	})
	defer srv.Close()
	if err := cli.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer cli.Close()
	cli.SetTimeout(5000)

	res, err := cli.CallFunction(context.Background(), "echo", value.Tuple(value.Utf8("hi")))
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if got := res.(value.String).String(); got != "q:hi" {
		t.Fatalf("result = %q, want %q", got, "q:hi")
	}
}

// TestQUIC_Matrix runs all four interaction patterns over QUIC (the coverage
// that used to live in the core transport matrix before QUIC was extracted).
func TestQUIC_Matrix(t *testing.T) {
	t.Run("unary", func(t *testing.T) {
		srv, cli := serverAuth(t, func(s valueserver.Server) {
			s.AddFunction("sq", valuerpc.List(valuerpc.Number), valuerpc.Number,
				func(_ context.Context, a value.Value) (value.Value, error) {
					n := a.(value.List).GetNumberAt(0).Long()
					return value.Long(n * n), nil
				})
		})
		defer srv.Close()
		mustConnect(t, cli)
		defer cli.Close()
		res, err := cli.CallFunction(context.Background(), "sq", value.Tuple(value.Long(9)))
		if err != nil {
			t.Fatalf("call: %v", err)
		}
		if got := res.(value.Number).Long(); got != 81 {
			t.Fatalf("sq(9) = %d, want 81", got)
		}
	})

	t.Run("serverStream", func(t *testing.T) {
		srv, cli := serverAuth(t, func(s valueserver.Server) {
			s.AddOutgoingStream("count", valuerpc.List(valuerpc.Number),
				func(_ context.Context, a value.Value) (<-chan value.Value, error) {
					n := a.(value.List).GetNumberAt(0).Long()
					out := make(chan value.Value)
					go func() {
						defer close(out)
						for i := int64(0); i < n; i++ {
							out <- value.Long(i)
						}
					}()
					return out, nil
				})
		})
		defer srv.Close()
		mustConnect(t, cli)
		defer cli.Close()
		readC, _, err := cli.GetStream(context.Background(), "count", value.Tuple(value.Long(5)), 16)
		if err != nil {
			t.Fatalf("get stream: %v", err)
		}
		var got int
		for v := range readC {
			if v == nil || v.Kind() == value.NULL {
				t.Fatalf("phantom Null on stream")
			}
			if v.(value.Number).Long() != int64(got) {
				t.Fatalf("value %d out of order", got)
			}
			got++
		}
		if got != 5 {
			t.Fatalf("received %d values, want 5", got)
		}
	})

	t.Run("clientStream", func(t *testing.T) {
		var mu sync.Mutex
		var sum int64
		done := make(chan struct{})
		srv, cli := serverAuth(t, func(s valueserver.Server) {
			s.AddIncomingStream("sum", valuerpc.Any,
				func(_ context.Context, a value.Value, inC <-chan value.Value) error {
					go func() {
						for v := range inC {
							if v != nil {
								mu.Lock()
								sum += v.(value.Number).Long()
								mu.Unlock()
							}
						}
						close(done)
					}()
					return nil
				})
		})
		defer srv.Close()
		mustConnect(t, cli)
		defer cli.Close()
		putC := make(chan value.Value, 4)
		if err := cli.PutStream(context.Background(), "sum", nil, putC); err != nil {
			t.Fatalf("put stream: %v", err)
		}
		for i := int64(1); i <= 4; i++ {
			putC <- value.Long(i)
		}
		close(putC)
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("server never saw end of stream")
		}
		mu.Lock()
		defer mu.Unlock()
		if sum != 10 {
			t.Fatalf("sum = %d, want 10", sum)
		}
	})

	t.Run("chat", func(t *testing.T) {
		srv, cli := serverAuth(t, func(s valueserver.Server) {
			s.AddChat("echo", valuerpc.Any,
				func(_ context.Context, a value.Value, inC <-chan value.Value) (<-chan value.Value, error) {
					out := make(chan value.Value)
					go func() {
						defer close(out)
						for v := range inC {
							out <- value.Utf8("c:" + v.(value.String).String())
						}
					}()
					return out, nil
				})
		})
		defer srv.Close()
		mustConnect(t, cli)
		defer cli.Close()
		sendC := make(chan value.Value, 3)
		readC, _, err := cli.Chat(context.Background(), "echo", nil, 16, sendC)
		if err != nil {
			t.Fatalf("chat: %v", err)
		}
		inputs := []string{"a", "bb", "ccc"}
		go func() {
			for _, s := range inputs {
				sendC <- value.Utf8(s)
			}
			close(sendC)
		}()
		var got []string
		for v := range readC {
			if v != nil && v.Kind() != value.NULL {
				got = append(got, v.(value.String).String())
			}
		}
		if len(got) != len(inputs) {
			t.Fatalf("received %d echoes %v, want %d", len(got), got, len(inputs))
		}
		for i, s := range inputs {
			if got[i] != "c:"+s {
				t.Fatalf("echo[%d] = %q", i, got[i])
			}
		}
	})
}

// TestQUIC_MutualAuth verifies mTLS over QUIC and PeerCertificates in the
// connect-authorizer.
func TestQUIC_MutualAuth(t *testing.T) {
	caPool, serverCert, clientCert := genCertPair(t)
	srv, err := valuequic.NewServer("127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientCAs:    caPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
	}, zap.NewNop())
	if err != nil {
		t.Fatalf("quic server: %v", err)
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
		func(_ context.Context, args value.Value) (value.Value, error) { return value.Utf8("pong"), nil })
	go srv.Run()

	cli := valuequic.NewClient(srv.Addr().String(), &tls.Config{
		RootCAs:      caPool,
		Certificates: []tls.Certificate{clientCert},
	})
	if err := cli.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer cli.Close()
	cli.SetTimeout(5000)

	if _, err := cli.CallFunction(context.Background(), "ping", nil); err != nil {
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

// TestQUIC_StreamsAreFreed runs many sequential calls with a tiny stream cap: a
// stream leak would hit the cap and block.
func TestQUIC_StreamsAreFreed(t *testing.T) {
	old := valuequic.MaxStreams
	valuequic.MaxStreams = 8
	defer func() { valuequic.MaxStreams = old }()

	srv, cli := serverAuth(t, func(s valueserver.Server) {
		s.AddFunction("inc", valuerpc.List(valuerpc.Number), valuerpc.Number,
			func(_ context.Context, a value.Value) (value.Value, error) {
				return value.Long(a.(value.List).GetNumberAt(0).Long() + 1), nil
			})
	})
	defer srv.Close()
	if err := cli.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer cli.Close()
	cli.SetTimeout(3000)

	for i := int64(0); i < 50; i++ {
		res, err := cli.CallFunction(context.Background(), "inc", value.Tuple(value.Long(i)))
		if err != nil {
			t.Fatalf("call %d failed (stream leak would block here): %v", i, err)
		}
		if got := res.(value.Number).Long(); got != i+1 {
			t.Fatalf("inc(%d) = %d, want %d", i, got, i+1)
		}
	}
}

func mustConnect(t testing.TB, cli valueclient.Client) {
	t.Helper()
	if err := cli.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	cli.SetTimeout(5000)
}

// TestQUIC_Concurrent fires many concurrent unary calls over one connection,
// exercising concurrent stream opening and per-request-id routing under the race
// detector; each caller must get exactly its own answer.
func TestQUIC_Concurrent(t *testing.T) {
	srv, cli := serverAuth(t, func(s valueserver.Server) {
		s.AddFunction("square", valuerpc.List(valuerpc.Number), valuerpc.Number,
			func(_ context.Context, a value.Value) (value.Value, error) {
				n := a.(value.List).GetNumberAt(0).Long()
				return value.Long(n * n), nil
			})
	})
	defer srv.Close()
	mustConnect(t, cli)
	defer cli.Close()

	const workers = 32
	const callsPer = 25
	var wg sync.WaitGroup
	errc := make(chan error, workers*callsPer)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for i := 0; i < callsPer; i++ {
				n := int64(base*callsPer + i)
				res, err := cli.CallFunction(context.Background(), "square", value.Tuple(value.Long(n)))
				if err != nil {
					errc <- fmt.Errorf("n=%d: %w", n, err)
					return
				}
				if got := res.(value.Number).Long(); got != n*n {
					errc <- fmt.Errorf("square(%d) = %d, want %d (mis-routed?)", n, got, n*n)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errc)
	for err := range errc {
		t.Error(err)
	}
}

// TestQUIC_MultipleConcurrentStreams opens several server-streams at once over
// one connection — the per-request-stream payoff — and checks each stream gets
// exactly its own ordered values (no cross-talk).
func TestQUIC_MultipleConcurrentStreams(t *testing.T) {
	srv, cli := serverAuth(t, func(s valueserver.Server) {
		s.AddOutgoingStream("seq", valuerpc.List(valuerpc.Number, valuerpc.Number),
			func(_ context.Context, a value.Value) (<-chan value.Value, error) {
				l := a.(value.List)
				start := l.GetNumberAt(0).Long()
				cnt := l.GetNumberAt(1).Long()
				out := make(chan value.Value)
				go func() {
					defer close(out)
					for i := int64(0); i < cnt; i++ {
						out <- value.Long(start + i)
					}
				}()
				return out, nil
			})
	})
	defer srv.Close()
	mustConnect(t, cli)
	defer cli.Close()

	const streams = 8
	const per = 200
	var wg sync.WaitGroup
	errc := make(chan error, streams)
	for s := 0; s < streams; s++ {
		wg.Add(1)
		go func(base int64) {
			defer wg.Done()
			readC, _, err := cli.GetStream(context.Background(), "seq", value.Tuple(value.Long(base*per), value.Long(per)), 32)
			if err != nil {
				errc <- fmt.Errorf("stream %d: %w", base, err)
				return
			}
			want := base * per
			for v := range readC {
				if v == nil || v.Kind() == value.NULL {
					continue
				}
				if got := v.(value.Number).Long(); got != want {
					errc <- fmt.Errorf("stream %d: got %d want %d (cross-talk)", base, got, want)
					return
				}
				want++
			}
			if want != base*per+per {
				errc <- fmt.Errorf("stream %d incomplete: ended at %d", base, want)
			}
		}(int64(s))
	}
	wg.Wait()
	close(errc)
	for err := range errc {
		t.Error(err)
	}
}

// TestQUIC_LargeServerStream streams many values over a single QUIC stream,
// exercising backpressure/throttle and the read funnel.
func TestQUIC_LargeServerStream(t *testing.T) {
	const n = 5000
	srv, cli := serverAuth(t, func(s valueserver.Server) {
		s.AddOutgoingStream("range", valuerpc.Any,
			func(_ context.Context, a value.Value) (<-chan value.Value, error) {
				out := make(chan value.Value)
				go func() {
					defer close(out)
					for i := 0; i < n; i++ {
						out <- value.Long(int64(i))
					}
				}()
				return out, nil
			})
	})
	defer srv.Close()
	mustConnect(t, cli)
	defer cli.Close()
	cli.SetTimeout(10000)

	readC, _, err := cli.GetStream(context.Background(), "range", nil, 128)
	if err != nil {
		t.Fatalf("get stream: %v", err)
	}
	count := 0
	for v := range readC {
		if v == nil || v.Kind() == value.NULL {
			continue
		}
		if v.(value.Number).Long() != int64(count) {
			t.Fatalf("value %d out of order: %d", count, v.(value.Number).Long())
		}
		count++
	}
	if count != n {
		t.Fatalf("received %d values, want %d", count, n)
	}
}

// TestQUIC_RejectsClientWithoutCert: an mTLS QUIC server must reject a client
// that presents no certificate (rejection surfaces at connect or first call).
func TestQUIC_RejectsClientWithoutCert(t *testing.T) {
	caPool, serverCert, _ := genCertPair(t)
	srv, err := valuequic.NewServer("127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientCAs:    caPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
	}, zap.NewNop())
	if err != nil {
		t.Fatalf("quic server: %v", err)
	}
	defer srv.Close()
	srv.AddFunction("ping", valuerpc.Void, valuerpc.String,
		func(_ context.Context, a value.Value) (value.Value, error) { return value.Utf8("pong"), nil })
	go srv.Run()

	cli := valuequic.NewClient(srv.Addr().String(), &tls.Config{RootCAs: caPool}) // no client cert
	defer cli.Close()
	cli.SetTimeout(800)
	if err := cli.Connect(); err == nil {
		if _, callErr := cli.CallFunction(context.Background(), "ping", nil); callErr == nil {
			t.Fatal("expected mTLS to reject a client with no certificate")
		}
	}
}

// TestQUIC_MaxFrameSize verifies the read-limit is enforced over QUIC.
func TestQUIC_MaxFrameSize(t *testing.T) {
	old := valuerpc.MaxFrameSize
	valuerpc.MaxFrameSize = 512
	defer func() { valuerpc.MaxFrameSize = old }()

	srv, cli := serverAuth(t, func(s valueserver.Server) {
		s.AddFunction("recv", valuerpc.Any, valuerpc.String,
			func(_ context.Context, a value.Value) (value.Value, error) { return value.Utf8("ok"), nil })
	})
	defer srv.Close()
	mustConnect(t, cli)
	defer cli.Close()
	cli.SetTimeout(800)

	if _, err := cli.CallFunction(context.Background(), "recv", value.Tuple(value.Utf8("small"))); err != nil {
		t.Fatalf("small call should succeed: %v", err)
	}
	big := strings.Repeat("x", 4096)
	if _, err := cli.CallFunction(context.Background(), "recv", value.Tuple(value.Utf8(big))); err == nil {
		t.Fatal("expected an over-limit message to be rejected")
	}
}

func BenchmarkQUICUnary(b *testing.B) {
	srv, cli := serverAuth(b, func(s valueserver.Server) {
		s.AddFunction("noop", valuerpc.List(valuerpc.Number), valuerpc.Number,
			func(_ context.Context, a value.Value) (value.Value, error) { return a.(value.List).GetNumberAt(0), nil })
	})
	defer srv.Close()
	if err := cli.Connect(); err != nil {
		b.Fatalf("connect: %v", err)
	}
	defer cli.Close()
	cli.SetTimeout(10000)

	arg := value.Tuple(value.Long(1))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := cli.CallFunction(context.Background(), "noop", arg); err != nil {
			b.Fatal(err)
		}
	}
}
