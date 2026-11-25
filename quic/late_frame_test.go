/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valuequic_test

import (
	"context"
	"crypto/tls"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.arpabet.com/value"
	valuequic "go.arpabet.com/value-rpc/quic"
	"go.arpabet.com/value-rpc/valuerpc"
	"go.arpabet.com/value-rpc/valueserver"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// These tests are about a message of a request whose stream is freed already,
// both halves closed: the credit for values the consumer of a stream took after
// the stream had ended, an end or a cancellation that crossed the end the peer
// sent. Such a message has nowhere to go. It must not open a stream of its own,
// which nothing would ever close, and it must not fail the write, which costs
// the writer more than the message.

// wire drives the two ends of a QUIC connection message by message, in place
// of a client and a server: opener is the end that makes the requests.
type wire struct {
	t                 *testing.T
	opener, responder valuerpc.MsgConn
	who               map[valuerpc.MsgConn]string
}

// wires gives the two ends of a connection in both directions: with the client
// making the requests, of positive ids, and with the server making them of its
// client, of negative ids. The rules of a stream go by who opened it, not by
// who is the client.
func wires(t *testing.T, run func(w *wire, rid func(n int64) int64)) {
	for _, tc := range []struct {
		name string
		sign int64
	}{
		{"the client makes the requests", 1},
		{"the server makes the requests", -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, cli := quicEnds(t)
			w := &wire{t: t, opener: cli, responder: srv}
			if tc.sign < 0 {
				w.opener, w.responder = srv, cli
			}
			w.who = map[valuerpc.MsgConn]string{w.opener: "the opener", w.responder: "the responder"}
			run(w, func(n int64) int64 { return tc.sign * n })
		})
	}
}

// sends writes a message of a request. The transport reads no more of a message
// than its type and the id of its request.
func (w *wire) sends(from valuerpc.MsgConn, mt valuerpc.MessageType, rid int64) {
	w.t.Helper()
	m := value.ImmutableMapOf(map[string]value.Value{
		valuerpc.DefaultDialect.MessageTypeField: mt.Long(),
		valuerpc.DefaultDialect.RequestIdField:   value.Long(rid),
	})
	if err := from.WriteMessage(m); err != nil {
		w.t.Fatalf("%s writes %s of request %d: %v", w.who[from], valuequic.MessageName(mt), rid, err)
	}
}

// reads takes the next message that came to an end, which has to be this one.
func (w *wire) reads(at valuerpc.MsgConn, mt valuerpc.MessageType, rid int64) {
	w.t.Helper()
	select {
	case r := <-readInBackground(at):
		if r.err != nil {
			w.t.Fatalf("%s waits for %s of request %d: %v", w.who[at], valuequic.MessageName(mt), rid, r.err)
		}
		gotType, _ := valuerpc.GetNumberField(r.msg, valuerpc.DefaultDialect.MessageTypeField)
		gotRid, _ := valuerpc.GetNumberField(r.msg, valuerpc.DefaultDialect.RequestIdField)
		if valuerpc.MessageType(gotType.Long()) != mt || gotRid.Long() != rid {
			w.t.Fatalf("%s read %s of request %d, want %s of request %d", w.who[at],
				valuequic.MessageName(valuerpc.MessageType(gotType.Long())), gotRid.Long(), valuequic.MessageName(mt), rid)
		}
	case <-time.After(3 * time.Second):
		w.t.Fatalf("%s of request %d did not come to %s", valuequic.MessageName(mt), rid, w.who[at])
	}
}

// passes is a message written at one end and read at the other.
func (w *wire) passes(from valuerpc.MsgConn, mt valuerpc.MessageType, rid int64) {
	w.t.Helper()
	to := w.responder
	if from == w.responder {
		to = w.opener
	}
	w.sends(from, mt, rid)
	w.reads(to, mt, rid)
}

// freed waits until neither end holds a stream: every request is over at both
// of its halves, at both ends.
func (w *wire) freed(when string) {
	w.t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		atOpener, atResponder := valuequic.StreamsHeld(w.opener), valuequic.StreamsHeld(w.responder)
		if atOpener == 0 && atResponder == 0 {
			return
		}
		if time.Now().After(deadline) {
			w.t.Fatalf("%s: the opener still holds %d stream(s) and the responder %d", when, atOpener, atResponder)
		}
		time.Sleep(time.Millisecond)
	}
}

// holdsNone checks, at once, that no end holds a stream.
func (w *wire) holdsNone(when string) {
	w.t.Helper()
	if atOpener, atResponder := valuequic.StreamsHeld(w.opener), valuequic.StreamsHeld(w.responder); atOpener != 0 || atResponder != 0 {
		w.t.Fatalf("%s: the opener holds %d stream(s) and the responder %d; a stream was opened for a message that opens no request",
			when, atOpener, atResponder)
	}
}

// TestQUIC_AFrameOfAFreedStreamIsDropped: after a request is over and its
// stream freed, either end may still write a message of it. The write succeeds,
// no stream is opened for it, and nothing of it reaches the peer.
func TestQUIC_AFrameOfAFreedStreamIsDropped(t *testing.T) {
	wires(t, func(w *wire, rid func(int64) int64) {
		// A get-stream, from the request to its end.
		w.passes(w.opener, valuerpc.GetStreamRequest, rid(1))
		w.passes(w.responder, valuerpc.StreamReady, rid(1))
		w.passes(w.opener, valuerpc.StreamCredit, rid(1))
		w.passes(w.responder, valuerpc.StreamValue, rid(1))
		w.passes(w.responder, valuerpc.StreamEnd, rid(1))
		w.freed("after the end of the stream")

		// The consumer takes the values it holds, and grants credit for them;
		// its application cancels a request that is over.
		w.sends(w.opener, valuerpc.StreamCredit, rid(1))
		w.sends(w.opener, valuerpc.CancelRequest, rid(1))
		// The producer had one more to say.
		w.sends(w.responder, valuerpc.StreamValue, rid(1))
		w.sends(w.responder, valuerpc.StreamEnd, rid(1))
		// The same of a request that never was.
		w.sends(w.opener, valuerpc.StreamCredit, rid(99))
		w.sends(w.responder, valuerpc.StreamCredit, rid(99))
		w.holdsNone("after the late frames")

		// Nothing of it was sent: the next message each end reads is of the
		// next request, which goes as any other.
		w.passes(w.opener, valuerpc.FunctionRequest, rid(2))
		w.passes(w.responder, valuerpc.FunctionResponse, rid(2))
		w.freed("after the next request")
	})
}

// TestQUIC_ARequestIsOpenedByItsOwnerOnly: the message that opens a request
// opens its stream, at the end that owns the id: the client the positive ones,
// the server the negative ones. At the other end it is refused, as it was.
func TestQUIC_ARequestIsOpenedByItsOwnerOnly(t *testing.T) {
	wires(t, func(w *wire, rid func(int64) int64) {
		m := value.ImmutableMapOf(map[string]value.Value{
			valuerpc.DefaultDialect.MessageTypeField: valuerpc.FunctionRequest.Long(),
			valuerpc.DefaultDialect.RequestIdField:   value.Long(rid(1)),
		})
		if err := w.responder.WriteMessage(m); err == nil {
			t.Fatal("a request with an id of the other end was written")
		}
		w.holdsNone("after the refused request")
		w.passes(w.opener, valuerpc.FunctionRequest, rid(1))
		w.passes(w.responder, valuerpc.FunctionResponse, rid(1))
		w.freed("after the request")
	})
}

// connectLogged is a client connected to a server whose errors are kept.
func connectLogged(t *testing.T, setup func(valueserver.Server)) (valuerpc.Peer, *observer.ObservedLogs) {
	t.Helper()
	caPool, serverCert, _ := genCertPair(t)
	core, logs := observer.New(zapcore.ErrorLevel)
	srv, err := valuequic.NewServer("127.0.0.1:0",
		&tls.Config{Certificates: []tls.Certificate{serverCert}}, zap.New(core))
	if err != nil {
		t.Fatalf("quic server: %v", err)
	}
	setup(srv)
	go srv.Run()
	t.Cleanup(func() { srv.Close() })

	cli := valuequic.NewClient(srv.Addr().String(), &tls.Config{RootCAs: caPool})
	mustConnect(t, cli)
	t.Cleanup(func() { cli.Close() })
	cli.SetTimeout(10000)
	return cli, logs
}

// TestQUIC_LateCreditOfAPutStreamCostsNoOtherRequest: a server whose handler
// lags takes the last values of a put-stream after the stream has ended, and
// grants credit for them then. That write used to fail, the stream being
// freed, and the sender of the server gave up the batch it was in: the answer
// to another request, now and then, which its caller waited for until its
// timeout.
func TestQUIC_LateCreditOfAPutStreamCostsNoOtherRequest(t *testing.T) {
	old := valuerpc.DefaultMaxPending
	valuerpc.DefaultMaxPending = 16 // a small window: credit is granted every 8 values
	defer func() { valuerpc.DefaultMaxPending = old }()

	cli, logs := connectLogged(t, func(s valueserver.Server) {
		s.AddIncomingStream("slow", valuerpc.Any,
			func(_ context.Context, _ value.Value, inC <-chan value.Value) error {
				go func() {
					for range inC {
						time.Sleep(200 * time.Microsecond) // the values wait, and are taken after the end
					}
				}()
				return nil
			})
		s.AddFunction("echo", valuerpc.Any, valuerpc.Any,
			func(_ context.Context, a value.Value) (value.Value, error) { return a, nil })
	})
	// Calls go on over the same connection all the while.
	stop := make(chan struct{})
	var calls, lost atomic.Int64
	var callers sync.WaitGroup
	for g := 0; g < 8; g++ {
		callers.Add(1)
		go func() {
			defer callers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				_, err := cli.CallFunction(ctx, "echo", value.Long(1))
				cancel()
				calls.Add(1)
				if err != nil {
					lost.Add(1)
				}
			}
		}()
	}

	const puts = 100
	for i := 0; i < puts; i++ {
		putC := make(chan value.Value, 40)
		for j := 0; j < 40; j++ {
			putC <- value.Long(int64(j))
		}
		close(putC)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := cli.PutStream(ctx, "slow", nil, putC)
		cancel()
		if err != nil {
			close(stop)
			callers.Wait()
			t.Fatalf("put-stream %d of %d: %v", i+1, puts, err)
		}
	}
	close(stop)
	callers.Wait()

	if n := lost.Load(); n != 0 {
		t.Errorf("%d of the %d calls made alongside got no answer", n, calls.Load())
	}
	for _, e := range logs.All() {
		t.Errorf("the server reported an error: %s: %v", e.Message, e.ContextMap()["error"])
	}
}

// TestQUIC_LateCreditOfAReversePutStreamKeepsTheConnection: the same the other
// way round. Here the server streams to its client, whose handler lags, and it
// is the client that grants credit for a stream that is freed. Its write used
// to fail, and a client takes a failed write for a lost connection: it dropped
// a connection that was in order, and dialed again.
func TestQUIC_LateCreditOfAReversePutStreamKeepsTheConnection(t *testing.T) {
	old := valuerpc.DefaultMaxPending
	valuerpc.DefaultMaxPending = 16
	defer func() { valuerpc.DefaultMaxPending = old }()

	srv, cli := serverAuth(t, func(s valueserver.Server) {
		s.AddFunction("upload", valuerpc.Any, valuerpc.Any,
			func(ctx context.Context, _ value.Value) (value.Value, error) {
				peer, ok := valueserver.PeerFromContext(ctx)
				if !ok {
					return nil, valuerpc.NewError(valuerpc.CodeInternal, "no peer")
				}
				putC := make(chan value.Value, 40)
				for j := 0; j < 40; j++ {
					putC <- value.Long(int64(j))
				}
				close(putC)
				return nil, peer.PutStream(ctx, "slow", nil, putC)
			})
	})
	defer srv.Close()
	cli.AddIncomingStream("slow", valuerpc.Any,
		func(_ context.Context, _ value.Value, inC <-chan value.Value) error {
			go func() {
				for range inC {
					time.Sleep(200 * time.Microsecond) // the values wait, and are taken after the end
				}
			}()
			return nil
		})
	mustConnect(t, cli)
	defer cli.Close()
	cli.SetTimeout(10000)

	const puts = 100
	for i := 0; i < puts; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_, err := cli.CallFunction(ctx, "upload", nil)
		cancel()
		if err != nil {
			t.Fatalf("put-stream %d of %d: %v", i+1, puts, err)
		}
		time.Sleep(5 * time.Millisecond) // the handler takes what it holds
	}
	if n := cli.Stats()["reconnects"]; n != 0 {
		t.Fatalf("the client reconnected %d time(s) in %d put-streams, over a connection that was in order", n, puts)
	}
}
