/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valuequic_test

import (
	"context"
	"testing"
	"time"

	"go.arpabet.com/value"
	valuequic "go.arpabet.com/value-rpc/quic"
	"go.arpabet.com/value-rpc/valueclient"
	"go.arpabet.com/value-rpc/valuerpc"
	"go.arpabet.com/value-rpc/valueserver"
)

// These tests are about the half of a QUIC stream that belongs to the side
// that opened a chat. A chat goes on after that side has sent its last value:
// the peer still streams, until it ends the chat, and has to be granted credit
// (StreamCredit) for what it sends. The half must therefore not be FIN'd with
// the initiator's StreamEnd, or the grants are dropped and the peer stalls
// after the values it had credit for. It is the chat's counterpart of
// TestQUIC_LargePutStream.

// TestQUIC_ChatStreamByItsMessages follows the stream of a chat message by
// message, for each way a chat ends: what the initiator writes after its own
// StreamEnd arrives, and the stream is freed, at both ends, when the responder
// has ended the chat.
func TestQUIC_ChatStreamByItsMessages(t *testing.T) {
	wires(t, func(w *wire, rid func(int64) int64) {
		// The initiator ends first: its half stays open for the credit it
		// grants, and is closed when the responder ends the chat.
		w.passes(w.opener, valuerpc.ChatRequest, rid(1))
		w.passes(w.responder, valuerpc.StreamReady, rid(1))
		w.passes(w.responder, valuerpc.StreamCredit, rid(1))
		w.passes(w.opener, valuerpc.StreamValue, rid(1))
		w.passes(w.opener, valuerpc.StreamEnd, rid(1))
		w.passes(w.opener, valuerpc.StreamCredit, rid(1)) // after its own end
		w.passes(w.responder, valuerpc.StreamValue, rid(1))
		w.passes(w.opener, valuerpc.StreamCredit, rid(1))
		w.passes(w.responder, valuerpc.StreamEnd, rid(1))
		w.freed("the initiator ended first, then the responder")

		// The responder ends first, which ends the chat: the initiator sends
		// nothing after that, and closes its half without a StreamEnd.
		w.passes(w.opener, valuerpc.ChatRequest, rid(2))
		w.passes(w.responder, valuerpc.StreamReady, rid(2))
		w.passes(w.opener, valuerpc.StreamValue, rid(2))
		w.passes(w.responder, valuerpc.StreamEnd, rid(2))
		w.freed("the responder ended the chat")
		// What the application of the initiator still had to say is not sent.
		w.sends(w.opener, valuerpc.StreamValue, rid(2))
		w.sends(w.opener, valuerpc.StreamEnd, rid(2))
		w.holdsNone("after the initiator said its last to a chat that is over")

		// The responder refuses the chat.
		w.passes(w.opener, valuerpc.ChatRequest, rid(3))
		w.passes(w.responder, valuerpc.ErrorResponse, rid(3))
		w.freed("the responder refused the chat")

		// The initiator cancels, and the responder ends what it streams.
		w.passes(w.opener, valuerpc.ChatRequest, rid(4))
		w.passes(w.responder, valuerpc.StreamReady, rid(4))
		w.passes(w.opener, valuerpc.CancelRequest, rid(4))
		w.passes(w.responder, valuerpc.StreamEnd, rid(4))
		w.freed("the initiator cancelled the chat")

		// The connection is none the worse.
		w.passes(w.opener, valuerpc.FunctionRequest, rid(5))
		w.passes(w.responder, valuerpc.FunctionResponse, rid(5))
		w.freed("after a call")
	})
}

// replay is a chat that takes all its peer has to say and answers only when
// the peer has said its last: every value it sends, it sends after the
// initiator's StreamEnd, so each needs credit that is granted after it.
func replay(_ context.Context, _ value.Value, inC <-chan value.Value) (<-chan value.Value, error) {
	out := make(chan value.Value)
	go func() {
		defer close(out)
		var all []value.Value
		for v := range inC {
			if v != nil && v.Kind() != value.NULL {
				all = append(all, v)
			}
		}
		for _, v := range all {
			out <- v
		}
	}()
	return out, nil
}

// talk sends the numbers 0..n-1 on a chat, ends its side, and reads what comes
// back: the same numbers, in order, and then the end of the chat.
func talk(ctx context.Context, peer valuerpc.Peer, name string, n int) (int, error) {
	sendC := make(chan value.Value)
	readC, _, err := peer.Chat(ctx, name, nil, 16, sendC)
	if err != nil {
		return 0, err
	}
	go func() {
		defer close(sendC)
		for i := 0; i < n; i++ {
			select {
			case sendC <- value.Long(int64(i)):
			case <-ctx.Done():
				return
			}
		}
	}()
	count := 0
	for {
		select {
		case v, ok := <-readC:
			if !ok {
				return count, nil
			}
			if v == nil || v.Kind() == value.NULL {
				continue
			}
			if got := v.(value.Number).Long(); got != int64(count) {
				return count, valuerpc.NewError(valuerpc.CodeInternal, "value %d came back as %d", count, got)
			}
			count++
		case <-ctx.Done():
			return count, ctx.Err()
		}
	}
}

// TestQUIC_ChatInitiatorGrantsCreditAfterItsOwnEnd: the client says all it has
// to say and ends its side; the server answers only then, far more than the
// window. The client has to go on granting credit with its side ended.
func TestQUIC_ChatInitiatorGrantsCreditAfterItsOwnEnd(t *testing.T) {
	old := valuerpc.DefaultMaxPending
	valuerpc.DefaultMaxPending = 16 // tiny window so replenishment is required
	defer func() { valuerpc.DefaultMaxPending = old }()

	const n = 500
	srv, cli := serverAuth(t, func(s valueserver.Server) {
		s.AddChat("replay", valuerpc.Any, replay)
	})
	defer srv.Close()
	mustConnect(t, cli)
	defer cli.Close()
	cli.SetTimeout(8000)

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	count, err := talk(ctx, cli, "replay", n)
	if err != nil {
		t.Fatalf("the chat stalled after %d of %d values came back (the credit the initiator grants after its own StreamEnd was dropped): %v",
			count, n, err)
	}
	if count != n {
		t.Fatalf("%d values came back, want %d", count, n)
	}
}

// TestQUIC_ReverseChatInitiatorGrantsCreditAfterItsOwnEnd: the same the other
// way round. The server opens the chat toward its client, says its last, and
// the client answers only then: here the server is the one that has to go on
// granting credit.
func TestQUIC_ReverseChatInitiatorGrantsCreditAfterItsOwnEnd(t *testing.T) {
	old := valuerpc.DefaultMaxPending
	valuerpc.DefaultMaxPending = 16
	defer func() { valuerpc.DefaultMaxPending = old }()

	const n = 500
	srv, cli := serverAuth(t, func(s valueserver.Server) {
		s.AddFunction("startChat", valuerpc.Any, valuerpc.Any,
			func(ctx context.Context, _ value.Value) (value.Value, error) {
				peer, ok := valueserver.PeerFromContext(ctx)
				if !ok {
					return nil, valuerpc.NewError(valuerpc.CodeInternal, "no peer")
				}
				count, err := talk(ctx, peer, "replay", n)
				if err != nil {
					return nil, valuerpc.NewError(valuerpc.CodeInternal, "stalled after %d of %d: %v", count, n, err)
				}
				return value.Long(int64(count)), nil
			})
	})
	defer srv.Close()
	cli.AddChat("replay", valuerpc.Any, replay)
	mustConnect(t, cli)
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	res, err := cli.CallFunction(ctx, "startChat", nil)
	if err != nil {
		t.Fatalf("reverse chat over quic: %v", err)
	}
	if got := res.(value.Number).Long(); got != n {
		t.Fatalf("%d values came back to the server, want %d", got, n)
	}
}

// TestQUIC_ChatStreamsAreFreed runs many chats, of every way a chat can end,
// with a tiny stream cap: a half that is not FIN'd leaves its stream open, and
// the cap is soon reached.
func TestQUIC_ChatStreamsAreFreed(t *testing.T) {
	old := valuequic.MaxStreams
	valuequic.MaxStreams = 8
	defer func() { valuequic.MaxStreams = old }()

	srv, cli := serverAuth(t, func(s valueserver.Server) {
		s.AddChat("replay", valuerpc.Any, replay)
		// Says nothing and ends its side at once, which ends the chat.
		s.AddChat("sink", valuerpc.Any,
			func(_ context.Context, _ value.Value, inC <-chan value.Value) (<-chan value.Value, error) {
				out := make(chan value.Value)
				close(out)
				go func() {
					for range inC {
					}
				}()
				return out, nil
			})
		// Refuses the chat when it is opened.
		s.AddChat("refuse", valuerpc.Any,
			func(_ context.Context, _ value.Value, _ <-chan value.Value) (<-chan value.Value, error) {
				return nil, valuerpc.NewError(valuerpc.CodeInvalidArgument, "not today")
			})
	})
	defer srv.Close()
	mustConnect(t, cli)
	defer cli.Close()
	cli.SetTimeout(3000)

	chat := func(what string, i int, run func(ctx context.Context) error) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := run(ctx); err != nil {
			t.Fatalf("%s, chat %d (a stream that was not freed blocks the next): %v", what, i, err)
		}
	}

	for i := 0; i < 30; i++ {
		chat("the initiator ends first", i, func(ctx context.Context) error {
			count, err := talk(ctx, cli, "replay", 5)
			if err == nil && count != 5 {
				err = valuerpc.NewError(valuerpc.CodeInternal, "%d of 5 values came back", count)
			}
			return err
		})
		chat("the responder ends first", i, func(ctx context.Context) error {
			sendC := make(chan value.Value)
			defer close(sendC)
			readC, _, err := cli.Chat(ctx, "sink", nil, 16, sendC)
			if err != nil {
				return err
			}
			for {
				select {
				case _, ok := <-readC:
					if !ok {
						return nil
					}
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		})
		chat("the responder refuses", i, func(ctx context.Context) error {
			sendC := make(chan value.Value)
			defer close(sendC)
			_, _, err := cli.Chat(ctx, "refuse", nil, 16, sendC)
			if valuerpc.CodeOf(err) != valuerpc.CodeInvalidArgument {
				return valuerpc.NewError(valuerpc.CodeInternal, "the refusal came as %v", err)
			}
			return nil
		})
	}

	// The connection is none the worse.
	if _, err := talkOnce(cli); err != nil {
		t.Fatalf("a chat after all of them: %v", err)
	}
}

func talkOnce(cli valueclient.Client) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return talk(ctx, cli, "replay", 3)
}
