/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valueserver_test

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valueclient"
	"go.arpabet.com/value-rpc/valueserver"
	"go.uber.org/zap"

	"go.arpabet.com/value-rpc/valuerpc"
)

// transportFactory starts a server (with setup applied) over one transport and
// returns it plus a matching, not-yet-connected client.
type transportFactory struct {
	name  string
	start func(t *testing.T, setup func(valueserver.Server)) (valueserver.Server, valueclient.Client)
}

func allTransports() []transportFactory {
	return []transportFactory{
		{"tcp", func(t *testing.T, setup func(valueserver.Server)) (valueserver.Server, valueclient.Client) {
			srv, err := valueserver.NewServer("127.0.0.1:0", zap.NewNop())
			if err != nil {
				t.Fatalf("server: %v", err)
			}
			setup(srv)
			go srv.Run()
			return srv, valueclient.NewClient(srv.Addr().String(), "")
		}},
		{"unix", func(t *testing.T, setup func(valueserver.Server)) (valueserver.Server, valueclient.Client) {
			sock := tmpSock(t)
			t.Cleanup(func() { os.Remove(sock) })
			srv, err := valueserver.NewUnixServer(sock, zap.NewNop())
			if err != nil {
				t.Fatalf("server: %v", err)
			}
			setup(srv)
			go srv.Run()
			return srv, valueclient.NewUnixClient(sock)
		}},
		{"websocket", func(t *testing.T, setup func(valueserver.Server)) (valueserver.Server, valueclient.Client) {
			srv, err := valueserver.NewWebSocketServer("127.0.0.1:0", "/rpc", zap.NewNop())
			if err != nil {
				t.Fatalf("server: %v", err)
			}
			setup(srv)
			go srv.Run()
			return srv, valueclient.NewWebSocketClient("ws://" + srv.Addr().String() + "/rpc")
		}},
		{"mem", func(t *testing.T, setup func(valueserver.Server)) (valueserver.Server, valueclient.Client) {
			name := t.Name() // unique per subtest
			srv, err := valueserver.NewMemServer(name, zap.NewNop())
			if err != nil {
				t.Fatalf("server: %v", err)
			}
			setup(srv)
			go srv.Run()
			return srv, valueclient.NewMemClient(name)
		}},
		// QUIC lives in the go.arpabet.com/value-rpc/quic submodule (to keep
		// quic-go out of the core); its four-pattern matrix is tested there.
	}
}

// TestTransportMatrix runs all four interaction patterns over every transport,
// giving a single cross-transport correctness guarantee.
func TestTransportMatrix(t *testing.T) {
	for _, tr := range allTransports() {
		t.Run(tr.name, func(t *testing.T) {

			t.Run("unary", func(t *testing.T) {
				srv, cli := tr.start(t, func(s valueserver.Server) {
					s.AddFunction("echo", valuerpc.List(valuerpc.String), valuerpc.String,
						func(_ context.Context, args value.Value) (value.Value, error) {
							return value.Utf8("e:" + args.(value.List).GetStringAt(0).String()), nil
						})
				})
				defer srv.Close()
				if err := cli.Connect(); err != nil {
					t.Fatalf("connect: %v", err)
				}
				defer cli.Close()
				cli.SetTimeout(5000)

				res, err := cli.CallFunction("echo", value.Tuple(value.Utf8("hi")))
				if err != nil {
					t.Fatalf("call: %v", err)
				}
				if got := res.(value.String).String(); got != "e:hi" {
					t.Fatalf("result = %q, want %q", got, "e:hi")
				}
			})

			t.Run("serverStream", func(t *testing.T) {
				srv, cli := tr.start(t, func(s valueserver.Server) {
					s.AddOutgoingStream("count", valuerpc.List(valuerpc.Number),
						func(_ context.Context, args value.Value) (<-chan value.Value, error) {
							n := args.(value.List).GetNumberAt(0).Long()
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
				if err := cli.Connect(); err != nil {
					t.Fatalf("connect: %v", err)
				}
				defer cli.Close()
				cli.SetTimeout(5000)

				readC, _, err := cli.GetStream("count", value.Tuple(value.Long(5)), 16)
				if err != nil {
					t.Fatalf("get stream: %v", err)
				}
				var got int
				for v := range readC {
					if v == nil || v.Kind() == value.NULL {
						t.Fatalf("phantom Null on stream")
					}
					if v.(value.Number).Long() != int64(got) {
						t.Fatalf("value %d out of order: %d", got, v.(value.Number).Long())
					}
					got++
				}
				if got != 5 {
					t.Fatalf("received %d values, want 5", got)
				}
			})

			t.Run("clientStream", func(t *testing.T) {
				var (
					mu   sync.Mutex
					sum  int64
					done = make(chan struct{})
				)
				srv, cli := tr.start(t, func(s valueserver.Server) {
					s.AddIncomingStream("sum", valuerpc.Any,
						func(_ context.Context, args value.Value, inC <-chan value.Value) error {
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
				if err := cli.Connect(); err != nil {
					t.Fatalf("connect: %v", err)
				}
				defer cli.Close()
				cli.SetTimeout(5000)

				putC := make(chan value.Value, 4)
				if err := cli.PutStream("sum", nil, putC); err != nil {
					t.Fatalf("put stream: %v", err)
				}
				for i := int64(1); i <= 4; i++ {
					putC <- value.Long(i)
				}
				close(putC)

				select {
				case <-done:
				case <-time.After(3 * time.Second):
					t.Fatal("server never observed end of client stream")
				}
				mu.Lock()
				defer mu.Unlock()
				if sum != 10 {
					t.Fatalf("server summed %d, want 10", sum)
				}
			})

			t.Run("chat", func(t *testing.T) {
				srv, cli := tr.start(t, func(s valueserver.Server) {
					s.AddChat("echo", valuerpc.Any,
						func(_ context.Context, args value.Value, inC <-chan value.Value) (<-chan value.Value, error) {
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
				if err := cli.Connect(); err != nil {
					t.Fatalf("connect: %v", err)
				}
				defer cli.Close()
				cli.SetTimeout(5000)

				sendC := make(chan value.Value, 3)
				readC, _, err := cli.Chat("echo", nil, 16, sendC)
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
					if want := "c:" + s; got[i] != want {
						t.Fatalf("echo[%d] = %q, want %q", i, got[i], want)
					}
				}
			})
		})
	}
}
