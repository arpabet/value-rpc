/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package valueclient_test

import (
	"context"
	"fmt"
	"testing"

	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valueclient"
	"go.arpabet.com/value-rpc/valuerpc"
	"go.arpabet.com/value-rpc/valueserver"
)

// --- a typed domain over the schemaless wire ---------------------------------
//
// vRPC is schemaless: handlers take/return value.Value. To get static Go types
// at call sites you define a valuerpc.Codec per message — the explicit, codegen-
// free mapping between your struct and a value.Map — then use the generic
// CallUnary / AddUnary (and the streaming variants) helpers.

type GetUserReq struct {
	ID int64
}

type User struct {
	ID    int64
	Name  string
	Email string
}

var getUserReqCodec = valuerpc.Codec[GetUserReq]{
	Encode: func(r GetUserReq) value.Value {
		return value.EmptyMap(true).Put("id", value.Long(r.ID))
	},
	Decode: func(v value.Value) (GetUserReq, error) {
		m, ok := v.(value.Map)
		if !ok {
			return GetUserReq{}, fmt.Errorf("expected a map")
		}
		return GetUserReq{ID: m.GetNumber("id").Long()}, nil
	},
}

var userCodec = valuerpc.Codec[User]{
	Encode: func(u User) value.Value {
		return value.EmptyMap(true).
			Put("id", value.Long(u.ID)).
			Put("name", value.Utf8(u.Name)).
			Put("email", value.Utf8(u.Email))
	},
	Decode: func(v value.Value) (User, error) {
		m, ok := v.(value.Map)
		if !ok {
			return User{}, fmt.Errorf("expected a map")
		}
		return User{
			ID:    m.GetNumber("id").Long(),
			Name:  m.GetString("name").String(),
			Email: m.GetString("email").String(),
		}, nil
	},
}

// UsersClient is a hand-written typed facade over the dynamic client — the
// recommended pattern for presenting a clean API to callers.
type UsersClient struct{ cli valueclient.Client }

func (c UsersClient) GetUser(ctx context.Context, id int64) (User, error) {
	return valueclient.CallUnary(ctx, c.cli, "users.get",
		GetUserReq{ID: id}, getUserReqCodec, userCodec)
}

func (c UsersClient) ListUsers(ctx context.Context, errp *error) (<-chan User, error) {
	out, _, err := valueclient.GetStreamTyped(ctx, c.cli, "users.list",
		GetUserReq{}, 16, getUserReqCodec, userCodec, errp)
	return out, err
}

func TestTypedService(t *testing.T) {
	db := map[int64]User{
		1: {ID: 1, Name: "Ada", Email: "ada@x.io"},
		2: {ID: 2, Name: "Linus", Email: "linus@x.io"},
	}

	sock, stop := serve(t, func(s valueserver.Server) {
		// Typed unary handler.
		if err := valueserver.AddUnary(s, "users.get", getUserReqCodec, userCodec,
			func(_ context.Context, req GetUserReq) (User, error) {
				u, ok := db[req.ID]
				if !ok {
					return User{}, valuerpc.NewError(valuerpc.CodeNotFound, "no user %d", req.ID)
				}
				return u, nil
			}); err != nil {
			t.Fatalf("AddUnary: %v", err)
		}
		// Typed server-stream handler.
		if err := valueserver.AddOutgoingStreamTyped(s, "users.list", getUserReqCodec, userCodec,
			func(_ context.Context, _ GetUserReq) (<-chan User, error) {
				out := make(chan User)
				go func() {
					defer close(out)
					for _, u := range []User{db[1], db[2]} {
						out <- u
					}
				}()
				return out, nil
			}); err != nil {
			t.Fatalf("AddOutgoingStreamTyped: %v", err)
		}
	})
	defer stop()

	uc := UsersClient{cli: dial(t, sock)}
	defer uc.cli.Close()
	ctx := context.Background()

	// Typed unary call.
	u, err := uc.GetUser(ctx, 1)
	if err != nil {
		t.Fatalf("GetUser: %v", err)
	}
	if u.Name != "Ada" || u.Email != "ada@x.io" {
		t.Fatalf("GetUser = %+v", u)
	}

	// Typed error still carries the code.
	if _, err := uc.GetUser(ctx, 99); valuerpc.CodeOf(err) != valuerpc.CodeNotFound {
		t.Fatalf("missing user: code = %v, want NotFound", valuerpc.CodeOf(err))
	}

	// Typed server stream.
	var streamErr error
	users, err := uc.ListUsers(ctx, &streamErr)
	if err != nil {
		t.Fatalf("ListUsers: %v", err)
	}
	var names []string
	for u := range users {
		names = append(names, u.Name)
	}
	if streamErr != nil {
		t.Fatalf("stream decode: %v", streamErr)
	}
	if len(names) != 2 || names[0] != "Ada" || names[1] != "Linus" {
		t.Fatalf("ListUsers names = %v", names)
	}
}
