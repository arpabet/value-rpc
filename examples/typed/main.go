/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

// Command typed demonstrates statically-typed call sites over the schemaless wire
// using valuerpc.Codec[T] plus the generic CallUnary / AddUnary helpers.
package main

import (
	"context"
	"fmt"
	"log"

	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valueclient"
	"go.arpabet.com/value-rpc/valuerpc"
	"go.arpabet.com/value-rpc/valueserver"
	"go.uber.org/zap"
)

type GetUserReq struct{ ID int64 }

type User struct {
	ID    int64
	Name  string
	Email string
}

// One Codec per message type maps the struct to/from a value.Map. This is the
// explicit, codegen-free bridge between Go types and the dynamic wire.
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

func main() {
	db := map[int64]User{
		1: {ID: 1, Name: "Ada", Email: "ada@x.io"},
		2: {ID: 2, Name: "Linus", Email: "linus@x.io"},
	}

	srv, err := valueserver.NewMemServer("typed-demo", zap.NewNop())
	if err != nil {
		log.Fatal(err)
	}
	defer srv.Close()

	must(valueserver.AddUnary(srv, "users.get", getUserReqCodec, userCodec,
		func(_ context.Context, r GetUserReq) (User, error) {
			u, ok := db[r.ID]
			if !ok {
				return User{}, valuerpc.NewError(valuerpc.CodeNotFound, "no user %d", r.ID)
			}
			return u, nil
		}))
	must(valueserver.AddOutgoingStreamTyped(srv, "users.list", getUserReqCodec, userCodec,
		func(_ context.Context, _ GetUserReq) (<-chan User, error) {
			out := make(chan User)
			go func() {
				defer close(out)
				out <- db[1]
				out <- db[2]
			}()
			return out, nil
		}))
	go srv.Run()

	cli := valueclient.NewMemClient("typed-demo")
	if err := cli.Connect(); err != nil {
		log.Fatal(err)
	}
	defer cli.Close()
	ctx := context.Background()

	// Typed unary call — req and resp are Go structs.
	u, err := valueclient.CallUnary(ctx, cli, "users.get", GetUserReq{ID: 1}, getUserReqCodec, userCodec)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("GetUser(1) = %+v\n", u)

	// Typed error still carries the machine-readable code.
	_, err = valueclient.CallUnary(ctx, cli, "users.get", GetUserReq{ID: 99}, getUserReqCodec, userCodec)
	fmt.Printf("GetUser(99) -> code=%v (%v)\n", valuerpc.CodeOf(err), err)

	// Typed server stream — a channel of decoded Users.
	var streamErr error
	users, _, err := valueclient.GetStreamTyped(ctx, cli, "users.list", GetUserReq{}, 8, getUserReqCodec, userCodec, &streamErr)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Print("ListUsers =")
	for u := range users {
		fmt.Printf(" %s", u.Name)
	}
	fmt.Println()
	if streamErr != nil {
		log.Fatalf("stream decode: %v", streamErr)
	}
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
