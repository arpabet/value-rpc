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

package main

import (
	"errors"
	"fmt"
	"github.com/consensusdb/value"
	"github.com/consensusdb/value-rpc/client"
	"github.com/consensusdb/value-rpc/server"
	"os"
	"time"
)

var testAddress = "localhost:9999"

var firstName = ""
var lastName = ""

func setName(args []value.Value) (value.Value, error) {
	if args[0] != nil {
		firstName = args[0].String()
	}
	if args[1] != nil {
		lastName = args[1].String()
	}
	return nil, nil
}

func getName(args []value.Value) (value.Value, error) {
	return value.Utf8(firstName + " " + lastName), nil
}

func run() error {

	srv, err := server.NewDevelopmentServer(testAddress)
	if err != nil {
		return err
	}
	defer srv.Close()

	srv.AddFunction("setName", 2, setName)
	srv.AddFunction("getName", 0, getName)

	go srv.Run()

	cli := client.NewClient(testAddress, "")
	err = cli.Connect()
	if err != nil {
		return err
	}

	nothing, err := cli.CallFunction("setName", []value.Value{
		value.Utf8("Alex"),
		value.Utf8("Shvid"),
	}, time.Second)

	if nothing != nil || err != nil {
		return errors.New("something wrong")
	}

	name, err := cli.CallFunction("getName", nil, time.Second)
	fmt.Println(name)

	return nil
}

func doMain() int {
	if err := run(); err != nil {
		fmt.Printf("Error on run(), %v\n", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(doMain())
}
