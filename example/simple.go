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
	"fmt"
	"github.com/consensusdb/value"
	"github.com/consensusdb/value-rpc/client"
	"github.com/consensusdb/value-rpc/server"
	"github.com/pkg/errors"
	"os"
	"time"
)

/**
@author Alex Shvid
*/

var testAddress = "localhost:9999"

var firstName = ""
var lastName = ""

func setName(args value.List) (value.Value, error) {
	if args.GetAt(0) != nil {
		firstName = args.GetAt(0).String()
	}
	if args.GetAt(1) != nil {
		lastName = args.GetAt(1).String()
	}
	return nil, nil
}

func getName(args value.List) (value.Value, error) {
	return value.Utf8(firstName + " " + lastName), nil
}

func scanNames(args value.List) (<-chan value.Value, error) {

	if args.Len() != 0 {
		return nil, errors.New("wrong args")
	}

	outC := make(chan value.Value, 2)

	go func() {
		time.Sleep(time.Second)
		outC <- value.Utf8("scan:" + firstName)
		outC <- value.Utf8("scan:" + lastName)
		close(outC)
	}()

	return outC, nil
}

func uploadNames(args value.List, inC <-chan value.Value) error {

	if args.Len() != 0 {
		return errors.New("wrong args")
	}

	go func() {

		for {
			name, ok := <-inC
			if !ok {
				fmt.Println("Uploaded END")
				break
			}
			fmt.Printf("Uploaded name: %s\n", name.String())
		}

	}()

	return nil
}

func echoChat(args value.List, inC <-chan value.Value) (<-chan value.Value, error) {

	outC := make(chan value.Value, 20)

	go func() {

		for {
			msg, ok := <-inC
			if !ok {
				fmt.Println("Chat END")
				break
			}
			outC <- msg
		}

	}()

	return outC, nil
}

func run() error {

	srv, err := server.NewDevelopmentServer(testAddress)
	if err != nil {
		return err
	}
	defer srv.Close()

	srv.AddFunction("setName", 2, setName)
	srv.AddFunction("getName", 0, getName)
	srv.AddOutgoingStream("scanNames", 0, scanNames)
	srv.AddIncomingStream("uploadNames", 0, uploadNames)
	srv.AddChat("echoChat", 0, echoChat)

	go srv.Run()

	cli := client.NewClient(testAddress, "")
	err = cli.Connect()
	if err != nil {
		return err
	}

	nothing, err := cli.CallFunction("setName", value.Tuple(
		value.Utf8("Alex"),
		value.Utf8("Shvid"),
	))

	if nothing != nil || err != nil {
		return errors.Errorf("something wrong, %v", err)
	}

	name, err := cli.CallFunction("getName", nil)
	fmt.Println(name)

	readC, requestId, err := cli.GetStream("scanNames", nil, 100)
	if err == nil {
		fmt.Printf("Scan requestId=%d\n", requestId)
		for {
			name, ok := <-readC
			if !ok {
				fmt.Println("Received END")
				break
			}
			fmt.Println("Received name: " + name.String())
		}
	} else {
		return errors.Errorf("get stream failed, %v", err)
	}

	uploadCh := make(chan value.Value, 2)
	err = cli.PutStream("uploadNames", nil, uploadCh)
	if err != nil {
		return errors.Errorf("put stream failed, %v", err)
	}

	uploadCh <- value.Utf8("Bob")
	uploadCh <- value.Utf8("Marley")
	close(uploadCh)

	sendCh := make(chan value.Value, 10)
	readC, requestId, err = cli.Chat("echoChat", nil, 100, sendCh)
	if err != nil {
		return errors.Errorf("chat request failed, %v", err)
	}
	fmt.Printf("Chat requestId=%d\n", requestId)

	go func() {
		for {
			msg, ok := <-readC
			if !ok {
				fmt.Println("Chat END")
				break
			}
			fmt.Println("Chat response: " + msg.String())
		}
	}()

	sendCh <- value.Utf8("Hi")
	sendCh <- value.Utf8("This is chat!")
	sendCh <- value.Utf8("Bye")
	close(sendCh)

	fmt.Println("Client END")

	time.Sleep(time.Second * 5)

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
