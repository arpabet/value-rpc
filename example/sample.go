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
	"arpabet.pkg.is/value"
	"arpabet.pkg.is/value-rpc/valueclient"
	"arpabet.pkg.is/value-rpc/valuerpc"
	"arpabet.pkg.is/value-rpc/valueserver"
	"github.com/pkg/errors"
	"os"
	"sync"
	"time"
)

/**
@author Alex Shvid
*/

var testAddress = "localhost:9999"

var firstName = ""
var lastName = ""

func setName(args value.Value) (value.Value, error) {

	listArgs := args.(value.List)
	firstName = listArgs.GetStringAt(0).String()
	lastName = listArgs.GetStringAt(1).String()

	return nil, nil
}

func getName(args value.Value) (value.Value, error) {
	return value.Utf8(firstName + " " + lastName), nil
}

func scanNames(args value.Value) (<-chan value.Value, error) {

	outC := make(chan value.Value, 2)

	go func() {
		fmt.Println("Scan server: <START>")
		fmt.Println("Scan server: Alex")
		outC <- value.Utf8("Alex")
		fmt.Println("Scan server: Bob")
		outC <- value.Utf8("Bob")
		close(outC)
		fmt.Println("Scan server: <END>")
	}()

	return outC, nil
}

func uploadNames(args value.Value, inC <-chan value.Value) error {

	go func() {

		fmt.Println("Upload server: <START>")
		for {
			name, ok := <-inC
			if !ok {
				fmt.Println("Upload server: <END>")
				break
			}
			fmt.Printf("Upload server: %s\n", name.String())
		}

	}()

	return nil
}

func reverse(s string) string {
	runes := []rune(s)
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}
	return string(runes)
}

func echoChat(args value.Value, inC <-chan value.Value) (<-chan value.Value, error) {

	outC := make(chan value.Value, 20)

	go func() {
		fmt.Println("Chat server: <START>")
		for {
			msg, ok := <-inC
			if !ok {
				close(outC)
				fmt.Println("Chat server: <END>")
				break
			}
			utterance := msg.String()
			answer := value.Utf8(reverse(utterance))
			fmt.Printf("Chat server echo: %s -> %s\n", utterance, answer.String())
			outC <- answer
		}

	}()

	return outC, nil
}

func run() error {

	srv, err := valueserver.NewDevelopmentServer(testAddress)
	if err != nil {
		return err
	}
	defer srv.Close()

	srv.AddFunction("setName",
		valuerpc.List(valuerpc.String, valuerpc.String),
		valuerpc.Void, setName)

	srv.AddFunction("getName", valuerpc.Void, valuerpc.String, getName)
	srv.AddOutgoingStream("scanNames", valuerpc.Void, scanNames)
	srv.AddIncomingStream("uploadNames", valuerpc.Void, uploadNames)
	srv.AddChat("echoChat", valuerpc.Void, echoChat)

	go srv.Run()

	var wg sync.WaitGroup

	cli := valueclient.NewClient(testAddress, "")
	err = cli.Connect()
	if err != nil {
		return err
	}

	/**
	Simple call example
	*/

	nothing, err := cli.CallFunction("setName", value.Tuple(
		value.Utf8("Alex"),
		value.Utf8("Shvid"),
	))

	if nothing != nil || err != nil {
		return errors.Errorf("something wrong, %v", err)
	}

	/**
	Simple call example with timeout
	*/

	cli.SetTimeout(0)
	name, err := cli.CallFunction("getName", nil)
	if err == valueclient.ErrTimeoutError {
		fmt.Println("TImeout received")
	} else {
		fmt.Println(name)
	}
	cli.SetTimeout(1000)

	/**
	Get stream example
	*/

	readC, requestId, err := cli.GetStream("scanNames", nil, 100)
	if err != nil {
		return errors.Errorf("get stream failed, %v", err)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		fmt.Printf("Scan client: <START> %d\n", requestId)
		for {
			name, ok := <-readC
			if !ok {
				fmt.Println("Scan client: <END>")
				break
			}
			fmt.Println("Scan client: " + name.String())
		}

	}()

	/**
	Put stream example
	*/

	uploadCh := make(chan value.Value, 2)
	err = cli.PutStream("uploadNames", nil, uploadCh)
	if err != nil {
		return errors.Errorf("put stream failed, %v", err)
	}

	fmt.Println("Upload client: <START>")
	fmt.Println("Upload client: Bob")
	uploadCh <- value.Utf8("Bob")

	fmt.Println("Upload client: Marley")
	uploadCh <- value.Utf8("Marley")

	close(uploadCh)
	fmt.Println("Upload client <END>")

	/**
	Chat example
	*/

	sendCh := make(chan value.Value, 10)
	readC, requestId, err = cli.Chat("echoChat", nil, 100, sendCh)
	if err != nil {
		return errors.Errorf("chat request failed, %v", err)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		fmt.Printf("Chat client response: <START> %d\n", requestId)
		for {
			msg, ok := <-readC
			if !ok {
				fmt.Println("Chat client response: <END>")
				break
			}
			fmt.Println("Chat client response: " + msg.String())
		}
	}()

	fmt.Println("Chat client send: <START>")
	fmt.Println("Chat client send: Hi")
	sendCh <- value.Utf8("Hi")
	fmt.Println("Chat client send: How do you do?")
	sendCh <- value.Utf8("How do you do?")
	fmt.Println("Chat client send: Bye")
	sendCh <- value.Utf8("Bye")
	close(sendCh)
	fmt.Println("Chat client send: <END>")

	wg.Wait()
	fmt.Println("Client <END>")

	// wait while server free session and see logs
	time.Sleep(time.Second)

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
