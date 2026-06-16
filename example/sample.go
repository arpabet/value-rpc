/*
 * Copyright (c) 2025-2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package main

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"go.arpabet.com/value"
	"go.arpabet.com/value-rpc/valueclient"
	"go.arpabet.com/value-rpc/valuerpc"
	"go.arpabet.com/value-rpc/valueserver"
	"os"
	"sync"
	"time"
)

var firstName = ""
var lastName = ""

func setName(_ context.Context, args value.Value) (value.Value, error) {

	listArgs := args.(value.List)
	firstName = listArgs.GetStringAt(0).String()
	lastName = listArgs.GetStringAt(1).String()

	return nil, nil
}

func getName(_ context.Context, args value.Value) (value.Value, error) {
	return value.Utf8(firstName + " " + lastName), nil
}

func scanNames(_ context.Context, args value.Value) (<-chan value.Value, error) {

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

func uploadNames(_ context.Context, args value.Value, inC <-chan value.Value) error {

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

func echoChat(_ context.Context, args value.Value, inC <-chan value.Value) (<-chan value.Value, error) {

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

// run is an end-to-end demo of all four interaction patterns: a unary call
// (setName/getName), a server stream (scanNames), a client stream (uploadNames),
// and a bidirectional chat (echoChat). The "Timeout received" line is expected —
// getName is called once with SetTimeout(0) on purpose to show the timeout path.
func run() error {

	// Bind an ephemeral port and read the actual address back via Addr().
	srv, err := valueserver.NewDevelopmentServer("127.0.0.1:0")
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

	cli := valueclient.NewClient(srv.Addr().String(), "")
	err = cli.Connect()
	if err != nil {
		return err
	}

	/**
	Simple call example
	*/

	nothing, err := cli.CallFunction("setName", value.Tuple(
		value.Utf8("Alex"),
		value.Utf8("Shu"),
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
		fmt.Println("Timeout received")
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
