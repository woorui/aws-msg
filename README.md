# aws-msg

![build](https://github.com/woorui/aws-msg/actions/workflows/go.yml/badge.svg)
[![codecov](https://codecov.io/gh/woorui/aws-msg/branch/main/graph/badge.svg?token=Y0030WHH14)](https://codecov.io/gh/woorui/aws-msg)

## Overview

Package aws-msg implements pub/sub primitives outlined in package "github.com/zerofox-oss/go-msg".

It's built on top of "github.com/aws/aws-sdk-go-v2".

## Why use

1. Elegant pub/sub API from https://github.com/zerofox-oss/go-msg

2. Support multiple goroutinue to pull messages from SQS; The receive speed is significantly improved.

3. Support graceful shutdown.

4. Backpressure for handling SQS message.
## Examples

```go
package main

import (
	"context"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqsmsg "github.com/woorui/aws-msg/sqs"
	"github.com/zerofox-oss/go-msg"
)

var (
	region    = "cn-northwest-1"
	key       = "QNWIOCMEROCVSL"
	secret    = "aswekdpa[veverjiwAmioeoqxkaij"
	queueName = "the-queue-name"
)
var client *sqs.Client

func init() {
	cfg, err := config.LoadDefaultConfig(
		context.TODO(),
		config.WithRegion(region),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(key, secret, ""),
		),
	)

	if err != nil {
		log.Fatal(err)
	}
	client = sqs.NewFromConfig(cfg)
}

// ExampleServer dumps SQS message to ./messages.log for 5 seconds
func ExampleServer() {
	server, err := sqsmsg.NewServer(queueName, client)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		err = server.Serve(dumpToPath("./messages.log"))
		if err != nil {
			log.Fatal(err)
		}
	}()

	time.Sleep(5 * time.Second)

	server.Shutdown(context.TODO())
}

// ExampleTopic show how to send an SQS message
func ExampleTopic() {
	ctx := context.Background()

	topic, err := sqsmsg.NewTopic(ctx, queueName, client)
	if err != nil {
		log.Fatal(err)
	}

	userCtx := context.Background()

	w := topic.NewWriter(userCtx)

	if _, err = w.Write([]byte("hello world")); err != nil {
		log.Fatal(err)
	}
	if err := w.Close(); err != nil {
		log.Fatal(err)
	}
}

// fileLog write sqs message to local file
// fileLog implement msg.Receiver
type fileLog struct {
	f *os.File
}

func dumpToPath(path string) *fileLog {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	return &fileLog{f: f}
}

func (f *fileLog) Receive(c context.Context, m *msg.Message) error {
	contents, _ := ioutil.ReadAll(m.Body)
	f.f.Write([]byte{'\n'})
	_, err := f.f.Write(contents)
	return err
}

```

## Reference

https://github.com/zerofox-oss/go-msg

https://github.com/aws/aws-sdk-go-v2

https://github.com/zerofox-oss/go-aws-msg
