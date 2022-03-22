package main

import (
	"context"
	"io/ioutil"
	"log"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqsmsg "github.com/woorui/aws-msg/sqs"
	"github.com/zerofox-oss/go-msg"
)

type FileStorage struct {
	f *os.File
}

func NewFileStorage(path string) *FileStorage {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	return &FileStorage{f: f}
}

func (f *FileStorage) Receive(c context.Context, m *msg.Message) error {
	contents, _ := ioutil.ReadAll(m.Body)
	f.f.Write([]byte{'\n'})
	_, err := f.f.Write(contents)
	return err
}

func Test_Example_Server(t *testing.T) {
	var (
		region    = "cn-northwest-1"
		key       = "QNWIOCMEROCVSL"
		secret    = "aswekdpa[veverjiwAmioeoqxkaij"
		queueName = "the-queue-name"
	)
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
	client := sqs.NewFromConfig(cfg)

	server, err := sqsmsg.NewServer(
		queueName,
		client,
		sqsmsg.SQSReceiveMessageInput(func(queueUrl *string) *sqs.ReceiveMessageInput {
			return &sqs.ReceiveMessageInput{
				QueueUrl:            queueUrl,
				MaxNumberOfMessages: 1,
				WaitTimeSeconds:     20,
			}
		}))
	if err != nil {
		log.Fatal(err)
	}

	err = server.Serve(NewFileStorage("./messages.log"))
	if err != nil {
		log.Fatal(err)
	}
}
