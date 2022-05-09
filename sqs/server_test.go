package sqsmsg

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/zerofox-oss/go-msg"
)

func Test_Server(t *testing.T) {
	type args struct {
		chars              string
		pullMessageDuraion time.Duration
		msgReceiveDuration time.Duration
		shutdownWaiting    time.Duration
		serverOptions      []ServerOption
	}
	tests := []struct {
		name   string
		args   args
		want   map[string]int
		equals bool
	}{
		{
			name: "fast",
			args: args{
				chars:              "aaabbbbccddddeeffccccddaassaassw",
				pullMessageDuraion: time.Millisecond,
				msgReceiveDuration: time.Millisecond,
				shutdownWaiting:    time.Second,
			},
			want:   map[string]int{"a": 7, "b": 4, "c": 6, "d": 6, "e": 2, "f": 2, "s": 4, "w": 1},
			equals: true,
		},
		{
			name: "pull message slowly",
			args: args{
				chars:              "aabbccc",
				pullMessageDuraion: time.Second,
				msgReceiveDuration: time.Microsecond,
				shutdownWaiting:    time.Millisecond,
				serverOptions: []ServerOption{
					SQSReceiveMessageInput(func(queueUrl *string) *sqs.ReceiveMessageInput {
						return &sqs.ReceiveMessageInput{
							QueueUrl: queueUrl, MaxNumberOfMessages: 1}
					}),
					ErrHandler(func(ctx context.Context, err error) error {
						return nil
					}),
				},
			},
			want:   map[string]int{},
			equals: true,
		},
		{
			name: "Receive message slowly",
			args: args{
				chars:              "aa",
				pullMessageDuraion: time.Microsecond,
				msgReceiveDuration: time.Second,
				shutdownWaiting:    time.Millisecond,
				serverOptions: []ServerOption{
					PoolSize(100),
					SQSReceiveMessageInput(func(queueUrl *string) *sqs.ReceiveMessageInput {
						return &sqs.ReceiveMessageInput{
							QueueUrl: queueUrl, MaxNumberOfMessages: 10}
					}),
				},
			},
			want:   map[string]int{"a": 2},
			equals: true,
		},
		{
			name: "slow",
			args: args{
				chars:              "aabbccc",
				pullMessageDuraion: time.Millisecond,
				msgReceiveDuration: time.Millisecond,
				shutdownWaiting:    5 * time.Millisecond,
			},
			want:   map[string]int{"a": 2, "b": 2, "c": 3},
			equals: true,
		},
		{
			name: "large data",
			args: args{
				chars:              strings.Repeat("ab", 8120),
				pullMessageDuraion: time.Microsecond,
				msgReceiveDuration: time.Microsecond,
				shutdownWaiting:    300 * time.Microsecond,
			},
			want:   map[string]int{},
			equals: false,
		},
		{
			name: "test options",
			args: args{
				chars:              "aaabbbbccddddeeffccccddaassaassw",
				pullMessageDuraion: time.Millisecond,
				msgReceiveDuration: time.Millisecond,
				shutdownWaiting:    time.Second,
				serverOptions: []ServerOption{
					SQSReceiveMessageInput(func(queueUrl *string) *sqs.ReceiveMessageInput {
						return &sqs.ReceiveMessageInput{
							QueueUrl:            queueUrl,
							MaxNumberOfMessages: 8}
					}),
					Decorators(func(rf msg.ReceiverFunc) msg.ReceiverFunc {
						return func(ctx context.Context, m *msg.Message) error {
							return rf(ctx, m)
						}
					}),
				},
			},
			want:   map[string]int{"a": 7, "b": 4, "c": 6, "d": 6, "e": 2, "f": 2, "s": 4, "w": 1},
			equals: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			appCtx := context.Background()

			options := []ServerOption{
				Context(appCtx),
				Retriever(1),
				PoolSize(3),
				Timeout(3 * time.Second),
			}

			options = append(options, tt.args.serverOptions...)

			server, err := NewServer(
				"hello",
				newMockSQSClient("world", tt.args.chars, tt.args.pullMessageDuraion),
				options...,
			)
			if err != nil {
				t.Fatal(err)
			}

			counter := newMockReceiver(tt.args.msgReceiveDuration)

			go func() {
				if err := server.Serve(counter); err != nil {
					fmt.Printf("server.Serve() return %+v \n", err)
				}
			}()

			time.Sleep(tt.args.shutdownWaiting)

			server.Shutdown(appCtx)
			server.Shutdown(appCtx)

			result := counter.result()

			if tt.equals && !reflect.DeepEqual(result, tt.want) {
				t.Errorf("counter result = %v, want %v", result, tt.want)
			}
		})
	}
}

type mockSQSClient struct {
	fakeQueueUrl string
	chars        string
	charch       chan types.Message
	ticker       *time.Ticker
	deletedIndex []struct{}
	mu           *sync.Mutex
}

// newMockSQSClient stores a chars and returns a specified number of
// item when calling the method until all of them are returned
//
// `receiveCost` is time cost for calling d.ReceiveMessage
// `fakeQueueUrl` will equals d.GetQueueUrl().QueueUrl
func newMockSQSClient(fakeQueueUrl string, chars string, receiveCost time.Duration) *mockSQSClient {
	charch := make(chan types.Message, len(chars))
	deletedChar := make([]struct{}, len(chars))
	for i, r := range chars {
		charch <- types.Message{
			MessageId: aws.String(strconv.Itoa(i)),
			MessageAttributes: map[string]types.MessageAttributeValue{
				"from": {
					DataType:    aws.String("String"),
					StringValue: aws.String("mock_sqs_client"),
				},
			},
			Body:          aws.String(string(r)),
			ReceiptHandle: aws.String(strconv.Itoa(i)),
		}
	}
	return &mockSQSClient{
		fakeQueueUrl: fakeQueueUrl,
		chars:        chars,
		charch:       charch,
		ticker:       time.NewTicker(receiveCost),
		deletedIndex: deletedChar,
		mu:           &sync.Mutex{},
	}
}

// GetQueueUrl get QueueUrl
func (d *mockSQSClient) GetQueueUrl(
	ctx context.Context, params *sqs.GetQueueUrlInput, optFns ...func(*sqs.Options)) (
	*sqs.GetQueueUrlOutput, error) {
	return &sqs.GetQueueUrlOutput{
		QueueUrl: aws.String(d.fakeQueueUrl),
	}, nil
}

// ReceiveMessage get `MaxNumberOfMessages` count item of chars
func (d *mockSQSClient) ReceiveMessage(
	ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (
	*sqs.ReceiveMessageOutput, error) {
	var messages []types.Message

	size := params.MaxNumberOfMessages

	<-d.ticker.C

	for i := 0; i < int(size); i++ {
		if len(d.charch) == 0 {
			break
		}
		msg := <-d.charch
		messages = append(messages, msg)
	}

	if len(messages) == 0 {
		return &sqs.ReceiveMessageOutput{}, nil
	}

	return &sqs.ReceiveMessageOutput{Messages: messages}, nil
}

// DeleteMessage records index deleted
func (c *mockSQSClient) DeleteMessage(
	ctx context.Context,
	params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (
	*sqs.DeleteMessageOutput, error) {

	c.mu.Lock()
	defer c.mu.Unlock()

	h := *params.ReceiptHandle

	i, err := strconv.Atoi(h)
	if err != nil {
		return &sqs.DeleteMessageOutput{}, err
	}

	c.deletedIndex[i] = struct{}{}

	return &sqs.DeleteMessageOutput{}, nil
}

// ChangeMessageVisibility only returns an success.
func (c *mockSQSClient) ChangeMessageVisibility(
	ctx context.Context,
	params *sqs.ChangeMessageVisibilityInput,
	optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	return &sqs.ChangeMessageVisibilityOutput{}, nil
}

type mockReceiver struct {
	mockTimeExecuted time.Duration
	in               map[string]int
	mu               *sync.Mutex
}

// newMockReceiver return  a mock receiver
// mockReceiver counts the number of item from calling mockSQSClient.ReceiveMessage()
func newMockReceiver(mockTimeExecuted time.Duration) *mockReceiver {
	return &mockReceiver{
		mockTimeExecuted: mockTimeExecuted,
		in:               make(map[string]int),
		mu:               &sync.Mutex{},
	}
}

func (c *mockReceiver) Receive(ctx context.Context, message *msg.Message) error {
	b, err := io.ReadAll(message.Body)
	if err != nil {
		return err
	}
	str := string(b)

	c.mu.Lock()
	{
		val, ok := c.in[str]
		if ok {
			val++
			c.in[str] = val
		} else {
			c.in[str] = 1
		}
	}
	c.mu.Unlock()

	MessageId(message)
	ReceiptHandle(message)

	time.Sleep(c.mockTimeExecuted)

	if len(c.result()) > 3 {
		return VisibilityTimeout(12 * time.Second)
	}
	return nil
}

// result make test result easy
func (c *mockReceiver) result() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make(map[string]int)
	for k, v := range c.in {
		result[k] = v
	}
	return result
}
