package sqs

import (
	"context"
	"io"
	"reflect"
	"strconv"
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
		chars           string
		dispatchcost    time.Duration
		counterBreaks   time.Duration
		shutdownWaiting time.Duration
		serverOptions   []ServerOption
	}
	tests := []struct {
		name string
		args args
		want map[string]int
	}{
		{
			name: "fast",
			args: args{
				chars:           "aaabbbbccddddeeffccccddaassaassw",
				dispatchcost:    time.Millisecond,
				counterBreaks:   time.Millisecond,
				shutdownWaiting: time.Second,
			},
			want: map[string]int{"a": 7, "b": 4, "c": 6, "d": 6, "e": 2, "f": 2, "s": 4, "w": 1},
		},
		{
			name: "pull message slowly",
			args: args{
				chars:           "aabbccc",
				dispatchcost:    time.Second,
				counterBreaks:   time.Microsecond,
				shutdownWaiting: time.Millisecond,
				serverOptions: []ServerOption{
					SQSReceiveMessageInput(func(queueUrl *string) *sqs.ReceiveMessageInput {
						return &sqs.ReceiveMessageInput{
							QueueUrl: queueUrl, MaxNumberOfMessages: 1}
					}),
				},
			},
			want: map[string]int{},
		},
		{
			name: "Receive message slowly",
			args: args{
				chars:           "aa",
				dispatchcost:    time.Microsecond,
				counterBreaks:   time.Second,
				shutdownWaiting: time.Millisecond,
				serverOptions: []ServerOption{
					MessageBacklogSize(100),
					SQSReceiveMessageInput(func(queueUrl *string) *sqs.ReceiveMessageInput {
						return &sqs.ReceiveMessageInput{
							QueueUrl: queueUrl, MaxNumberOfMessages: 10}
					}),
				},
			},
			want: map[string]int{"a": 2},
		},
		{
			name: "slow",
			args: args{
				chars:           "aabbccc",
				dispatchcost:    time.Millisecond,
				counterBreaks:   time.Millisecond,
				shutdownWaiting: 5 * time.Millisecond,
			},
			want: map[string]int{"a": 2, "b": 2, "c": 3},
		},
		{
			name: "test options",
			args: args{
				chars:           "aaabbbbccddddeeffccccddaassaassw",
				dispatchcost:    time.Millisecond,
				counterBreaks:   time.Millisecond,
				shutdownWaiting: time.Second,
				serverOptions: []ServerOption{
					SQSReceiveMessageInput(func(queueUrl *string) *sqs.ReceiveMessageInput {
						return &sqs.ReceiveMessageInput{
							QueueUrl:            queueUrl,
							MaxNumberOfMessages: 8}
					}),
				},
			},
			want: map[string]int{"a": 7, "b": 4, "c": 6, "d": 6, "e": 2, "f": 2, "s": 4, "w": 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			appCtx := context.Background()

			options := []ServerOption{
				Context(appCtx),
				PoolSize(1),
				MessageBacklogSize(3),
				Timeout(3 * time.Second),
			}

			options = append(options, tt.args.serverOptions...)

			server, err := NewServer(
				"hello",
				newRuneDispatcher("world", tt.args.chars, tt.args.dispatchcost),
				options...,
			)
			if err != nil {
				t.Fatal(err)
			}

			counter := newRuneCounter(tt.args.counterBreaks)

			go func() {
				if err := server.Serve(counter); err != nil {
					t.Logf("server.Serve() return %+v", err)
				}
			}()

			time.Sleep(tt.args.shutdownWaiting)

			server.Shutdown(appCtx)

			result := counter.result()

			if !reflect.DeepEqual(result, tt.want) {
				t.Errorf("counter result = %v, want %v", result, tt.want)
			}
		})
	}
}

type runeDispatcher struct {
	fakeQueueUrl string
	chars        string
	charch       chan types.Message
	ticker       *time.Ticker
	deletedIndex []struct{}
	mu           *sync.Mutex
}

// newRuneDispatch dispatch the item of chars
// `cost` is time cost for calling d.ReceiveMessage
// `fakeQueueUrl` == d.GetQueueUrl().QueueUrl
func newRuneDispatcher(fakeQueueUrl string, chars string, cost time.Duration) *runeDispatcher {
	charch := make(chan types.Message, len(chars))
	deletedChar := make([]struct{}, len(chars))
	for i, r := range chars {
		charch <- types.Message{
			MessageId: aws.String(strconv.Itoa(i)),
			MessageAttributes: map[string]types.MessageAttributeValue{
				"from": {
					DataType:         aws.String("String"),
					BinaryListValues: [][]byte{},
					BinaryValue:      []byte{},
					StringListValues: []string{},
					StringValue:      aws.String("rune dispatch"),
				},
			},
			Body:          aws.String(string(r)),
			ReceiptHandle: aws.String(strconv.Itoa(i)),
		}
	}
	return &runeDispatcher{
		fakeQueueUrl: fakeQueueUrl,
		chars:        chars,
		charch:       charch,
		ticker:       time.NewTicker(cost),
		deletedIndex: deletedChar,
		mu:           &sync.Mutex{},
	}
}

// GetQueueUrl get runeDispatch.title
func (d *runeDispatcher) GetQueueUrl(
	ctx context.Context, params *sqs.GetQueueUrlInput, optFns ...func(*sqs.Options)) (
	*sqs.GetQueueUrlOutput, error) {
	return &sqs.GetQueueUrlOutput{
		QueueUrl: aws.String(d.fakeQueueUrl),
	}, nil
}

// ReceiveMessage get rune
func (d *runeDispatcher) ReceiveMessage(
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

	return &sqs.ReceiveMessageOutput{
		Messages: messages,
	}, nil
}

// DeleteMessage records index deleted
func (c *runeDispatcher) DeleteMessage(
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

type runeCounter struct {
	breaks time.Duration
	in     map[string]int
	mu     *sync.Mutex
}

// newRuneCounter return *runeCounter
// runeCounter counter the number rune appeared from chars
func newRuneCounter(breaks time.Duration) *runeCounter {
	return &runeCounter{
		breaks: breaks,
		in:     make(map[string]int),
		mu:     &sync.Mutex{},
	}
}

func (c *runeCounter) Receive(ctx context.Context, message *msg.Message) error {

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

	time.Sleep(c.breaks)

	return nil
}

func (c *runeCounter) result() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make(map[string]int)
	for k, v := range c.in {
		result[k] = v
	}
	return result
}
