package sqsmsg

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/smithy-go/middleware"
)

func Test_Topic(t *testing.T) {
	type args struct {
		strs string
	}
	tests := []struct {
		name string
		args args
		want map[string]int
	}{
		{
			name: "word logistics",
			args: args{
				strs: "aabbccddddd",
			},
			want: map[string]int{"a": 2, "b": 2, "c": 2, "d": 5},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			appCtx := context.Background()

			client := newMockSender()

			topic, err := NewTopic(appCtx, "", client)

			if err != nil {
				t.Fatal(err)
			}

			for _, r := range tt.args.strs {
				userCtx := context.Background()

				w := topic.NewWriter(userCtx)

				SetDelay(w, time.Second)
				SetOptFns(w, sqs.WithAPIOptions(func(s *middleware.Stack) error { return nil }))

				w.Attributes().Set("a", "bbb")
				if _, err = w.Write([]byte(string(r))); err != nil {
					t.Fatal(err)
				}
				if err := w.Close(); err != nil {
					t.Fatal(err)
				}
			}

			result := client.result()

			if !reflect.DeepEqual(result, tt.want) {
				t.Errorf("counter result = %v, want %v", result, tt.want)
			}
		})
	}
}

type mockSender struct {
	in map[string]int
	mu *sync.Mutex
}

func newMockSender() *mockSender {
	return &mockSender{
		in: make(map[string]int),
		mu: &sync.Mutex{},
	}
}
func (s *mockSender) GetQueueUrl(
	ctx context.Context,
	params *sqs.GetQueueUrlInput, optFns ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
	return &sqs.GetQueueUrlOutput{
		QueueUrl: aws.String("http://mock.aws.sqs.com"),
	}, nil
}

func (s *mockSender) SendMessage(
	ctx context.Context,
	params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {

	str := *params.MessageBody

	s.mu.Lock()
	{
		val, ok := s.in[str]
		if ok {
			val++
			s.in[str] = val
		} else {
			s.in[str] = 1
		}
	}
	s.mu.Unlock()

	return &sqs.SendMessageOutput{}, nil
}

func (s *mockSender) result() map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make(map[string]int)
	for k, v := range s.in {
		result[k] = v
	}
	return result
}
