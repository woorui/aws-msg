package sqs

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

var (
	defaultTimeout  = 5 * time.Second
	defaultPoolSize = uint32(runtime.NumCPU() * 3)
)

type serverOption struct {
	ctx                    context.Context
	timeout                time.Duration
	poolSize               uint32
	messageBacklogSize     uint32
	errHandler             func(ctx context.Context, err error) error
	sqsReceiveMessageInput func(queueUrl *string) *sqs.ReceiveMessageInput
}

func defaultServerOptions() *serverOption {
	return &serverOption{
		ctx:                context.Background(),
		timeout:            defaultTimeout,
		poolSize:           defaultPoolSize,
		messageBacklogSize: defaultPoolSize * 10,
		sqsReceiveMessageInput: func(queueUrl *string) *sqs.ReceiveMessageInput {
			return &sqs.ReceiveMessageInput{
				QueueUrl:              queueUrl,
				AttributeNames:        []types.QueueAttributeName{types.QueueAttributeNameAll},
				MaxNumberOfMessages:   10,
				MessageAttributeNames: []string{string(types.QueueAttributeNameAll)},
				WaitTimeSeconds:       20}
		},
		errHandler: func(ctx context.Context, err error) error {
			if err != nil {
				fmt.Printf("aws-msg-sqs: %+v \n", err)
			}
			return nil
		},
	}
}

type ServerOption func(o *serverOption) error

// PoolSize controls the maximum number of aws message receive routines allowed
func PoolSize(size uint32) ServerOption {
	return func(o *serverOption) error {
		o.poolSize = size
		return nil
	}
}

// MessageBacklogSize change message backlog size
func MessageBacklogSize(size uint32) ServerOption {
	return func(o *serverOption) error {
		o.messageBacklogSize = size
		return nil
	}
}

// ErrHandler handle server error, includes aws errors and receive errors
func ErrHandler(handler func(ctx context.Context, err error) error) ServerOption {
	return func(o *serverOption) error {
		o.errHandler = handler
		return nil
	}
}

// Context inject context for server

// TODO: merge userCtx and appCtx
func Context(ctx context.Context) ServerOption {
	return func(o *serverOption) error {
		o.ctx = ctx
		return nil
	}
}

// Timeout controls timeout for receiving message
func Timeout(timeout time.Duration) ServerOption {
	return func(o *serverOption) error {
		o.timeout = timeout
		return nil
	}
}

// SQSReceiveMessageInput allows people to customize *sqs.ReceiveMessageInput
//
// defaults is:
//
// func(queueUrl *string) *sqs.ReceiveMessageInput {
// 	return &sqs.ReceiveMessageInput{
// 		QueueUrl:              queueUrl,
// 		AttributeNames:        []types.QueueAttributeName{types.QueueAttributeNameAll},
// 		MaxNumberOfMessages:   10,
// 		MessageAttributeNames: []string{string(types.QueueAttributeNameAll)},
// 		WaitTimeSeconds:       20}
// }
func SQSReceiveMessageInput(fn func(queueUrl *string) *sqs.ReceiveMessageInput) ServerOption {
	return func(o *serverOption) error {
		o.sqsReceiveMessageInput = fn
		return nil
	}
}

type topicOption struct {
	sqsSendMessageInput func(body, queueUrl *string) *sqs.SendMessageInput
}

func defaultTopicOptions() *topicOption {
	return &topicOption{
		sqsSendMessageInput: func(body, queueUrl *string) *sqs.SendMessageInput {
			return &sqs.SendMessageInput{
				MessageBody:  body,
				QueueUrl:     queueUrl,
				DelaySeconds: 0,
			}
		},
	}
}

type TopicOption func(o *topicOption) error

// SQSSendMessageInput allows people to customize *sqs.SendMessageInput.
//
// defaults is:
//
// func(body, queueUrl *string) *sqs.SendMessageInput {
//	return &sqs.SendMessageInput{
//		MessageBody:  body,
//		QueueUrl:     queueUrl,
//		DelaySeconds: 0,
// 	}
func SQSSendMessageInput(fn func(body, queueUrl *string) *sqs.SendMessageInput) TopicOption {
	return func(o *topicOption) error {
		o.sqsSendMessageInput = fn
		return nil
	}
}
