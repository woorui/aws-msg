package sqsmsg

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/zerofox-oss/go-msg"
)

var (
	defaultTimeout   = 5 * time.Second
	defaultRetriever = uint32(runtime.NumCPU() * 3)
)

type serverOption struct {
	ctx                    context.Context
	timeout                time.Duration
	retriever              uint32
	poolSize               uint32
	decorators             []func(msg.ReceiverFunc) msg.ReceiverFunc
	errHandler             func(ctx context.Context, err error) error
	sqsReceiveMessageInput func(queueUrl *string) *sqs.ReceiveMessageInput
}

func defaultServerOptions() *serverOption {
	return &serverOption{
		ctx:                    context.Background(),
		timeout:                defaultTimeout,
		retriever:              defaultRetriever,
		poolSize:               defaultRetriever * 10,
		decorators:             []func(msg.ReceiverFunc) msg.ReceiverFunc{},
		sqsReceiveMessageInput: defalutReceiveMessageInput,
		errHandler: func(ctx context.Context, err error) error {
			if err != nil {
				fmt.Printf("aws-msg-sqs: %+v \n", err)
			}
			return nil
		},
	}
}

type ServerOption func(o *serverOption)

// PoolSize controls the maximum number of aws message handle-routine,
// one goroutinue handle one aws message.
func PoolSize(size uint32) ServerOption {
	return func(o *serverOption) {
		o.poolSize = size
	}
}

// Retriever controls the number of aws message receive-routine,
func Retriever(num uint32) ServerOption {
	return func(o *serverOption) {
		o.retriever = num
	}
}

// Decorators add Decorators for Receiver.Receive
func Decorators(ds ...func(msg.ReceiverFunc) msg.ReceiverFunc) ServerOption {
	return func(o *serverOption) {
		o.decorators = ds
	}
}

// ErrHandler handle server error, includes aws errors and receive errors,
// There means an unexpected error If ErrHandler returns not nil error, the server will shutdown.
func ErrHandler(handler func(ctx context.Context, err error) error) ServerOption {
	return func(o *serverOption) {
		o.errHandler = handler
	}
}

// Context inject context for server

// TODO: merge userCtx and appCtx
func Context(ctx context.Context) ServerOption {
	return func(o *serverOption) {
		o.ctx = ctx
	}
}

// Timeout controls timeout for receiving message
func Timeout(timeout time.Duration) ServerOption {
	return func(o *serverOption) {
		o.timeout = timeout
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
	return func(o *serverOption) {
		o.sqsReceiveMessageInput = fn
	}
}

func defalutReceiveMessageInput(queueUrl *string) *sqs.ReceiveMessageInput {
	return &sqs.ReceiveMessageInput{
		QueueUrl:              queueUrl,
		AttributeNames:        []types.QueueAttributeName{types.QueueAttributeNameAll},
		MaxNumberOfMessages:   10,
		MessageAttributeNames: []string{string(types.QueueAttributeNameAll)},
		WaitTimeSeconds:       20}
}
