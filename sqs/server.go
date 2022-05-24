package sqsmsg

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/zerofox-oss/go-msg"
)

const changeVisibilityTimeout = "changeVisibilityTimeout"

// ServerClient holds some necessary methods of *sqs.Client for server
type ServerClient interface {
	GetQueueUrl(ctx context.Context, params *sqs.GetQueueUrlInput, optFns ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error)
	ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
	ChangeMessageVisibility(ctx context.Context, params *sqs.ChangeMessageVisibilityInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error)
}

// Server represents a msg.Server for retrieving messages and receiving messages
// from an AWS SQS Queue.
type Server struct {
	options       *serverOption
	appCtx        context.Context    // context used to control the lifecycle of the Server.
	appCancelFunc context.CancelFunc // CancelFunc to signal the server should stop requesting messages.
	QueueURL      *string
	client        ServerClient
	ReceiveFunc   msg.ReceiverFunc
}

// NewServer return a handling AWS SQS server
//
// NewServer should be used prior to running Serve.
func NewServer(queueName string, client ServerClient, op ...ServerOption) (msg.Server, error) {
	options := defaultServerOptions()
	for _, o := range op {
		if err := o(options); err != nil {
			return nil, err
		}
	}
	appCtx, appCancelFunc := context.WithCancel(options.ctx)

	urlOutput, err := client.GetQueueUrl(appCtx, &sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	})
	if err != nil {
		appCancelFunc()
		return nil, err
	}

	srv := &Server{
		options:       options,
		appCtx:        appCtx,
		appCancelFunc: appCancelFunc,
		QueueURL:      urlOutput.QueueUrl,
		client:        client,
	}

	return srv, nil
}

// Serve continuously receives messages from an AWS SQS Queue, and calls Receive on `r`.
//
// Serve is blocking and will not return until Shutdown is called or unknown error happened on the Server.
func (srv *Server) Serve(r msg.Receiver) error {
	srv.ReceiveFunc = msg.ReceiverFunc(func(ctx context.Context, m *msg.Message) error {
		next := r.Receive
		ds := srv.options.decorators
		for i := len(srv.options.decorators) - 1; i >= 0; i-- {
			next = ds[i](next)
		}
		return next(ctx, m)
	})

	var wg sync.WaitGroup

	messagech, errch := srv.Polling(&wg, srv.options.retriever)

	var gerr error

	for {
		select {
		case <-srv.appCtx.Done():
			goto waiting
		case err := <-errch:
			if se := srv.options.errHandler(srv.appCtx, err); se != nil {
				gerr = se
				goto waiting
			}
		case message := <-messagech:
			wg.Add(1)
			go func() {
				defer wg.Done()
				userCtx, cancel := context.WithTimeout(srv.appCtx, srv.options.timeout)
				defer cancel()
				if err := srv.handleMessage(userCtx, message); err != nil {
					if srv.appCtx != nil {
						return
					}
					errch <- err
				}
			}()
		}
	}

waiting:
	wg.Wait()
	close(messagech)
	close(errch)

	return gerr
}

// Polling calls retrieve using specified number of goroutinue.
func (srv *Server) Polling(wg *sync.WaitGroup, n uint32) (chan types.Message, chan error) {
	var (
		ctx       = srv.appCtx
		messagech = make(chan types.Message, srv.options.poolSize)
		errch     = make(chan error)
		num       = int(n)
	)
	wg.Add(num)
	for i := 0; i < num; i++ {
		go srv.retrieve(wg, ctx, messagech, errch)
	}

	return messagech, errch
}

// retrieve calls sqs.Client.ReceiveMessage to retrieve messages form an AWS SQS Queue
func (srv *Server) retrieve(wg *sync.WaitGroup, ctx context.Context, messagech chan types.Message, errch chan error) {
	for {
		resp, err := srv.client.ReceiveMessage(
			ctx,
			srv.options.sqsReceiveMessageInput(srv.QueueURL),
		)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			errch <- err
			continue
		}
		for _, message := range resp.Messages {
			messagech <- message
		}
	}
	wg.Done()
}

// handleMessage handle a message, includes calling r.Receive and deleting message
// after receive message success.
func (srv *Server) handleMessage(ctx context.Context, message types.Message) error {
	msgMessage := &msg.Message{
		Attributes: convertToMsgAttrs(message.MessageAttributes),
		Body:       bytes.NewBufferString(*message.Body),
	}
	if message.MessageId != nil {
		msgMessage.Attributes.Set("MessageId", *message.MessageId)
	}
	if message.ReceiptHandle != nil {
		msgMessage.Attributes.Set("ReceiptHandle", *message.ReceiptHandle)
	}
	if err := srv.ReceiveFunc(ctx, msgMessage); err != nil {
		if se := new(visibilityTimeout); errors.As(err, se) {
			if _, err := srv.client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
				QueueUrl:          srv.QueueURL,
				ReceiptHandle:     message.ReceiptHandle,
				VisibilityTimeout: int32(se.duration.Seconds()),
			}); err != nil {
				return err
			}
		} else {
			return err
		}
	}

	if _, err := srv.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      srv.QueueURL,
		ReceiptHandle: message.ReceiptHandle,
	}); err != nil {
		return err
	}

	return nil
}

// Shutdown shutdown the SQS server
//
// Shutdown will handle rest AWS SQS message after calling Shutdown
// until all AWS SQS  have been handled.
func (srv *Server) Shutdown(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-srv.appCtx.Done():
		return msg.ErrServerClosed
	default:
	}

	srv.appCancelFunc()

	fmt.Println("aws-sqs: pubsub server shutdown")

	return nil
}

// convertToMsgAttrs creates msg.Attributes from sqs.Message.Attributes.
func convertToMsgAttrs(awsAttrs map[string]types.MessageAttributeValue) msg.Attributes {
	attr := make(msg.Attributes)
	for k, v := range awsAttrs {
		attr.Set(k, *v.StringValue)
	}
	return attr
}

// VisibilityTimeout.
//
// The default visibility timeout for a message is 30 seconds. The minimum is 0
// seconds. The maximum is 12 hours. For more information, see Visibility Timeout
// (https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html)
func VisibilityTimeout(d time.Duration) error {
	if d.Seconds() < 0 || d.Hours() > 12 {
		return errors.New("aws-msg: visibility timeout, the minimum is 0, seconds. the maximum is 12 hours")
	}
	return visibilityTimeout{duration: d}
}

// visibilityTimeout
type visibilityTimeout struct {
	duration time.Duration
}

func (e visibilityTimeout) Error() string {
	return fmt.Sprintf("Is changing visibility timeout: duration = %s", e.duration)
}

// ReceiptHandle get sqs ReceiptHandle for handling sqs.message from msg.Message
func ReceiptHandle(m *msg.Message) string { return m.Attributes.Get("ReceiptHandle") }

// MessageId get sqs MessageId from Message
func MessageId(w *msg.Message) string { return w.Attributes.Get("MessageId") }
