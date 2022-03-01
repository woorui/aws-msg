package sqs

import (
	"bytes"
	"context"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/zerofox-oss/go-msg"
)

// ServerClient holds some necessary methods of *sqs.Client for server
type ServerClient interface {
	GetQueueUrl(ctx context.Context, params *sqs.GetQueueUrlInput, optFns ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error)
	ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
}

// Server represents a msg.Server for pulling messages and receiving messages
// from an AWS SQS Queue.
type Server struct {
	options                    *serverOption
	errch                      chan error
	pool                       chan struct{}      // The maximum number of aws message receive routines allowed.
	messageCh                  chan types.Message // buffered sqs message channel.
	appCtx                     context.Context    // context used to control the lifecycle of the Server.
	appCancelFunc              context.CancelFunc // CancelFunc to signal the server should stop requesting messages.
	QueueURL                   *string
	client                     ServerClient
	ReceiveFunc                msg.ReceiverFunc
	sqsReceiveMessageInputPool *sync.Pool
	wg                         *sync.WaitGroup
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
	serverCtx, appCancelFunc := context.WithCancel(options.ctx)

	urlOutput, err := client.GetQueueUrl(serverCtx, &sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	})
	if err != nil {
		appCancelFunc()
		return nil, err
	}

	srv := &Server{
		options:       options,
		errch:         make(chan error, 1),
		pool:          make(chan struct{}, options.poolSize),
		messageCh:     make(chan types.Message, options.messageBacklogSize),
		appCtx:        serverCtx,
		appCancelFunc: appCancelFunc,
		QueueURL:      urlOutput.QueueUrl,
		client:        client,
		sqsReceiveMessageInputPool: &sync.Pool{New: func() interface{} {
			return options.sqsReceiveMessageInput(urlOutput.QueueUrl)
		}},
	}

	return srv, nil
}

// Serve continuously receives messages from an AWS SQS Queue, and calls Receive on `r`.
//
// Serve is blocking and will not return until Shutdown is called or unknown error happened on the Server.
func (srv *Server) Serve(r msg.Receiver) error {
	srv.ReceiveFunc = r.Receive

	userCtx, cancel := context.WithTimeout(srv.options.ctx, srv.options.timeout)

	srv.wg = &sync.WaitGroup{}

	// start work
	for i := 0; i < int(srv.options.poolSize); i++ {
		srv.pool <- struct{}{}
	}
	for {
		select {
		case err := <-srv.errch:
			cancel()
			return err
		case <-srv.appCtx.Done():
			cancel()
			return srv.handleErr(srv.appCtx.Err())
		case <-srv.pool:
			go func() {
				srv.receiveMessage(userCtx)
				srv.pool <- struct{}{}
			}()
		case message := <-srv.messageCh:
			srv.wg.Add(1)
			go func() {
				defer srv.wg.Done()
				srv.handleMessage(userCtx, message)
			}()
		}
	}
}

// receiveMessage uses sqs.Client.ReceiveMessage to pull messages form an AWS SQS Queue
func (srv *Server) receiveMessage(ctx context.Context) error {
	input := srv.sqsReceiveMessageInputPool.Get().(*sqs.ReceiveMessageInput)
	defer func() {
		srv.sqsReceiveMessageInputPool.Put(input)
	}()
	resp, err := srv.client.ReceiveMessage(ctx, input)
	if err != nil {
		if err = srv.handleErr(err); err != nil {
			return err
		}
	}
	for _, message := range resp.Messages {
		srv.messageCh <- message
	}
	return nil
}

func (srv *Server) handleErr(err error) error {
	err = srv.options.errHandler(srv.appCtx, err)
	if err != nil {
		srv.errch <- err
	}
	return err
}

// handleMessage handle a message, includes calling r.Receive and deleting message
// after receive message success.
func (srv *Server) handleMessage(ctx context.Context, message types.Message) error {
	msgMessage := &msg.Message{
		Attributes: convertToMsgAttrs(message.MessageAttributes),
		Body:       bytes.NewBufferString(*message.Body),
	}
	messageId := message.MessageId
	if messageId != nil {
		msgMessage.Attributes.Set("MessageId", *messageId)
	}
	if err := srv.ReceiveFunc(ctx, msgMessage); err != nil {
		if err = srv.handleErr(err); err != nil {
			return err
		}
	}
	if _, err := srv.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      srv.QueueURL,
		ReceiptHandle: message.ReceiptHandle,
	}); err != nil {
		if err = srv.handleErr(err); err != nil {
			return err
		}
	}
	return nil
}

// Shutdown shutdown the SQS server
//
// Shutdown will handle rest AWS SQS message after calling Shutdown
// until all AWS SQS  have been handled.
func (srv *Server) Shutdown(ctx context.Context) error {
	srv.appCancelFunc()

	srv.wg.Wait()
	srv.errch <- msg.ErrServerClosed

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
