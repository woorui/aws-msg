package sqs

import (
	"bytes"
	"context"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/zerofox-oss/go-msg"
)

// TopicClient holds some necessary methods of *sqs.Client for topic
type TopicClient interface {
	GetQueueUrl(ctx context.Context, params *sqs.GetQueueUrlInput, optFns ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error)
	SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
}

// Topic publishes Messages to an AWS SQS.
type Topic struct {
	options  *topicOption
	QueueURL *string
	client   TopicClient
}

// NewTopic returns an AWS SQS topic wrap
func NewTopic(appCtx context.Context, queueName string, client TopicClient, op ...TopicOption) (msg.Topic, error) {
	urlOutput, err := client.GetQueueUrl(appCtx, &sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	})

	if err != nil {
		return nil, err
	}

	options := defaultTopicOptions()
	for _, o := range op {
		if err := o(options); err != nil {
			return nil, err
		}
	}

	return &Topic{
		options:  options,
		QueueURL: urlOutput.QueueUrl,
		client:   client,
	}, nil
}

// MessageWriter writes data to an AWS SQS Queue.
type MessageWriter struct {
	attributes msg.Attributes
	buf        *bytes.Buffer
	ctx        context.Context
	closed     bool
	mux        sync.Mutex
	queueURL   *string     // the URL to the queue.
	client     TopicClient // SQS client
	input      func(body, queueUrl *string) *sqs.SendMessageInput
}

// NewWriter returns a MessageWriter.
func (t *Topic) NewWriter(ctx context.Context) msg.MessageWriter {
	return &MessageWriter{
		buf:      &bytes.Buffer{},
		ctx:      ctx,
		client:   t.client,
		queueURL: t.QueueURL,
		input:    t.options.sqsSendMessageInput,
	}
}

// Attributes returns the msg.Attributes associated with the MessageWriter
func (w *MessageWriter) Attributes() *msg.Attributes {
	if w.attributes == nil {
		w.attributes = make(map[string][]string)
	}
	return &w.attributes
}

// Write writes data to the MessageWriter's internal buffer.
//
// Once a MessageWriter is closed, it cannot be used again.
func (w *MessageWriter) Write(p []byte) (int, error) {
	w.mux.Lock()
	defer w.mux.Unlock()

	if w.closed {
		return 0, msg.ErrClosedMessageWriter
	}
	return w.buf.Write(p)
}

// Close converts it's buffered data and attributes to an SQS message
// and publishes it to a queue.
//
// If the MessageWriter is already closed it will return an error.
func (w *MessageWriter) Close() error {
	w.mux.Lock()
	defer w.mux.Unlock()

	if w.closed {
		return msg.ErrClosedMessageWriter
	}
	w.closed = true

	params := w.input(aws.String(w.buf.String()), w.queueURL)

	attrs := *w.Attributes()
	if len(attrs) > 0 {
		if params.MessageAttributes == nil {
			params.MessageAttributes = make(map[string]types.MessageAttributeValue, len(attrs))
		}
		for key, val := range buildSQSAttributes(w.Attributes()) {
			params.MessageAttributes[key] = val
		}
	}

	_, err := w.client.SendMessage(w.ctx, params)
	return err
}

// buildSQSAttributes converts msg.Attributes into SQS message attributes.
// uses csv encoding to use AWS's String datatype
func buildSQSAttributes(attr *msg.Attributes) map[string]types.MessageAttributeValue {
	attrs := make(map[string]types.MessageAttributeValue)

	for k, v := range *attr {
		attrs[k] = types.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String(strings.Join(v, ",")),
		}
	}
	return attrs
}
