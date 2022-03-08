package sqsmsg

import (
	"bytes"
	"context"
	"errors"
	"io/ioutil"
	"math"
	"strings"
	"sync"
	"time"

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
	QueueURL *string
	client   TopicClient
}

// NewTopic returns an AWS SQS topic wrap
func NewTopic(appCtx context.Context, queueName string, client TopicClient) (msg.Topic, error) {
	urlOutput, err := client.GetQueueUrl(appCtx, &sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	})

	if err != nil {
		return nil, err
	}

	return &Topic{
		QueueURL: urlOutput.QueueUrl,
		client:   client,
	}, nil
}

// MessageWriter writes data to an AWS SQS Queue.
type MessageWriter struct {
	message  *msg.Message
	ctx      context.Context
	closed   bool
	mux      sync.Mutex
	queueURL *string     // the URL to the queue.
	client   TopicClient // SQS client
	params   *sqs.SendMessageInput
	optFns   []func(*sqs.Options)
}

// NewWriter returns a MessageWriter.
func (t *Topic) NewWriter(ctx context.Context) msg.MessageWriter {
	w := &MessageWriter{
		message:  &msg.Message{},
		ctx:      ctx,
		closed:   false,
		mux:      sync.Mutex{},
		queueURL: t.QueueURL,
		client:   t.client,
		params:   &sqs.SendMessageInput{},
	}
	return w
}

// Attributes returns the msg.Attributes associated with the MessageWriter
func (w *MessageWriter) Attributes() *msg.Attributes {
	if w.message.Attributes == nil {
		w.message.Attributes = make(map[string][]string)
	}
	return &w.message.Attributes
}

// Write writes data to the MessageWriter's internal buffer.
//
// Once a MessageWriter is closed, it cannot be used again.
func (w *MessageWriter) Write(p []byte) (int, error) {
	var buf bytes.Buffer
	w.mux.Lock()
	defer w.mux.Unlock()

	n, err := buf.Write(p)
	if err != nil {
		return 0, err
	}
	w.message.Body = &buf

	if w.closed {
		return 0, msg.ErrClosedMessageWriter
	}
	return n, err
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

	buf, err := ioutil.ReadAll(w.message.Body)
	if err != nil {
		return err
	}

	w.params.MessageBody = aws.String(string(buf))

	attrs := *w.Attributes()
	if len(attrs) > 0 {
		if w.params.MessageAttributes == nil {
			w.params.MessageAttributes = make(map[string]types.MessageAttributeValue, len(attrs))
		}
		for key, val := range buildSQSAttributes(w.Attributes()) {
			w.params.MessageAttributes[key] = val
		}
	}

	_, err = w.client.SendMessage(w.ctx, w.params, w.optFns...)
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

// SetDelay sets a delay on the Message.
// The delay must be between 0 and 900 seconds, according to the aws sdk.
func SetDelay(mw msg.MessageWriter, delay time.Duration) error {
	w, ok := mw.(*MessageWriter)
	if !ok {
		return errors.New("aws-msg-sqs: Can't use SetDelay to current MessageWriter")
	}
	w.params.DelaySeconds = int32(math.Min(math.Max(delay.Seconds(), 0), 900))
	return nil
}

// SetOptFns sets sqs SendMessage api optFns.
func SetOptFns(mw msg.MessageWriter, optFns ...func(*sqs.Options)) error {
	w, ok := mw.(*MessageWriter)
	if !ok {
		return errors.New("aws-msg-sqs: Can't use SetOptFns to current MessageWriter")
	}
	w.optFns = optFns
	return nil
}
