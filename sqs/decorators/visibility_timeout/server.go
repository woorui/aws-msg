package visibilitytimeout

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqsmsg "github.com/woorui/aws-msg/sqs"
	"github.com/zerofox-oss/go-msg"
)

const changeVisibilityTimeoutKey = "change_visibility_timeout_seconds"

type VisibilityTimeout struct {
	sqsClient Client
}

// Client interface of sqs.Client
type Client interface {
	ChangeMessageVisibility(
		ctx context.Context,
		params *sqs.ChangeMessageVisibilityInput,
		optFns ...func(*sqs.Options),
	) (*sqs.ChangeMessageVisibilityOutput, error)
}

// New return an VisibilityTimeout Decorator
func New(sqsClient Client) *VisibilityTimeout { return &VisibilityTimeout{sqsClient: sqsClient} }

// Decorator decorates msg.Receiver with a timeout for change sqs message visibility.
//
// use `SetVisibilityTimeout` to inject the visibilityTimeout you want.
func (v *VisibilityTimeout) Decorator(next msg.Receiver) msg.Receiver {
	return msg.ReceiverFunc(func(ctx context.Context, m *msg.Message) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			err := next.Receive(ctx, m)
			if err != nil {
				return err
			}
			ok, seconds := getVisibilityTimeout(m)
			if ok {
				_, err := v.sqsClient.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
					ReceiptHandle:     aws.String(sqsmsg.MessageId(m)),
					VisibilityTimeout: int32(seconds),
				})
				if err != nil {
					return err
				}
			}
			return nil
		}
	})
}

// SetVisibilityTimeout.
//
// The default visibility timeout for a message is 30 seconds. The minimum is 0
// seconds. The maximum is 12 hours. For more information, see Visibility Timeout
// (https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html)
func SetVisibilityTimeout(m *msg.Message, d time.Duration) error {
	if d.Seconds() < 0 || d.Hours() > 12 {
		return errors.New("aws-msg: visibility timeout, the minimum is 0, seconds. the maximum is 12 hours")
	}
	m.Attributes.Set(changeVisibilityTimeoutKey, strconv.Itoa(int(d.Seconds())))
	return nil
}

func getVisibilityTimeout(m *msg.Message) (bool, int32) {
	val := m.Attributes.Get(changeVisibilityTimeoutKey)
	if val == "" {
		return false, 0
	}
	sec := m.Attributes.Get(changeVisibilityTimeoutKey)
	seconds, _ := strconv.Atoi(sec)

	return true, int32(seconds)
}
