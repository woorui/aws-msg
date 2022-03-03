package throttle

import (
	"context"
	"time"

	"github.com/zerofox-oss/go-msg"
)

const sleepSecondsKey = "sleep_seconds"

// Throttle decorates msg.Receiver with throttling.
// goroutinue will sleep a while if you set sleep duration by calling Sleep(m, d)
func Throttle(next msg.Receiver) msg.Receiver {
	return msg.ReceiverFunc(func(ctx context.Context, m *msg.Message) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			defer func() {
				ok, d := getSleep(m)
				if ok {
					time.Sleep(d)
				}
			}()
			return next.Receive(ctx, m)
		}
	})
}

// Sleep let Receiver sleep for a while.
func Sleep(m *msg.Message, d time.Duration) {
	m.Attributes.Set(sleepSecondsKey, d.String())
}

func getSleep(m *msg.Message) (bool, time.Duration) {
	val := m.Attributes.Get(sleepSecondsKey)
	if val == "" {
		return false, 0
	}
	d, _ := time.ParseDuration(val)
	return true, d
}
