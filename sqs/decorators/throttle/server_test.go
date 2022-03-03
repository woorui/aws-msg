package throttle

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zerofox-oss/go-msg"
)

type mockServer struct {
	sleeped int32
}

func (s *mockServer) Receive(ctx context.Context, m *msg.Message) error {
	ok, _ := getSleep(m)
	if ok {
		sleeped := atomic.AddInt32(&s.sleeped, 1)
		s.sleeped = sleeped
	}
	return nil
}

func Test_Server(t *testing.T) {
	server := &mockServer{}

	ctx := context.Background()

	m := &msg.Message{Attributes: make(msg.Attributes)}

	Sleep(m, time.Second)

	receiver := Throttle(server)

	receiver.Receive(ctx, m)

	if server.sleeped != 1 {
		t.Fatal("Throttle sleep case test failed")
	}
}

func Test_No_Sleep_Server(t *testing.T) {
	server := &mockServer{}

	ctx := context.Background()

	m := &msg.Message{Attributes: make(msg.Attributes)}

	receiver := Throttle(server)

	receiver.Receive(ctx, m)

	if server.sleeped != 0 {
		t.Fatal("Throttle no sleep case test failed")
	}
}

func Test_Ctx_Cancel_Server(t *testing.T) {
	server := &mockServer{}

	ctx, cancel := context.WithCancel(context.Background())

	m := &msg.Message{Attributes: make(msg.Attributes)}

	receiver := Throttle(server)
	cancel()

	err := receiver.Receive(ctx, m)

	if err != ctx.Err() {
		t.Fatal("Throttle ctx cancel case test failed")
	}
}
