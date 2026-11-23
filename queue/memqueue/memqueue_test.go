package memqueue_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/RatulSikder97/agentflow/queue"
	"github.com/RatulSikder97/agentflow/queue/memqueue"
)

func receive(t *testing.T, q *memqueue.Queue, within time.Duration) queue.Delivery {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), within)
	defer cancel()
	d, err := q.Receive(ctx)
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	return d
}

func TestFIFOAndAck(t *testing.T) {
	q := memqueue.New()
	ctx := context.Background()
	for _, id := range []string{"a", "b"} {
		if err := q.Enqueue(ctx, queue.Job{RunID: "r", StepID: id}); err != nil {
			t.Fatal(err)
		}
	}
	d1, d2 := receive(t, q, time.Second), receive(t, q, time.Second)
	if d1.Job().StepID != "a" || d2.Job().StepID != "b" {
		t.Fatalf("order = %s, %s", d1.Job().StepID, d2.Job().StepID)
	}
	_ = d1.Ack(ctx)
	_ = d2.Ack(ctx)
	if q.Len() != 0 {
		t.Errorf("Len = %d after acks", q.Len())
	}
}

func TestUnackedIsRedelivered(t *testing.T) {
	q := memqueue.New(memqueue.WithVisibility(40 * time.Millisecond))
	ctx := context.Background()
	_ = q.Enqueue(ctx, queue.Job{RunID: "r", StepID: "a"})
	first := receive(t, q, time.Second)
	again := receive(t, q, time.Second) // visibility expires -> redelivered
	if again.Job().Key() != first.Job().Key() {
		t.Fatalf("redelivered %v, want %v", again.Job(), first.Job())
	}
}

func TestExtendDefersRedelivery(t *testing.T) {
	q := memqueue.New(memqueue.WithVisibility(60 * time.Millisecond))
	ctx := context.Background()
	_ = q.Enqueue(ctx, queue.Job{RunID: "r", StepID: "a"})
	d := receive(t, q, time.Second)
	stop := time.After(150 * time.Millisecond)
	tick := time.NewTicker(15 * time.Millisecond)
	defer tick.Stop()
	go func() {
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				_ = d.Extend(ctx)
			}
		}
	}()
	rctx, cancel := context.WithTimeout(ctx, 120*time.Millisecond)
	defer cancel()
	if _, err := q.Receive(rctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Receive during extension = %v, want deadline exceeded", err)
	}
}

func TestCloseUnblocksReceivers(t *testing.T) {
	q := memqueue.New()
	errc := make(chan error, 1)
	go func() {
		_, err := q.Receive(context.Background())
		errc <- err
	}()
	time.Sleep(10 * time.Millisecond)
	_ = q.Close()
	select {
	case err := <-errc:
		if !errors.Is(err, queue.ErrClosed) {
			t.Fatalf("err = %v, want ErrClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("receiver not woken by Close")
	}
	if err := q.Enqueue(context.Background(), queue.Job{RunID: "r"}); !errors.Is(err, queue.ErrClosed) {
		t.Errorf("Enqueue after close = %v", err)
	}
}
