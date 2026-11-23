package redisqueue_test

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/RatulSikder97/agentflow/queue"
	"github.com/RatulSikder97/agentflow/queue/redisqueue"
)

func setup(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return mr, rdb
}

func newQueue(t *testing.T, rdb *redis.Client, consumer string, minIdle time.Duration) *redisqueue.Queue {
	t.Helper()
	q, err := redisqueue.New(context.Background(), rdb, redisqueue.Options{
		Consumer: consumer, MinIdle: minIdle, ClaimEvery: minIdle / 2, Block: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	return q
}

func recv(t *testing.T, q *redisqueue.Queue, within time.Duration) (queue.Delivery, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), within)
	defer cancel()
	return q.Receive(ctx)
}

func TestEnqueueReceiveAck(t *testing.T) {
	_, rdb := setup(t)
	ctx := context.Background()
	q := newQueue(t, rdb, "w1", time.Minute)
	job := queue.Job{RunID: "run_1", StepID: "a", Trace: map[string]string{"traceparent": "00-abc-def-01"}}
	if err := q.Enqueue(ctx, job); err != nil {
		t.Fatal(err)
	}
	d, err := recv(t, q, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if d.Job().RunID != job.RunID || d.Job().StepID != job.StepID || d.Job().Trace["traceparent"] != "00-abc-def-01" {
		t.Fatalf("job = %+v", d.Job())
	}
	if n, _ := q.Pending(ctx); n != 1 {
		t.Errorf("pending before ack = %d, want 1", n)
	}
	if err := d.Ack(ctx); err != nil {
		t.Fatal(err)
	}
	if n, _ := q.Pending(ctx); n != 0 {
		t.Errorf("pending after ack = %d, want 0", n)
	}
	if _, err := recv(t, q, 100*time.Millisecond); err == nil {
		t.Error("received a job after ack")
	}
}

func TestGroupCreationIsIdempotent(t *testing.T) {
	_, rdb := setup(t)
	newQueue(t, rdb, "w1", time.Minute)
	newQueue(t, rdb, "w2", time.Minute) // BUSYGROUP must be tolerated
}

// A job received by a consumer that dies is claimed by another consumer once
// it has been idle for MinIdle.
func TestStuckJobIsAutoclaimed(t *testing.T) {
	_, rdb := setup(t)
	ctx := context.Background()
	dead := newQueue(t, rdb, "dead-worker", 50*time.Millisecond)
	if err := dead.Enqueue(ctx, queue.Job{RunID: "run_1", StepID: "b"}); err != nil {
		t.Fatal(err)
	}
	if _, err := recv(t, dead, time.Second); err != nil {
		t.Fatal(err)
	}
	// dead-worker never acks.
	alive := newQueue(t, rdb, "alive-worker", 50*time.Millisecond)
	d, err := recv(t, alive, 2*time.Second)
	if err != nil {
		t.Fatalf("job was not reclaimed: %v", err)
	}
	if d.Job().StepID != "b" {
		t.Fatalf("claimed %+v", d.Job())
	}
	if err := d.Ack(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestExtendPreventsClaim(t *testing.T) {
	_, rdb := setup(t)
	ctx := context.Background()
	busy := newQueue(t, rdb, "busy", 80*time.Millisecond)
	_ = busy.Enqueue(ctx, queue.Job{RunID: "run_1", StepID: "slow"})
	d, err := recv(t, busy, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		tick := time.NewTicker(20 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				_ = d.Extend(ctx)
			}
		}
	}()
	other := newQueue(t, rdb, "other", 80*time.Millisecond)
	if d2, err := recv(t, other, 300*time.Millisecond); err == nil {
		t.Fatalf("job %+v was stolen while being extended", d2.Job())
	}
}

func TestPoisonMessageIsDropped(t *testing.T) {
	_, rdb := setup(t)
	ctx := context.Background()
	q := newQueue(t, rdb, "w1", time.Minute)
	if err := rdb.XAdd(ctx, &redis.XAddArgs{Stream: "agentflow:jobs", Values: map[string]any{"job": "not json"}}).Err(); err != nil {
		t.Fatal(err)
	}
	_ = q.Enqueue(ctx, queue.Job{RunID: "run_1", StepID: "ok"})
	d, err := recv(t, q, time.Second)
	if err != nil || d.Job().StepID != "ok" {
		t.Fatalf("got %v, %v; want the valid job", d, err)
	}
}
