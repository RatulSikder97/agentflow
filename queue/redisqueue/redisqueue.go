// Package redisqueue distributes step jobs across workers with a Redis Stream
// and a consumer group. Each worker process is a consumer; jobs it receives
// sit in the group's pending entries list (PEL) until acknowledged. Jobs left
// pending longer than MinIdle (a crashed or stuck worker) are taken over by
// another consumer with XAUTOCLAIM.
package redisqueue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/RatulSikder97/agentflow/queue"
)

// Options configures a Queue. Zero values take the defaults shown.
type Options struct {
	Stream     string        // "agentflow:jobs"
	Group      string        // "agentflow-workers"
	Consumer   string        // hostname-pid
	MinIdle    time.Duration // 30s: pending time before a job may be reclaimed
	ClaimEvery time.Duration // MinIdle/2: how often to run XAUTOCLAIM
	Block      time.Duration // 1s: XREADGROUP block time
	MaxLen     int64         // 100000: approximate stream cap
}

func (o *Options) defaults() {
	if o.Stream == "" {
		o.Stream = "agentflow:jobs"
	}
	if o.Group == "" {
		o.Group = "agentflow-workers"
	}
	if o.Consumer == "" {
		host, _ := os.Hostname()
		o.Consumer = fmt.Sprintf("%s-%d", host, os.Getpid())
	}
	if o.MinIdle == 0 {
		o.MinIdle = 30 * time.Second
	}
	if o.ClaimEvery == 0 {
		o.ClaimEvery = o.MinIdle / 2
	}
	if o.Block == 0 {
		o.Block = time.Second
	}
	if o.MaxLen == 0 {
		o.MaxLen = 100_000
	}
}

// Queue is a Redis Streams backed job queue.
type Queue struct {
	rdb  redis.UniversalClient
	opts Options

	mu        sync.Mutex
	buf       []redis.XMessage
	lastClaim time.Time
}

// New creates the consumer group (and stream) if needed.
func New(ctx context.Context, rdb redis.UniversalClient, opts Options) (*Queue, error) {
	opts.defaults()
	err := rdb.XGroupCreateMkStream(ctx, opts.Stream, opts.Group, "0").Err()
	if err != nil && !strings.HasPrefix(err.Error(), "BUSYGROUP") {
		return nil, fmt.Errorf("redisqueue: create group: %w", err)
	}
	return &Queue{rdb: rdb, opts: opts}, nil
}

// Enqueue appends a job to the stream.
func (q *Queue) Enqueue(ctx context.Context, job queue.Job) error {
	b, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("redisqueue: encode job: %w", err)
	}
	err = q.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: q.opts.Stream,
		MaxLen: q.opts.MaxLen,
		Approx: true,
		Values: map[string]any{"job": string(b)},
	}).Err()
	if err != nil {
		return fmt.Errorf("redisqueue: xadd: %w", err)
	}
	return nil
}

// Receive returns the next job: first any reclaimed stuck jobs, then new ones.
func (q *Queue) Receive(ctx context.Context) (queue.Delivery, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if msg, ok := q.pop(); ok {
			d, err := q.decode(ctx, msg)
			if err != nil {
				continue // poison message already acked
			}
			return d, nil
		}
		if q.claimDue() {
			if err := q.autoclaim(ctx); err != nil {
				return nil, err
			}
			continue
		}
		streams, err := q.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    q.opts.Group,
			Consumer: q.opts.Consumer,
			Streams:  []string{q.opts.Stream, ">"},
			Count:    1,
			Block:    q.opts.Block,
		}).Result()
		if errors.Is(err, redis.Nil) {
			continue
		}
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, fmt.Errorf("redisqueue: xreadgroup: %w", err)
		}
		q.mu.Lock()
		for _, s := range streams {
			q.buf = append(q.buf, s.Messages...)
		}
		q.mu.Unlock()
	}
}

func (q *Queue) pop() (redis.XMessage, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.buf) == 0 {
		return redis.XMessage{}, false
	}
	m := q.buf[0]
	q.buf = q.buf[1:]
	return m, true
}

func (q *Queue) claimDue() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if time.Since(q.lastClaim) < q.opts.ClaimEvery {
		return false
	}
	q.lastClaim = time.Now()
	return true
}

// autoclaim moves jobs idle for at least MinIdle from any consumer to us.
func (q *Queue) autoclaim(ctx context.Context) error {
	msgs, _, err := q.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   q.opts.Stream,
		Group:    q.opts.Group,
		Consumer: q.opts.Consumer,
		MinIdle:  q.opts.MinIdle,
		Start:    "0-0",
		Count:    16,
	}).Result()
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("redisqueue: xautoclaim: %w", err)
	}
	q.mu.Lock()
	q.buf = append(q.buf, msgs...)
	q.mu.Unlock()
	return nil
}

func (q *Queue) decode(ctx context.Context, msg redis.XMessage) (queue.Delivery, error) {
	raw, _ := msg.Values["job"].(string)
	var job queue.Job
	if err := json.Unmarshal([]byte(raw), &job); err != nil || job.RunID == "" {
		// Undecodable: drop it rather than redeliver forever.
		_ = q.ack(ctx, msg.ID)
		return nil, fmt.Errorf("redisqueue: bad message %s", msg.ID)
	}
	return &delivery{q: q, id: msg.ID, job: job}, nil
}

func (q *Queue) ack(ctx context.Context, id string) error {
	pipe := q.rdb.TxPipeline()
	pipe.XAck(ctx, q.opts.Stream, q.opts.Group, id)
	pipe.XDel(ctx, q.opts.Stream, id)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("redisqueue: ack %s: %w", id, err)
	}
	return nil
}

// Pending returns the number of delivered but unacknowledged jobs.
func (q *Queue) Pending(ctx context.Context) (int64, error) {
	p, err := q.rdb.XPending(ctx, q.opts.Stream, q.opts.Group).Result()
	if err != nil {
		return 0, fmt.Errorf("redisqueue: xpending: %w", err)
	}
	return p.Count, nil
}

type delivery struct {
	q   *Queue
	id  string
	job queue.Job
}

func (d *delivery) Job() queue.Job { return d.job }

func (d *delivery) Ack(ctx context.Context) error { return d.q.ack(ctx, d.id) }

// Extend re-claims the message for the same consumer, which resets its idle
// time so XAUTOCLAIM on other workers leaves it alone.
func (d *delivery) Extend(ctx context.Context) error {
	err := d.q.rdb.XClaimJustID(ctx, &redis.XClaimArgs{
		Stream:   d.q.opts.Stream,
		Group:    d.q.opts.Group,
		Consumer: d.q.opts.Consumer,
		MinIdle:  0,
		Messages: []string{d.id},
	}).Err()
	if err != nil {
		return fmt.Errorf("redisqueue: extend %s: %w", d.id, err)
	}
	return nil
}
