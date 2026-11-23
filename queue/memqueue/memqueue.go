// Package memqueue is an in-process queue with the same at-least-once
// semantics as redisqueue: unacknowledged jobs become visible again after a
// visibility timeout.
package memqueue

import (
	"context"
	"sync"
	"time"

	"github.com/RatulSikder97/agentflow/queue"
)

// DefaultVisibility is how long a received job stays invisible before it is
// redelivered when it has not been acknowledged.
const DefaultVisibility = 30 * time.Second

type entry struct {
	id       uint64
	job      queue.Job
	deadline time.Time // zero while ready
}

// Queue is a concurrency-safe in-memory queue.
type Queue struct {
	mu         sync.Mutex
	ready      []*entry
	inflight   map[uint64]*entry
	nextID     uint64
	visibility time.Duration
	notify     chan struct{}
	done       chan struct{}
	closed     bool
}

// Option configures a Queue.
type Option func(*Queue)

// WithVisibility sets the redelivery timeout for unacknowledged jobs.
func WithVisibility(d time.Duration) Option { return func(q *Queue) { q.visibility = d } }

// New returns an empty queue.
func New(opts ...Option) *Queue {
	q := &Queue{
		inflight:   map[uint64]*entry{},
		visibility: DefaultVisibility,
		notify:     make(chan struct{}, 1),
		done:       make(chan struct{}),
	}
	for _, o := range opts {
		o(q)
	}
	return q
}

// Enqueue adds a job.
func (q *Queue) Enqueue(_ context.Context, job queue.Job) error {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return queue.ErrClosed
	}
	q.nextID++
	q.ready = append(q.ready, &entry{id: q.nextID, job: job})
	q.mu.Unlock()
	q.wake()
	return nil
}

func (q *Queue) wake() {
	select {
	case q.notify <- struct{}{}:
	default:
	}
}

// Receive blocks until a job is available, ctx is done or the queue closes.
func (q *Queue) Receive(ctx context.Context) (queue.Delivery, error) {
	tick := time.NewTicker(q.visibility / 4)
	defer tick.Stop()
	for {
		if d, ok, err := q.tryReceive(); err != nil || ok {
			return d, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-q.done:
			return nil, queue.ErrClosed
		case <-q.notify:
		case <-tick.C:
		}
	}
}

func (q *Queue) tryReceive() (queue.Delivery, bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil, false, queue.ErrClosed
	}
	now := time.Now()
	for id, e := range q.inflight { // reclaim expired leases
		if now.After(e.deadline) {
			delete(q.inflight, id)
			e.deadline = time.Time{}
			q.ready = append(q.ready, e)
		}
	}
	if len(q.ready) == 0 {
		return nil, false, nil
	}
	e := q.ready[0]
	q.ready = q.ready[1:]
	e.deadline = now.Add(q.visibility)
	q.inflight[e.id] = e
	if len(q.ready) > 0 {
		q.wake() // more work: let another blocked receiver proceed
	}
	return &delivery{q: q, e: e}, true, nil
}

// Len returns ready plus in-flight jobs.
func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.ready) + len(q.inflight)
}

// Close wakes all receivers with queue.ErrClosed.
func (q *Queue) Close() error {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return nil
	}
	q.closed = true
	q.mu.Unlock()
	close(q.done)
	return nil
}

type delivery struct {
	q *Queue
	e *entry
}

func (d *delivery) Job() queue.Job { return d.e.job }

func (d *delivery) Ack(context.Context) error {
	d.q.mu.Lock()
	defer d.q.mu.Unlock()
	delete(d.q.inflight, d.e.id)
	return nil
}

func (d *delivery) Extend(context.Context) error {
	d.q.mu.Lock()
	defer d.q.mu.Unlock()
	if _, ok := d.q.inflight[d.e.id]; ok {
		d.e.deadline = time.Now().Add(d.q.visibility)
	}
	return nil
}
