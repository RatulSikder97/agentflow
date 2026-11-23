// Package queue defines the step job message passed between the scheduler and
// workers. Implementations: memqueue (in-process) and redisqueue (Redis
// Streams consumer groups).
//
// Delivery is at-least-once. A job that is received but not acknowledged is
// redelivered after the implementation's visibility timeout, so handlers must
// be idempotent; the engine achieves that with compare-and-set step state.
package queue

import (
	"context"
	"errors"
)

// ErrClosed is returned by Receive after the queue has been closed.
var ErrClosed = errors.New("queue closed")

// Job asks a worker to execute one step of one run.
type Job struct {
	RunID  string `json:"run_id"`
	StepID string `json:"step_id"`
	// Trace carries W3C trace context so a step span joins the run's trace
	// even when it executes on another machine.
	Trace map[string]string `json:"trace,omitempty"`
}

// Key is the idempotency key of the job.
func (j Job) Key() string { return j.RunID + "/" + j.StepID }

// Delivery is a received job that must be acknowledged once handled.
type Delivery interface {
	Job() Job
	// Ack removes the job from the queue permanently.
	Ack(ctx context.Context) error
	// Extend tells the queue the job is still being worked on, resetting its
	// visibility timeout so it is not redelivered to another worker.
	Extend(ctx context.Context) error
}
