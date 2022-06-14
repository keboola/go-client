package client

import (
	"context"
	"sync"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

// RunGroupConcurrencyLimit is maximum number of concurrent requests in one RunGroup.
const RunGroupConcurrencyLimit = 32

// RunGroup allows scheduling requests using Add method and then send them concurrently using RunAndWait method.
//
// The sending will stop when the first error occurs.
// The first error will be returned from RunAndWait method.
//
// If you need to send requests immediately,
// or you want to wait and collect all errors, use client.WaitGroup instead.
type RunGroup struct {
	ctx    context.Context
	sender Sender
	start  *sync.WaitGroup
	group  *errgroup.Group
	sem    *semaphore.Weighted // limit concurrency
}

// NewRunGroup creates new RunGroup.
func NewRunGroup(ctx context.Context, sender Sender) *RunGroup {
	return RunGroupWithLimit(ctx, sender, RunGroupConcurrencyLimit)
}

// RunGroupWithLimit creates new RunGroup with given concurrent requests limit.
func RunGroupWithLimit(ctx context.Context, sender Sender, limit int64) *RunGroup {
	// Postpone sending until RunAndWait is called
	start := &sync.WaitGroup{}
	start.Add(1)

	group, ctx := errgroup.WithContext(ctx)
	return &RunGroup{
		ctx:    ctx,
		sender: sender,
		start:  start,
		group:  group,
		sem:    semaphore.NewWeighted(limit),
	}
}

// Add request for sending.
// Request will be sent after calling RunAndWait.
// Additional requests can be added using Add method (for example from a request callback),
// even RunAndWait has already been called, but not finished yet.
func (g *RunGroup) Add(request Sendable) {
	g.group.Go(func() error {
		// Wait for RunAndWait call
		g.start.Wait()

		// Limit number of concurrent requests
		if err := g.sem.Acquire(g.ctx, 1); err != nil {
			// Ctx is done, return
			return nil
		}
		defer g.sem.Release(1)

		return request.SendOrErr(g.ctx, g.sender)
	})
}

// RunAndWait starts sending requests and waits for the result.
// After first error sending stops and error is returned.
//
// Additional requests can be added using Add method (for example from a request callback),
// even RunAndWait has already been called, but not finished yet.
func (g *RunGroup) RunAndWait() error {
	g.start.Done()
	return g.group.Wait()
}
