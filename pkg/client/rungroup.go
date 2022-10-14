package client

import (
	"context"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

// RunGroupConcurrencyLimit is the maximum number of concurrent requests in one RunGroup.
const RunGroupConcurrencyLimit = 32

// RunGroup allows scheduling requests by Add method
// and then send them concurrently by the RunAndWait method.
//
// The sending will stop when the first error occurs.
// The first error will be returned from the RunAndWait method.
//
// If you need to send requests immediately,
// or if you want to wait and collect all errors, use client.WaitGroup instead.
type RunGroup struct {
	ctx    context.Context
	sender Sender
	start  chan struct{} // postpone sending until RunAndWait will be called
	group  *errgroup.Group
	sem    *semaphore.Weighted // limit concurrency
}

// NewRunGroup creates a new RunGroup.
func NewRunGroup(ctx context.Context, sender Sender) *RunGroup {
	return RunGroupWithLimit(ctx, sender, RunGroupConcurrencyLimit)
}

// RunGroupWithLimit creates a new RunGroup with given concurrent requests limit.
func RunGroupWithLimit(ctx context.Context, sender Sender, limit int64) *RunGroup {
	group, ctx := errgroup.WithContext(ctx)
	return &RunGroup{
		ctx:    ctx,
		sender: sender,
		start:  make(chan struct{}),
		group:  group,
		sem:    semaphore.NewWeighted(limit),
	}
}

// Add request for sending.
// The request will be sent on call of the RunAndWait method.
// Additional requests can be added using the Add method (for example from a request callback),
// even if RunAndWait has already been called, but is not yet finished.
func (g *RunGroup) Add(request Sendable) {
	g.group.Go(func() error {
		// Postpone sending until RunAndWait will be called
		<-g.start

		// Limit number of concurrent requests
		if err := g.sem.Acquire(g.ctx, 1); err != nil {
			// Ctx is done, return
			return err
		}
		defer g.sem.Release(1)

		return request.SendOrErr(g.ctx)
	})
}

// RunAndWait starts sending requests and waits for the result.
// After the first error sending stops and the error is returned.
//
// Additional requests can be added using the Add method (for example from a request callback),
// even if RunAndWait has already been called, but is not yet finished.
func (g *RunGroup) RunAndWait() error {
	close(g.start)
	return g.group.Wait()
}
