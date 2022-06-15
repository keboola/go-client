package client

import (
	"context"
	"sync"

	"golang.org/x/sync/semaphore"

	"github.com/hashicorp/go-multierror"
)

// WaitGroupConcurrencyLimit is the  maximum number of concurrent requests in one WaitGroup.
const WaitGroupConcurrencyLimit = 8

// WaitGroup allows sending requests concurrently using Send method
// and wait until all requests are completed using the Wait method.
//
// The request starts immediately after calling the Send method.
// If an error occurs, sending will not stop, all requests will be sent.
// Wait method at the end returns all errors that have occurred, if any.
//
// If you need to schedule requests and send them later,
// or if you want to stop at the first error, use client.RunGroup instead.
type WaitGroup struct {
	ctx    context.Context
	sender Sender
	wg     *sync.WaitGroup     // wait for all
	sem    *semaphore.Weighted // limit concurrency

	lock *sync.Mutex // for err
	err  *multierror.Error
}

// NewWaitGroup creates new WaitGroup.
func NewWaitGroup(ctx context.Context, sender Sender) *WaitGroup {
	return NewWaitGroupWithLimit(ctx, sender, WaitGroupConcurrencyLimit)
}

// NewWaitGroupWithLimit creates new WaitGroup with given concurrent requests  limit.
func NewWaitGroupWithLimit(ctx context.Context, sender Sender, limit int64) *WaitGroup {
	return &WaitGroup{ctx: ctx, sender: sender, wg: &sync.WaitGroup{}, sem: semaphore.NewWeighted(limit), lock: &sync.Mutex{}}
}

// Wait for all requests to complete. All errors that have occurred will be returned.
func (g *WaitGroup) Wait() error {
	g.wg.Wait()
	// If there is only one error, then unwrap multierror
	if g.err != nil && len(g.err.Errors) == 1 {
		return g.err.Errors[0]
	}
	return g.err.ErrorOrNil()
}

// Send a concurrent request.
func (g *WaitGroup) Send(request Sendable) {
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()

		// Limit number of concurrent requests
		if err := g.sem.Acquire(g.ctx, 1); err != nil {
			// Ctx is done, return
			return
		}
		defer g.sem.Release(1)

		if err := request.SendOrErr(g.ctx, g.sender); err != nil {
			g.lock.Lock()
			defer g.lock.Unlock()
			g.err = multierror.Append(g.err, err)
		}
	}()
}
