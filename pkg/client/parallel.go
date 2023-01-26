package client

import (
	"context"
)

type ParallelAPIRequests []Sendable

// Parallel wraps parallel requests to one Sendable interface.
func Parallel(requests ...Sendable) ParallelAPIRequests {
	return requests
}

func (v ParallelAPIRequests) SendOrErr(ctx context.Context) error {
	wg := NewWaitGroup(ctx)
	for _, r := range v {
		wg.Send(r)
	}
	return wg.Wait()
}
