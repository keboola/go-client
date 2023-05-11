package request

import (
	"context"
	"fmt"
)

// APIRequest with response mapped to the generic type R.
type APIRequest[R Result] interface {
	// WithBefore method registers callback to be executed before the request.
	// If an error is returned, the request is not sent.
	WithBefore(func(ctx context.Context) error) APIRequest[R]
	// WithOnComplete method registers callback to be executed when the request is completed.
	WithOnComplete(func(ctx context.Context, result R, err error) error) APIRequest[R]
	// WithOnSuccess method registers callback to be executed when the request is completed and `code >= 200 and <= 299`.
	WithOnSuccess(func(ctx context.Context, result R) error) APIRequest[R]
	// WithOnError method registers callback to be executed when the request is completed and `code >= 400`.
	WithOnError(func(ctx context.Context, err error) error) APIRequest[R]
	// Send sends the request by the sender.
	Send(ctx context.Context) (result R, err error)
	SendOrErr(ctx context.Context) error
}

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

// NewAPIRequest creates an API request with the result mapped to the R type.
// It is composed of one or multiple Sendable (HTTPRequest or APIRequest).
func NewAPIRequest[R Result](result R, requests ...Sendable) APIRequest[R] {
	if len(requests) == 0 {
		panic(fmt.Errorf("at least one request must be provided"))
	}
	return &apiRequest[R]{requests: requests, result: result}
}

// NewNoOperationAPIRequest returns an APIRequest that immediately returns a Result without calling any HTTPRequest.
// It is handy in situations where there is no work to be done.
func NewNoOperationAPIRequest[R Result](result R) APIRequest[R] {
	return &apiRequest[R]{result: result}
}

// apiRequest implements generic APIRequest interface.
type apiRequest[R Result] struct {
	requests []Sendable
	before   []func(ctx context.Context) error
	after    []func(ctx context.Context, result R, err error) error
	result   R
}

func (r apiRequest[R]) WithBefore(fn func(ctx context.Context) error) APIRequest[R] {
	r.before = append(r.before, fn)
	return r
}

func (r apiRequest[R]) WithOnComplete(fn func(ctx context.Context, result R, err error) error) APIRequest[R] {
	r.after = append(r.after, fn)
	return r
}

func (r apiRequest[R]) WithOnSuccess(fn func(ctx context.Context, result R) error) APIRequest[R] {
	r.after = append(r.after, func(ctx context.Context, result R, err error) error {
		if err == nil {
			err = fn(ctx, result)
		}
		return err
	})
	return r
}

func (r apiRequest[R]) WithOnError(fn func(ctx context.Context, err error) error) APIRequest[R] {
	r.after = append(r.after, func(ctx context.Context, result R, err error) error {
		if err != nil {
			err = fn(ctx, err)
		}
		return err
	})
	return r
}

func (r apiRequest[R]) Send(ctx context.Context) (result R, err error) {
	// Stop if context has been cancelled
	if err := ctx.Err(); err != nil {
		return r.result, err
	}

	// Invoke "before" listeners
	for _, fn := range r.before {
		if err := fn(ctx); err != nil {
			return r.result, err
		}
	}

	// Stop if context has been cancelled
	if err := ctx.Err(); err != nil {
		return r.result, err
	}

	// Send requests in parallel
	wg := NewWaitGroup(ctx)
	for _, request := range r.requests {
		wg.Send(request)
	}

	// Process error by listener, if any
	err = wg.Wait()

	// Invoke "after" listeners
	for _, fn := range r.after {
		// Stop if context has been cancelled
		if err := ctx.Err(); err != nil {
			return r.result, err
		}
		err = fn(ctx, r.result, err)
	}

	return r.result, err
}

func (r apiRequest[R]) SendOrErr(ctx context.Context) error {
	_, err := r.Send(ctx)
	return err
}