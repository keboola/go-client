package request_test

import (
	"context"
	"testing"
	"time"

	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/request"
)

func TestWaitGroup(t *testing.T) {
	t.Parallel()
	c, transport := client.NewMockedClient()
	c = c.WithBaseURL("https://example.com")
	transport.RegisterResponder("GET", `=~^https://example.com/`, httpmock.NewStringResponder(200, "OK"))

	// Create run group
	g := request.NewWaitGroup(context.Background())

	// Send requests
	g.Send(request.NewHTTPRequest(c).WithGet("foo1"))
	g.Send(request.NewHTTPRequest(c).WithGet("foo2"))
	g.Send(request.NewHTTPRequest(c).
		WithGet("foo3").
		WithOnSuccess(func(ctx context.Context, response request.HTTPResponse) error {
			g.Send(request.NewHTTPRequest(c).WithGet("foo5"))
			return nil
		}).
		WithOnError(func(ctx context.Context, response request.HTTPResponse, err error) error {
			g.Send(request.NewHTTPRequest(c).WithGet("err"))
			return err
		}),
	)
	g.Send(request.NewHTTPRequest(c).
		WithGet("foo4").
		WithOnSuccess(func(ctx context.Context, response request.HTTPResponse) error {
			g.Send(request.NewHTTPRequest(c).WithGet("foo6"))
			return nil
		}),
	)

	// Requests are sent immediately
	time.Sleep(100 * time.Millisecond)
	assert.Greater(t, transport.GetTotalCallCount(), 0)

	// Wait for all requests
	assert.NoError(t, g.Wait())

	// No new request
	assert.Equal(t, map[string]int{
		"GET =~^https://example.com/":  6,
		"GET https://example.com/foo1": 1,
		"GET https://example.com/foo2": 1,
		"GET https://example.com/foo3": 1,
		"GET https://example.com/foo4": 1,
		"GET https://example.com/foo5": 1,
		"GET https://example.com/foo6": 1,
	}, transport.GetCallCountInfo())
}

func TestWaitGroup_HandleError(t *testing.T) {
	t.Parallel()
	c, transport := client.NewMockedClient()
	c = c.WithBaseURL("https://example.com")
	transport.RegisterResponder("GET", `=~^https://example.com/`, httpmock.NewStringResponder(401, "Forbidden"))

	// Create run group
	g := request.NewWaitGroup(context.Background())

	// Send requests
	requestsCount := 100
	assert.Greater(t, requestsCount, request.RunGroupConcurrencyLimit)
	for i := 1; i <= requestsCount; i++ {
		g.Send(request.NewHTTPRequest(c).WithGet("foo"))
	}

	// All errors are returned
	err := g.Wait()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), `100 errors occurred:`)

	// All requests have been sent
	assert.Equal(t, transport.GetTotalCallCount(), 100)
}
