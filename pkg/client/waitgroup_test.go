package client_test

import (
	"context"
	"testing"
	"time"

	"github.com/jarcoal/httpmock"
	"github.com/keboola/go-client/pkg/client"
	"github.com/stretchr/testify/assert"
)

func TestWaitGroup(t *testing.T) {
	t.Parallel()
	c, transport := client.NewMockedClient()
	c = c.WithBaseURL("https://example.com")
	transport.RegisterResponder("GET", `=~^https://example.com/`, httpmock.NewStringResponder(200, "OK"))

	// Create run group
	g := client.NewWaitGroup(context.Background(), c)

	// Send requests
	g.Send(client.NewHTTPRequest().WithGet("foo1"))
	g.Send(client.NewHTTPRequest().WithGet("foo2"))
	g.Send(client.
		NewHTTPRequest().
		WithGet("foo3").
		WithOnSuccess(func(ctx context.Context, sender client.Sender, response client.HTTPResponse) error {
			g.Send(client.NewHTTPRequest().WithGet("foo5"))
			return nil
		}).
		WithOnError(func(ctx context.Context, sender client.Sender, response client.HTTPResponse, err error) error {
			g.Send(client.NewHTTPRequest().WithGet("err"))
			return err
		}),
	)
	g.Send(client.
		NewHTTPRequest().
		WithGet("foo4").
		WithOnSuccess(func(ctx context.Context, sender client.Sender, response client.HTTPResponse) error {
			g.Send(client.NewHTTPRequest().WithGet("foo6"))
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
	g := client.NewWaitGroup(context.Background(), c)

	// Send requests
	requestsCount := 100
	assert.Greater(t, requestsCount, client.RunGroupConcurrencyLimit)
	for i := 1; i <= requestsCount; i++ {
		g.Send(client.NewHTTPRequest().WithGet("foo"))
	}

	// All errors are returned
	err := g.Wait()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), `100 errors occurred:`)

	// All requests have been sent
	assert.Equal(t, transport.GetTotalCallCount(), 100)
}
