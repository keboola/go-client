package request_test

import (
	"context"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/request"
)

func TestRunGroup(t *testing.T) {
	t.Parallel()
	c, transport := client.NewMockedClient()
	c = c.WithBaseURL("https://example.com")
	transport.RegisterResponder("GET", `=~^https://example.com/`, httpmock.NewStringResponder(200, "OK"))

	// Create run group
	g := request.NewRunGroup(context.Background(), c)

	// Add requests
	g.Add(request.NewHTTPRequest(c).WithGet("foo1"))
	g.Add(request.NewHTTPRequest(c).WithGet("foo2"))
	g.Add(request.NewHTTPRequest(c).
		WithGet("foo3").
		WithOnSuccess(func(ctx context.Context, response request.HTTPResponse) error {
			g.Add(request.NewHTTPRequest(c).WithGet("foo5"))
			return nil
		}).
		WithOnError(func(ctx context.Context, response request.HTTPResponse, err error) error {
			g.Add(request.NewHTTPRequest(c).WithGet("err"))
			return err
		}),
	)
	g.Add(request.NewHTTPRequest(c).
		WithGet("foo4").
		WithOnSuccess(func(ctx context.Context, response request.HTTPResponse) error {
			g.Add(request.NewHTTPRequest(c).WithGet("foo6"))
			return nil
		}),
	)

	// No requests have been sent yet
	assert.Equal(t, 0, transport.GetTotalCallCount())

	// Run and wait
	assert.NoError(t, g.RunAndWait())

	// All requests have been sent
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

func TestRunGroup_HandleError(t *testing.T) {
	t.Parallel()
	c, transport := client.NewMockedClient()
	c = c.WithBaseURL("https://example.com")
	transport.RegisterResponder("GET", `=~^https://example.com/`, httpmock.NewStringResponder(401, "Forbidden"))

	// Create run group
	g := request.NewRunGroup(context.Background(), c)

	// Add requests
	requestsCount := 100
	assert.Greater(t, requestsCount, request.RunGroupConcurrencyLimit)
	for i := 1; i <= requestsCount; i++ {
		g.Add(request.NewHTTPRequest(c).WithGet("foo"))
	}

	// No requests have been sent yet
	assert.Equal(t, 0, transport.GetTotalCallCount())

	// Run and wait, first error is returned
	err := g.RunAndWait()
	assert.Error(t, err)
	assert.Equal(t, `request GET "https://example.com/foo" failed: 401 Unauthorized`, err.Error())

	// NOT all requests have been sent
	// Sending stops when the first error occurs
	assert.Less(t, transport.GetTotalCallCount(), 100)
}
