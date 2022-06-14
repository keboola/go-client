package client_test

import (
	"context"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/keboola/go-client/pkg/client"
	"github.com/stretchr/testify/assert"
)

func TestRunGroup(t *testing.T) {
	t.Parallel()
	c, transport := client.NewMockedClient()
	c = c.WithBaseURL("https://example.com")
	transport.RegisterResponder("GET", `=~^https://example.com/`, httpmock.NewStringResponder(200, "OK"))

	// Create run group
	g := client.NewRunGroup(context.Background(), c)

	// Add requests
	g.Add(client.NewHTTPRequest().WithGet("foo1"))
	g.Add(client.NewHTTPRequest().WithGet("foo2"))
	g.Add(client.
		NewHTTPRequest().
		WithGet("foo3").
		WithOnSuccess(func(ctx context.Context, sender client.Sender, response client.HTTPResponse) error {
			g.Add(client.NewHTTPRequest().WithGet("foo5"))
			return nil
		}).
		WithOnError(func(ctx context.Context, sender client.Sender, response client.HTTPResponse, err error) error {
			g.Add(client.NewHTTPRequest().WithGet("err"))
			return err
		}),
	)
	g.Add(client.
		NewHTTPRequest().
		WithGet("foo4").
		WithOnSuccess(func(ctx context.Context, sender client.Sender, response client.HTTPResponse) error {
			g.Add(client.NewHTTPRequest().WithGet("foo6"))
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
	g := client.NewRunGroup(context.Background(), c)

	// Add requests
	requestsCount := 100
	assert.Greater(t, requestsCount, client.RunGroupConcurrencyLimit)
	for i := 1; i <= requestsCount; i++ {
		g.Add(client.NewHTTPRequest().WithGet("foo"))
	}

	// No requests have been sent yet
	assert.Equal(t, 0, transport.GetTotalCallCount())

	// Run and wait, first error returned
	err := g.RunAndWait()
	assert.Error(t, err)
	assert.Equal(t, `request GET "https://example.com/foo" failed: 401 Unauthorized`, err.Error())

	// NOT all requests have been sent
	// Sending stops when first error occurs
	assert.Less(t, transport.GetTotalCallCount(), 100)
}
