package client_test

import (
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"

	. "github.com/keboola/go-client/pkg/client"
	. "github.com/keboola/go-client/pkg/client/trace"
	. "github.com/keboola/go-client/pkg/request"
)

func TestRetryCount(t *testing.T) {
	t.Parallel()

	// Mocked response
	transport := httpmock.NewMockTransport()
	transport.RegisterResponder("GET", `https://example.com`, httpmock.NewStringResponder(504, "test"))

	// Setup
	retryCount := 10
	var delays []time.Duration

	// Create client
	ctx := context.Background()
	c := New().
		WithTransport(transport).
		WithRetry(RetryConfig{
			Condition:     DefaultRetryCondition(),
			Count:         retryCount,
			WaitTimeStart: 1 * time.Microsecond,
			WaitTimeMax:   20 * time.Microsecond,
		}).
		AndTrace(func() *ClientTrace {
			return &ClientTrace{
				HTTPRequestRetry: func(_ int, delay time.Duration) {
					delays = append(delays, delay)
				},
			}
		})

	// Get
	_, _, err := NewHTTPRequest(c).
		WithGet("https://example.com").
		WithOnComplete(func(ctx context.Context, response HTTPResponse, err error) error {
			// Check context
			attempt, found := ContextRetryAttempt(response.RawRequest().Context())
			assert.True(t, found)
			assert.Equal(t, retryCount, attempt)
			return err
		}).
		Send(ctx)
	assert.Error(t, err)
	assert.Equal(t, `request GET "https://example.com" failed: 504 Gateway Timeout`, err.Error())

	// Check number of requests
	assert.Equal(t, 1+retryCount, transport.GetCallCountInfo()["GET https://example.com"])

	// Check delays
	assert.Equal(t, []time.Duration{
		1 * time.Microsecond,
		2 * time.Microsecond,
		4 * time.Microsecond,
		8 * time.Microsecond,
		16 * time.Microsecond,
		20 * time.Microsecond,
		20 * time.Microsecond,
		20 * time.Microsecond,
		20 * time.Microsecond,
		20 * time.Microsecond,
	}, delays)
}

func TestRetryBodyRewind(t *testing.T) {
	t.Parallel()

	// Mocked response
	transport := httpmock.NewMockTransport()
	transport.RegisterResponder("POST", `https://example.com`, func(req *http.Request) (*http.Response, error) {
		requestBody, err := io.ReadAll(req.Body)
		assert.NoError(t, err)
		// Each retry attempt must send same body
		assert.Equal(t, `{"foo":"bar"}`, string(requestBody))
		return httpmock.NewStringResponse(502, "retry!"), nil
	})

	// Create client
	ctx := context.Background()
	c := New().
		WithTransport(transport).
		WithRetry(TestingRetry())

	// Post
	jsonBody := map[string]any{"foo": "bar"}
	_, _, err := NewHTTPRequest(c).WithPost("https://example.com").WithJSONBody(jsonBody).Send(ctx)
	assert.Error(t, err)
	assert.Equal(t, `request POST "https://example.com" failed: 502 Bad Gateway`, err.Error())

	// Check number of requests
	assert.Equal(t, 1+5, transport.GetCallCountInfo()["POST https://example.com"])
}

func TestDoNotRetry(t *testing.T) {
	t.Parallel()

	// Mocked response
	transport := httpmock.NewMockTransport()
	transport.RegisterResponder("GET", "https://example.com", httpmock.NewStringResponder(403, "test"))

	// Setup
	var delays []time.Duration

	// Create client
	ctx := context.Background()
	c := New().
		WithTransport(transport).
		WithRetry(RetryConfig{
			Condition:     DefaultRetryCondition(),
			Count:         10,
			WaitTimeStart: 1 * time.Microsecond,
			WaitTimeMax:   20 * time.Microsecond,
		}).
		AndTrace(func() *ClientTrace {
			return &ClientTrace{
				HTTPRequestRetry: func(_ int, delay time.Duration) {
					delays = append(delays, delay)
				},
			}
		})

	// Get
	_, _, err := NewHTTPRequest(c).WithGet("https://example.com").Send(ctx)
	assert.Error(t, err)
	assert.Equal(t, `request GET "https://example.com" failed: 403 Forbidden`, err.Error())

	// Check number of requests
	assert.Equal(t, 1, transport.GetCallCountInfo()["GET https://example.com"])

	// Check delays
	assert.Empty(t, delays)
}
