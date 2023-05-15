package trace_test

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/client/trace"
	"github.com/keboola/go-client/pkg/request"
)

func TestLogTracer(t *testing.T) {
	t.Parallel()

	// Mocked response
	transport := httpmock.NewMockTransport()
	transport.RegisterResponder("GET", `https://example.com`, httpmock.ResponderFromMultipleResponses([]*http.Response{
		{StatusCode: http.StatusLocked},
		{StatusCode: http.StatusTooManyRequests},
		{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("OK1"))},
		{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("OK2"))},
	}))

	// Logs for trace testing
	var logs strings.Builder

	// Create client
	ctx := context.Background()
	c := client.New().
		WithTransport(transport).
		WithRetry(client.TestingRetry()).
		AndTrace(trace.LogTracer(&logs))

	// Expected trace
	expected := `
HTTP_REQUEST[0001] START GET "https://example.com"
HTTP_REQUEST[0001] DONE  GET "https://example.com" | 423 | %s
HTTP_REQUEST[0001] RETRY GET "https://example.com" | 1x | 1ms
HTTP_REQUEST[0001] START GET "https://example.com"
HTTP_REQUEST[0001] DONE  GET "https://example.com" | 429 | %s
HTTP_REQUEST[0001] RETRY GET "https://example.com" | 2x | 1ms
HTTP_REQUEST[0001] START GET "https://example.com"
HTTP_REQUEST[0001] DONE  GET "https://example.com" | 200 | %s
HTTP_REQUEST[0001] BODY  GET "https://example.com" | %s
HTTP_REQUEST[0002] START GET "https://example.com"
HTTP_REQUEST[0002] DONE  GET "https://example.com" | 200 | %s
HTTP_REQUEST[0002] BODY  GET "https://example.com" | %s
`

	// Test
	str := ""
	_, result, err := request.NewHTTPRequest(c).WithGet("https://example.com").WithResult(&str).Send(ctx)
	assert.NoError(t, err)
	assert.Equal(t, "OK1", *result.(*string))
	_, result, err = request.NewHTTPRequest(c).WithGet("https://example.com").WithResult(&str).Send(ctx)
	assert.NoError(t, err)
	assert.Equal(t, "OK2", *result.(*string))
	wildcards.Assert(t, strings.TrimLeft(expected, "\n"), logs.String())
}
