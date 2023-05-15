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

func TestDumpTracer(t *testing.T) {
	t.Parallel()

	// Mocked response
	transport := httpmock.NewMockTransport()
	transport.RegisterResponder("GET", `https://example.com`, httpmock.ResponderFromMultipleResponses([]*http.Response{
		{StatusCode: http.StatusLocked},
		{StatusCode: http.StatusTooManyRequests},
		{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("OK"))},
	}))

	// Logs for trace testing
	var logs strings.Builder

	// Create client
	ctx := context.Background()
	c := client.New().
		WithTransport(transport).
		WithRetry(client.TestingRetry()).
		AndTrace(trace.DumpTracer(&logs))

	// Expected trace
	expected := `
>>>>>> HTTP DUMP
GET / HTTP/1.1
Host: example.com
User-Agent: keboola-go-client
Accept-Encoding: gzip, br
------
HTTP/0.0 423 Locked
Content-Length: 0
<<<<<< HTTP DUMP END

>>>>>> HTTP RETRY | ATTEMPT: 1 | DELAY: 1ms |  GET / 423 | ERROR: <nil>

>>>>>> HTTP DUMP
GET / HTTP/1.1
Host: example.com
User-Agent: keboola-go-client
Accept-Encoding: gzip, br
------
HTTP/0.0 429 Too Many Requests
Content-Length: 0
<<<<<< HTTP DUMP END

>>>>>> HTTP RETRY | ATTEMPT: 2 | DELAY: 1ms |  GET / 429 | ERROR: <nil>

>>>>>> HTTP DUMP
GET / HTTP/1.1
Host: example.com
User-Agent: keboola-go-client
Accept-Encoding: gzip, br
------
HTTP/0.0 200 OK
Content-Length: 0
------
OK
<<<<<< HTTP DUMP END

>>>>>> HTTP REQUEST PROCESSED |  GET / 200 | ERROR: <nil> | HEADERS AT: %s | DONE AT: %s
`

	// Test
	str := ""
	_, result, err := request.NewHTTPRequest(c).WithGet("https://example.com").WithResult(&str).Send(ctx)
	assert.NoError(t, err)
	assert.Equal(t, "OK", *result.(*string))
	wildcards.Assert(t, strings.TrimLeft(expected, "\n"), logs.String())
}
