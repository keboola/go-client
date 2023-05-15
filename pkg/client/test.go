package client

import (
	"os"

	"github.com/jarcoal/httpmock"

	"github.com/keboola/go-client/pkg/client/trace"
)

var testTransport = DefaultTransport() //nolint:gochecknoglobals

// NewTestClient creates the Client for tests.
//
// If the TEST_HTTP_CLIENT_VERBOSE environment variable is set to "true",
// then all HTTP requests and responses are dumped to stdout.
//
// Output may contain unmasked tokens, do not use it in production!
func NewTestClient() Client {
	c := New().WithTransport(testTransport)
	if os.Getenv("TEST_HTTP_CLIENT_VERBOSE") == "true" { //nolint:forbidigo
		c = c.AndTrace(trace.DumpTracer(os.Stdout))
	}
	return c
}

// NewMockedClient creates the Client with mocked HTTP transport.
func NewMockedClient() (Client, *httpmock.MockTransport) {
	mockTransport := httpmock.NewMockTransport()
	return NewTestClient().WithTransport(mockTransport), mockTransport
}
