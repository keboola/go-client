package client

import (
	"os"

	"github.com/jarcoal/httpmock"
)

var testTransport = DefaultTransport()

// NewTestClient creates the Client for tests.
//
// If the TEST_HTTP_CLIENT_VERBOSE environment variable is set to "true",
// then all HTTP requests and responses are dumped to stdout.
//
// Output may contain unmasked tokens, do not use it in production!
func NewTestClient() Client {
	return New().
		WithTransport(testTransport).
		AndTrace(func() *Trace {
			if os.Getenv("TEST_HTTP_CLIENT_VERBOSE") == "true" {
				return DumpTracer(os.Stdout)
			}
			return nil
		})
}

// NewMockedClient creates the Client with mocked HTTP transport.
func NewMockedClient() (Client, *httpmock.MockTransport) {
	mockTransport := httpmock.NewMockTransport()
	return NewTestClient().WithTransport(mockTransport), mockTransport
}
