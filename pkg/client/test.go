package client

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"os"
	"strings"
	"time"

	"github.com/jarcoal/httpmock"
)

var testTransport = DefaultTransport()

// NewTestClient creates the Client for tests.
//
// If the TEST_HTTP_CLIENT_VERBOSE environment variable is set to "true",
// then all HTTP requests and responses are dumped to stdout.
//
// Output may contain unmasked tokens, do not use it in production.
func NewTestClient() Client {
	return New().
		WithTransport(testTransport).
		WithTrace(func() *Trace {
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

// DumpTracer dumps HTTP request and response to a writer.
// Output may contain unmasked tokens, do not use it in production.
func DumpTracer(wr io.Writer) *Trace {
	var requestMethod, requestUri string
	var responseStatusCode int
	var requestDump []byte
	var responseErr error
	var startTime, headersTime time.Time

	t := &Trace{}
	t.HTTPRequestStart = func(r *http.Request) {
		startTime = time.Now()
		requestMethod = r.Method
		requestUri = r.URL.RequestURI()
		requestDump, _ = httputil.DumpRequestOut(r, true)
	}
	t.HTTPRequestDone = func(r *http.Response, err error) {
		// Response can be nil, for example, if some network error occurred
		if r != nil {
			responseStatusCode = r.StatusCode
			responseErr = err
			headersTime = time.Now()

			// Dump request of mocked responses
			if requestDump == nil {
				requestDump, _ = httputil.DumpRequestOut(r.Request, true)
			}
		}

		if err != nil {
			responseErr = err
		}

		// Dump request and response
		fmt.Fprintln(wr)
		fmt.Fprintln(wr, ">>>>>> HTTP DUMP")
		fmt.Fprintln(wr, strings.TrimSpace(string(requestDump)))
		fmt.Fprintln(wr, "------")
		if err != nil {
			fmt.Fprintln(wr, "ERROR: ", err)
		} else {
			responseDump, _ := httputil.DumpResponse(r, false)
			fmt.Fprintln(wr, strings.TrimSpace(string(responseDump)))
		}
		fmt.Fprintln(wr, "<<<<<< HTTP DUMP END")
		fmt.Fprintln(wr)
		fmt.Fprintln(wr)
	}
	t.HTTPRequestRetry = func(attempt int, delay time.Duration) {
		fmt.Fprintln(wr)
		fmt.Fprintln(wr, ">>>>>> HTTP RETRY", "| ATTEMPT:", attempt, "| DELAY:", delay, "| ", requestMethod, requestUri, responseStatusCode, "| ERROR:", responseErr)
		fmt.Fprintln(wr)
		fmt.Fprintln(wr)
	}
	t.RequestProcessed = func(result any, err error) {
		fmt.Fprintln(wr)
		fmt.Fprintln(wr, ">>>>>> HTTP REQUEST PROCESSED", "| ", requestMethod, requestUri, responseStatusCode, "| ERROR:", responseErr, "| HEADERS AT:", headersTime.Sub(startTime), "| DONE AT:", time.Since(startTime))
		fmt.Fprintln(wr)
		fmt.Fprintln(wr)
	}
	return t
}
