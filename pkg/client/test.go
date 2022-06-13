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

// NewTestClient creates HTTP client for tests.
// If TEST_HTTP_CLIENT_VERBOSE environment variable is set to "true", then all HTTP requests and responses are dumped to stdout.
// Output contains unmasked tokens, do not use TEST_HTTP_CLIENT_VERBOSE in production.
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

func NewMockedClient() (Client, *httpmock.MockTransport) {
	mockTransport := httpmock.NewMockTransport()
	return NewTestClient().WithTransport(mockTransport), mockTransport
}

// DumpTracer dumps HTTP request and response to a writer.
// Output contains unmasked tokens, do not use it in production.
func DumpTracer(wr io.Writer) *Trace {
	var req, res []byte
	var startTime, headersTime, bodyTime time.Time
	t := &Trace{}
	t.HTTPRequestStart = func(r *http.Request) {
		startTime = time.Now()
		req, _ = httputil.DumpRequestOut(r, true)
	}
	t.HTTPRequestDone = func(r *http.Response, err error) {
		if err == nil {
			headersTime = time.Now()
			res, _ = httputil.DumpResponse(r, true)
			bodyTime = time.Now()
		}
	}
	t.RequestProcessed = func(result any, err error) {
		fmt.Fprintln(wr)
		fmt.Fprintln(wr, ">>>>>> HTTP DUMP")
		fmt.Fprintln(wr, strings.TrimSpace(string(req)))
		fmt.Fprintln(wr, "------")
		if err != nil {
			fmt.Fprintln(wr, "ERROR: ", err)
			fmt.Fprintln(wr, "<<<<<< HTTP DUMP END")
			fmt.Fprintln(wr)
			fmt.Fprintln(wr)
		} else {
			fmt.Fprintln(wr, strings.TrimSpace(string(res)))
			fmt.Fprintln(wr, "<<<<<< HTTP DUMP END, ", "HEADERS AT:", headersTime.Sub(startTime), "BODY AT:", bodyTime.Sub(startTime), ", DONE AT:", time.Since(startTime))
			fmt.Fprintln(wr)
			fmt.Fprintln(wr)
		}
	}
	return t
}
