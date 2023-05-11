package trace

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"os"
	"strings"
	"time"

	"github.com/keboola/go-client/pkg/client/decode"
)

const dumpTraceMaxLength = 2000

type dumpTrace struct {
	ClientTrace
	wr io.Writer
}

// DumpTracer dumps HTTP request and response to a writer.
// Output may contain unmasked tokens, do not use it in production!
func DumpTracer(wr io.Writer) Factory {
	return func() *ClientTrace {
		var requestMethod, requestURI string
		var responseStatusCode int
		var requestDump []byte
		var responseErr error
		var startTime, headersTime time.Time

		t := &dumpTrace{wr: wr}
		t.HTTPRequestStart = func(r *http.Request) {
			startTime = time.Now()
			requestMethod = r.Method
			requestURI = r.URL.RequestURI()
			requestDump, _ = httputil.DumpRequestOut(r, true)
		}
		t.HTTPRequestDone = func(r *http.Response, err error) {
			// Response can be nil, for example, if some network error occurred
			if r != nil {
				responseStatusCode = r.StatusCode
				responseErr = err
				headersTime = time.Now()
			}
			if err != nil {
				responseErr = err
			}

			// Dump request
			t.log()
			t.log(">>>>>> HTTP DUMP")
			t.dump(string(requestDump))

			// Dump response
			t.log("------")
			if err != nil {
				t.log("ERROR: ", err)
			} else {
				// Dump response headers
				if v, err := httputil.DumpResponse(r, false); err == nil {
					t.log(strings.TrimSpace(string(v)))
				} else {
					t.log("cannot dump response headers: ", err)
				}
				// Dump response body
				if r.Body != nil {
					// Decode body and copy raw body to rawBody buffer
					var rawBody bytes.Buffer
					var decodedBody strings.Builder
					bodyReader, err := decode.Decode(io.NopCloser(io.TeeReader(r.Body, &rawBody)), r.Header.Get("Content-Encoding"))
					if err != nil {
						t.log("cannot read response body: ", err)
					}
					if _, err := io.Copy(&decodedBody, bodyReader); err != nil {
						t.log("cannot read response body: ", err)
					}
					// Set buffered raw body back to the response
					r.Body = io.NopCloser(bytes.NewReader(rawBody.Bytes()))
					// Dump decoded response
					t.log("------")
					t.dump(decodedBody.String())
				}
			}
			t.log("<<<<<< HTTP DUMP END")
		}
		t.HTTPRequestRetry = func(attempt int, delay time.Duration) {
			t.log()
			t.log(">>>>>> HTTP RETRY", "| ATTEMPT:", attempt, "| DELAY:", delay, "| ", requestMethod, requestURI, responseStatusCode, "| ERROR:", responseErr)
		}
		t.RequestProcessed = func(result any, err error) {
			t.log()
			t.log(">>>>>> HTTP REQUEST PROCESSED", "| ", requestMethod, requestURI, responseStatusCode, "| ERROR:", responseErr, "| HEADERS AT:", headersTime.Sub(startTime), "| DONE AT:", time.Since(startTime))
		}
		return &t.ClientTrace
	}
}

func (t *dumpTrace) dump(body string) {
	body = strings.TrimSpace(body)
	if len(body) > dumpTraceMaxLength && os.Getenv("HTTP_DUMP_TRACE_FULL") != "true" { //nolint:forbidigo
		t.log(body[:dumpTraceMaxLength])
		t.log("... (set env HTTP_DUMP_TRACE_FULL=true to see full output)")
	} else {
		t.log(body)
	}
}

func (t *dumpTrace) log(a ...any) {
	_, _ = fmt.Fprintln(t.wr, a...)
}
