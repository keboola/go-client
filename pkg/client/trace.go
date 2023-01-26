package client

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"net/http/httputil"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"time"
)

const dumpTraceMaxLength = 2000

// Trace is a set of hooks to run at various stages of an outgoing HTTPRequest.
type Trace struct {
	httptrace.ClientTrace // native, low level trace
	// GotRequest is called when Client.Send method is called.
	GotRequest func(ctx context.Context, request HTTPRequest) context.Context
	// RequestProcessed is called when Client.Send method is done.
	RequestProcessed func(result any, err error)
	// HTTPRequestStart is called when the request begins. It includes redirects and retries.
	HTTPRequestStart func(request *http.Request)
	// HTTPRequestStart is called when the request completes. It includes redirects and retries.
	HTTPRequestDone func(response *http.Response, err error)
	// HttpRequestRetry is called before retry delay.
	HTTPRequestRetry func(attempt int, delay time.Duration)
}

// TraceFactory creates Trace hooks for a request.
type TraceFactory func() *Trace

// compose modifies t such that it respects the previously-registered hooks in old,
// subject to the composition policy requested in t.Compose.
// Copy of httptrace.compose.
func (t *Trace) compose(old *Trace) {
	if old == nil {
		return
	}
	tv := reflect.ValueOf(t).Elem()
	ov := reflect.ValueOf(old).Elem()
	structType := tv.Type()
	for i := 0; i < structType.NumField(); i++ {
		tf := tv.Field(i)
		hookType := tf.Type()
		if hookType.Kind() != reflect.Func {
			continue
		}
		of := ov.Field(i)
		if of.IsNil() {
			continue
		}
		if tf.IsNil() {
			tf.Set(of)
			continue
		}

		// Make a copy of tf for tf to call. (Otherwise it
		// creates a recursive call cycle and stack overflows)
		tfCopy := reflect.ValueOf(tf.Interface())

		// We need to call both tf and of in some order.
		newFunc := reflect.MakeFunc(hookType, func(args []reflect.Value) []reflect.Value {
			tfCopy.Call(args)
			return of.Call(args)
		})
		tv.Field(i).Set(newFunc)
	}
}

type logTrace struct {
	Trace
	wr io.Writer
}

func LogTracer(wr io.Writer) TraceFactory {
	var idGenerator uint64
	return func() *Trace {
		requestID := atomic.AddUint64(&idGenerator, 1)

		var request *http.Request
		var connStartTime time.Time
		var startTime time.Time
		var doneTime time.Time
		var statusCode int

		t := &logTrace{wr: wr}
		t.ConnectStart = func(network, addr string) {
			connStartTime = time.Now()
		}
		t.GotConn = func(info httptrace.GotConnInfo) {
			var infoStr string
			if info.Reused {
				if info.WasIdle {
					infoStr = "reused conn"
				} else {
					infoStr = fmt.Sprintf("reused conn (was idle=%s)", info.IdleTime)
				}
			} else {
				infoStr = fmt.Sprintf("new conn | %s", time.Since(connStartTime))
			}
			t.log(requestID, fmt.Sprintf(`CONN  %s "%s" | %s`, request.Method, request.URL.String(), infoStr))
		}
		t.HTTPRequestStart = func(r *http.Request) {
			request = r
			startTime = time.Now()
			t.log(requestID, fmt.Sprintf(`START %s "%s"`, request.Method, request.URL.String()))
		}
		t.HTTPRequestDone = func(r *http.Response, err error) {
			doneTime = time.Now()
			var errorStr string
			if err == nil {
				statusCode = r.StatusCode
			} else {
				errorStr = fmt.Sprintf(" | error=%s", err)
			}
			t.log(requestID, fmt.Sprintf(`DONE  %s "%s" | %d | %s%s`, request.Method, request.URL.String(), statusCode, doneTime.Sub(startTime).String(), errorStr))
		}
		t.HTTPRequestRetry = func(attempt int, delay time.Duration) {
			t.log(requestID, fmt.Sprintf(`RETRY %s "%s" | %dx | %s`, request.Method, request.URL.String(), attempt, delay))
		}
		t.RequestProcessed = func(result any, err error) {
			var errorStr string
			if err != nil {
				errorStr = fmt.Sprintf(" | error=%s", err)
			}
			t.log(requestID, fmt.Sprintf(`BODY  %s "%s" | %s%s`, request.Method, request.URL.String(), time.Since(doneTime).String(), errorStr))
		}
		return &t.Trace
	}
}

func (t *logTrace) log(requestID uint64, a ...any) {
	a = append([]any{fmt.Sprintf("HTTP_REQUEST[%04d]", requestID)}, a...)
	fmt.Fprintln(t.wr, a...)
}

type dumpTrace struct {
	Trace
	wr io.Writer
}

// DumpTracer dumps HTTP request and response to a writer.
// Output may contain unmasked tokens, do not use it in production!
func DumpTracer(wr io.Writer) TraceFactory {
	return func() *Trace {
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
					bodyReader, err := decodeBody(io.NopCloser(io.TeeReader(r.Body, &rawBody)), r.Header.Get("Content-Encoding"))
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
		return &t.Trace
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
	fmt.Fprintln(t.wr, a...)
}
