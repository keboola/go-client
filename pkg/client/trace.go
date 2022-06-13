package client

import (
	"net/http"
	"net/http/httptrace"
	"time"
)

// Trace is a set of hooks to run at various stages of an outgoing HTTPRequest.
type Trace struct {
	httptrace.ClientTrace // native, low level trace
	// GotRequest is called when Client.Send method is called.
	GotRequest func(request HTTPRequest)
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
