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
