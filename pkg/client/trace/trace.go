// Package trace extends the httptrace.ClientTrace and adds additional HTTPRequest hooks.
// A custom ClientTrace definition can be registered in the client.Client by the AndTrace method.
package trace

import (
	"context"
	"net/http"
	"net/http/httptrace"
	"reflect"
	"time"

	"github.com/keboola/go-client/pkg/request"
)

// Factory creates ClientTrace hooks for a request.
type Factory func(ctx context.Context, request request.HTTPRequest) (context.Context, *ClientTrace)

// ClientTrace is a set of hooks to run at various stages of an outgoing HTTPRequest.
type ClientTrace struct {
	httptrace.ClientTrace // native, low level trace
	// HTTPRequestStart is called when the request begins.
	// It is invoked one or more times, because it includes redirects and retries.
	HTTPRequestStart func(request *http.Request)
	// HTTPResponse is called when all headers and the HTTP status code is received.
	// The body stream have not been read yet!
	// It is invoked one or more times, because it includes redirects and retries.
	HTTPResponse func(response *http.Response, err error)
	// HTTPRequestDone is called when the request is completed and the response body is closed.
	// It is invoked one or more times, because it includes redirects and retries.
	HTTPRequestDone func(response *http.Response, read int64, err error)
	// RetryDelay is called before retry delay.
	RetryDelay func(attempt int, delay time.Duration)
	// BodyParseStart is called when the body parsing begins.
	BodyParseStart func(response *http.Response)
	// BodyParseDone is called when the body parsing completes.
	BodyParseDone func(response *http.Response, result any, err error, parseError error)
	// RequestProcessed is called when Client.Send method is done.
	// It is invoked only once after all redirects and retries.
	RequestProcessed func(result any, err error)
}

// Compose modifies t such that it respects the previously-registered hooks in old,
// subject to the composition policy requested in t.Compose.
// Copy of httptrace.compose.
func (t *ClientTrace) Compose(old *ClientTrace) {
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
			of.Call(args)
			return tfCopy.Call(args)
		})
		tv.Field(i).Set(newFunc)
	}
}
