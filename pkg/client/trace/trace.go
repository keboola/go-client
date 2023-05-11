// Package trace extends the httptrace.ClientTrace and adds additional HTTPRequest hooks.
// A custom ClientTrace definition can be registered in the client.Client by the AndTrace method.
package trace

import (
	"context"
	"github.com/keboola/go-client/pkg/request"
	"net/http"
	"net/http/httptrace"
	"reflect"
	"time"
)

// Factory creates ClientTrace hooks for a request.
type Factory func(ctx context.Context, request request.HTTPRequest) (context.Context, *ClientTrace)

// ClientTrace is a set of hooks to run at various stages of an outgoing HTTPRequest.
type ClientTrace struct {
	httptrace.ClientTrace // native, low level trace
	// HTTPRequestStart is called when the request begins. It includes redirects and retries.
	HTTPRequestStart func(request *http.Request)
	// HTTPRequestStart is called when the request completes. It includes redirects and retries.
	HTTPRequestDone func(response *http.Response, err error)
	// HttpRequestRetry is called before retry delay.
	HTTPRequestRetry func(attempt int, delay time.Duration)
	// RequestProcessed is called when Client.Send method is done.
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
