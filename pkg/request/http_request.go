package request

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strings"

	"go.opentelemetry.io/otel/trace"
)

// Result - any value.
type Result = any

// NoResult type.
type NoResult struct{}

// HTTPRequest is an immutable HTTP request.
type HTTPRequest interface {
	httpRequestReadOnly
	// WithGet is shortcut for WithMethod(http.MethodGet).WithURL(url)
	WithGet(url string) HTTPRequest
	// WithPost is shortcut for WithMethod(http.MethodPost).WithURL(url)
	WithPost(url string) HTTPRequest
	// WithPut is shortcut for WithMethod(http.MethodPut).WithURL(url)
	WithPut(url string) HTTPRequest
	// WithDelete is shortcut for WithMethod(http.MethodDelete).WithURL(url)
	WithDelete(url string) HTTPRequest
	// WithMethod method sets the HTTP method.
	WithMethod(method string) HTTPRequest
	// WithBaseURL method sets the base URL.
	WithBaseURL(baseURL string) HTTPRequest
	// WithURL method sets the URL.
	WithURL(url string) HTTPRequest
	// AndHeader method sets a single header field and its value.
	AndHeader(header string, value string) HTTPRequest
	// AndQueryParam method sets single parameter and its value.
	AndQueryParam(param, value string) HTTPRequest
	// WithQueryParams method sets multiple parameters and its values.
	WithQueryParams(params map[string]string) HTTPRequest
	// AndPathParam method sets single URL path key-value pair.
	AndPathParam(param, value string) HTTPRequest
	// WithPathParams method sets multiple URL path key-value pairs.
	WithPathParams(params map[string]string) HTTPRequest
	// WithFormBody method sets Form parameters and Content-Type header to "application/x-www-form-urlencoded".
	WithFormBody(form map[string]string) HTTPRequest
	// WithJSONBody method sets request body to the JSON value and Content-Type header to "application/json".
	WithJSONBody(body any) HTTPRequest
	// WithBody method sets request body.
	WithBody(body any) HTTPRequest
	// WithContentType method sets custom content type.
	WithContentType(contentType string) HTTPRequest
	// WithError method registers the request `Error` value for automatic mapping.
	WithError(err error) HTTPRequest
	// WithResult method registers the request `Result` value for automatic mapping.
	WithResult(result any) HTTPRequest
	// WithOnComplete method registers callback to be executed when the request is completed.
	WithOnComplete(func(ctx context.Context, response HTTPResponse, err error) error) HTTPRequest
	// WithOnSuccess method registers callback to be executed when the request is completed and `code >= 200 and <= 299`.
	WithOnSuccess(func(ctx context.Context, response HTTPResponse) error) HTTPRequest
	// WithOnError method registers callback to be executed when the request is completed and `code >= 400`.
	WithOnError(func(ctx context.Context, response HTTPResponse, err error) error) HTTPRequest
	// Send method sends defined request and returns response, mapped result and error.
	Send(ctx context.Context) (response HTTPResponse, result any, err error)
	SendOrErr(ctx context.Context) error
}

type httpRequestReadOnly interface {
	// Method returns HTTP method.
	Method() string
	// URL method returns HTTP URL.
	URL() *url.URL
	// RequestHeader method returns HTTP request headers.
	RequestHeader() http.Header
	// QueryParams method returns HTTP query parameters.
	QueryParams() url.Values
	// PathParams method returns HTTP path parameters mapped to a {placeholder} in the URL.
	PathParams() map[string]string
	// RequestBody method returns a definition of HTTP request body.
	// Supported request body data types are:
	// `*string`, `*[]byte`, `*struct`, `*map`, `*slice`, `io.ReadSeeker` and `io.ReadSeekCloser`.
	// Automatic marshaling for JSON is provided, if it is `*struct`, `*map`, or `*slice`.
	RequestBody() any
	// ErrorDef method returns a target value for error result mapping.
	ErrorDef() error
	// ResultDef method returns a target value for result mapping.
	ResultDef() any
}

// NewHTTPRequest creates immutable HTTP request.
func NewHTTPRequest(sender Sender) HTTPRequest {
	return httpRequest{sender: sender, header: make(http.Header)}
}

// httpRequest implements HTTPRequest interface.
type httpRequest struct {
	sender      Sender
	method      string
	baseURL     *url.URL
	url         *url.URL
	header      http.Header
	queryParams url.Values
	pathParams  map[string]string
	body        any
	resultDef   any
	errorDef    error
	listeners   []func(ctx context.Context, response HTTPResponse, err error) error
}

func (r httpRequest) Tracer() trace.Tracer {
	if tp, ok := r.sender.(withTracer); ok {
		return tp.Tracer()
	}
	return nil
}

func (r httpRequest) Method() string {
	if r.method == "" {
		panic(fmt.Errorf("request method is not set"))
	}
	return r.method
}

func (r httpRequest) URL() *url.URL {
	if r.url == nil {
		panic(fmt.Errorf("request url is not set"))
	}

	clone := *r.url
	outURL := &clone
	if r.baseURL != nil && !outURL.IsAbs() {
		outURL.Path = strings.TrimLeft(outURL.Path, "/")
		outURL = r.baseURL.ResolveReference(outURL)
	}

	return outURL
}

func (r httpRequest) RequestHeader() http.Header {
	return r.header
}

func (r httpRequest) QueryParams() url.Values {
	return r.queryParams
}

func (r httpRequest) PathParams() map[string]string {
	return r.pathParams
}

func (r httpRequest) RequestBody() any {
	return r.body
}

func (r httpRequest) ErrorDef() error {
	return r.errorDef
}

func (r httpRequest) ResultDef() any {
	return r.resultDef
}

func (r httpRequest) WithGet(url string) HTTPRequest {
	return r.WithMethod(http.MethodGet).WithURL(url)
}

func (r httpRequest) WithPost(url string) HTTPRequest {
	return r.WithMethod(http.MethodPost).WithURL(url)
}

func (r httpRequest) WithPut(url string) HTTPRequest {
	return r.WithMethod(http.MethodPut).WithURL(url)
}

func (r httpRequest) WithDelete(url string) HTTPRequest {
	return r.WithMethod(http.MethodDelete).WithURL(url)
}

func (r httpRequest) WithMethod(method string) HTTPRequest {
	r.method = method
	return r
}

func (r httpRequest) WithURL(urlStr string) HTTPRequest {
	if v, err := url.Parse(urlStr); err == nil {
		r.url = v
	} else {
		panic(fmt.Errorf(`url "%s" is not valid :%w`, urlStr, err))
	}
	return r
}

func (r httpRequest) WithBaseURL(baseURL string) HTTPRequest {
	if v, err := url.Parse(strings.TrimRight(baseURL, "/")); err == nil {
		// Normalize base URL, so r.baseURL.ResolveReference(...) will work
		v.Path = strings.TrimRight(v.Path, "/") + "/"
		r.baseURL = v
	} else {
		panic(fmt.Errorf(`base url "%s" is not valid :%w`, baseURL, err))
	}
	return r
}

func (r httpRequest) AndHeader(header string, value string) HTTPRequest {
	r.header = r.header.Clone()
	r.header.Set(header, value)
	return r
}

func (r httpRequest) AndQueryParam(key, value string) HTTPRequest {
	r.queryParams = cloneURLValues(r.queryParams)
	r.queryParams.Set(key, value)
	return r
}

func (r httpRequest) WithQueryParams(params map[string]string) HTTPRequest {
	r.queryParams = make(url.Values)
	for k, v := range params {
		r.queryParams.Set(k, v)
	}
	return r
}

func (r httpRequest) AndPathParam(key, value string) HTTPRequest {
	r.pathParams = cloneParams(r.pathParams)
	r.pathParams[key] = value
	return r
}

func (r httpRequest) WithPathParams(params map[string]string) HTTPRequest {
	r.pathParams = make(map[string]string)
	for k, v := range params {
		r.pathParams[k] = v
	}
	return r
}

func (r httpRequest) WithFormBody(form map[string]string) HTTPRequest {
	formData := make(url.Values)
	for k, v := range form {
		formData.Set(k, v)
	}
	r.body = formData.Encode()
	return r.AndHeader("Content-Type", "application/x-www-form-urlencoded")
}

func (r httpRequest) WithJSONBody(body any) HTTPRequest {
	r.body = body
	return r.AndHeader("Content-Type", "application/json")
}

func (r httpRequest) WithBody(body any) HTTPRequest {
	r.body = body
	return r
}

func (r httpRequest) WithContentType(contentType string) HTTPRequest {
	return r.AndHeader("Content-Type", contentType)
}

func (r httpRequest) WithError(err error) HTTPRequest {
	if reflect.ValueOf(err).Kind() != reflect.Ptr {
		panic(fmt.Errorf(`error must be defined by a pointer`))
	}
	r.errorDef = err
	return r
}

func (r httpRequest) WithResult(result any) HTTPRequest {
	_, ok1 := result.(io.Writer)
	_, ok2 := result.(io.WriteCloser)
	if !ok1 && !ok2 && reflect.ValueOf(result).Kind() != reflect.Ptr {
		panic(fmt.Errorf(`result must be defined by a pointer`))
	}
	r.resultDef = result
	return r
}

func (r httpRequest) WithOnComplete(fn func(ctx context.Context, response HTTPResponse, err error) error) HTTPRequest {
	r.listeners = append(r.listeners, fn)
	return r
}

func (r httpRequest) WithOnSuccess(fn func(ctx context.Context, response HTTPResponse) error) HTTPRequest {
	r.listeners = append(r.listeners, func(ctx context.Context, response HTTPResponse, err error) error {
		if err == nil {
			return fn(ctx, response)
		}
		return err
	})
	return r
}

func (r httpRequest) WithOnError(fn func(ctx context.Context, response HTTPResponse, err error) error) HTTPRequest {
	r.listeners = append(r.listeners, func(ctx context.Context, response HTTPResponse, err error) error {
		if err != nil {
			return fn(ctx, response, err)
		}
		return err
	})
	return r
}

func (r httpRequest) Send(ctx context.Context) (HTTPResponse, any, error) {
	// Stop if context has been cancelled
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}

	// Send request
	rawResponse, result, err := r.sender.Send(ctx, r)
	out := &httpResponse{httpRequest: r, rawResponse: rawResponse, result: result, err: err}

	// Invoke listeners
	for _, fn := range r.listeners {
		// Stop if context has been cancelled
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		out.err = fn(ctx, out, out.err)
	}

	return out, out.result, out.err
}

func (r httpRequest) SendOrErr(ctx context.Context) error {
	_, _, err := r.Send(ctx)
	return err
}
