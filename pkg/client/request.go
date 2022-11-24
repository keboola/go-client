package client

import (
	"context"
	jsonlib "encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strings"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/spf13/cast"
)

// Result - any value.
type Result = any

// NoResult type.
type NoResult struct{}

// Sendable is HTTPRequest or APIRequest.
type Sendable interface {
	SendOrErr(ctx context.Context, sender Sender) error
}

// Sender represents an HTTP client, the Client is a default implementation using the standard net/http package.
type Sender interface {
	// Send method sends defined request and returns response.
	// Type of the return value "result" must be the same as type of the HTTPRequest.ResultDef(), otherwise panic will occur.
	//   In Go, this rule cannot be written using generic types yet, methods cannot have generic types.
	//   Send[R Result](ctx context.Context, request HTTPRequest[R]) (rawResponse *http.Response, result R, error error)
	Send(ctx context.Context, request HTTPRequest) (rawResponse *http.Response, result any, err error)
}

type httpRequestReadOnly interface {
	// Method returns HTTP method.
	Method() string
	// URL method returns HTTP URL.
	URL() string
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

type httpResponseCommon interface {
	// ResponseHeader method returns HTTP response headers.
	ResponseHeader() http.Header
	// StatusCode method returns HTTP status code.
	StatusCode() int
	// RawRequest method returns the standard HTTP request, from the last retry attempt.
	RawRequest() *http.Request
	// RawResponse method returns the standard HTTP response.
	RawResponse() *http.Response
	// IsSuccess method returns true if HTTP status `code >= 200 and <= 299` otherwise false.
	IsSuccess() bool
	// IsError method returns true if HTTP status `code >= 400` otherwise false.
	IsError() bool
	// Error method returns the error response mapped to a data type specified by ErrorDef if any.
	// It can also return native HTTP errors, e.g. some network problems.
	Error() error
}

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
	WithOnComplete(func(ctx context.Context, sender Sender, response HTTPResponse, err error) error) HTTPRequest
	// WithOnSuccess method registers callback to be executed when the request is completed and `code >= 200 and <= 299`.
	WithOnSuccess(func(ctx context.Context, sender Sender, response HTTPResponse) error) HTTPRequest
	// WithOnError method registers callback to be executed when the request is completed and `code >= 400`.
	WithOnError(func(ctx context.Context, sender Sender, response HTTPResponse, err error) error) HTTPRequest
	// Send method sends defined request and returns response, mapped result and error.
	Send(ctx context.Context, sender Sender) (response HTTPResponse, result any, err error)
	SendOrErr(ctx context.Context, sender Sender) error
}

// HTTPResponse with response mapped to the Result() value.
type HTTPResponse interface {
	httpRequestReadOnly
	httpResponseCommon
	// Result method returns the response mapped as a data type, if any.
	Result() any
}

// APIRequest with response mapped to the generic type R.
type APIRequest[R Result] interface {
	// WithBefore method registers callback to be executed before the request.
	// If an error is returned, the request is not sent.
	WithBefore(func(ctx context.Context, sender Sender) error) APIRequest[R]
	// WithOnComplete method registers callback to be executed when the request is completed.
	WithOnComplete(func(ctx context.Context, sender Sender, result R, err error) error) APIRequest[R]
	// WithOnSuccess method registers callback to be executed when the request is completed and `code >= 200 and <= 299`.
	WithOnSuccess(func(ctx context.Context, sender Sender, result R) error) APIRequest[R]
	// WithOnError method registers callback to be executed when the request is completed and `code >= 400`.
	WithOnError(func(ctx context.Context, sender Sender, err error) error) APIRequest[R]
	// Send sends the request by the sender.
	Send(ctx context.Context, sender Sender) (result R, err error)
	SendOrErr(ctx context.Context, sender Sender) error
}

// NewHTTPRequest creates immutable HTTP request.
func NewHTTPRequest() HTTPRequest {
	return httpRequest{header: make(http.Header)}
}

// NewAPIRequest creates an API request with the result mapped to the R type.
// It is composed of one or multiple Sendable (HTTPRequest or APIRequest).
func NewAPIRequest[R Result](result R, requests ...Sendable) APIRequest[R] {
	if len(requests) == 0 {
		panic(fmt.Errorf("at least one request must be provided"))
	}
	return &apiRequest[R]{requests: requests, result: result}
}

// NewNoOperationAPIRequest returns an APIRequest that immediately returns a Result without calling any HTTPRequest.
// It is handy in situations where there is no work to be done.
func NewNoOperationAPIRequest[R Result](result R) APIRequest[R] {
	return &apiRequest[R]{result: result}
}

// httpRequest implements HTTPRequest interface.
type httpRequest struct {
	method      string
	baseURL     *url.URL
	url         *url.URL
	header      http.Header
	queryParams url.Values
	pathParams  map[string]string
	body        any
	resultDef   any
	errorDef    error
	listeners   []func(ctx context.Context, sender Sender, response HTTPResponse, err error) error
}

// httpResponse implements HTTPResponse interface.
type httpResponse struct {
	httpRequest
	rawResponse *http.Response
	result      any
	err         error
}

// apiRequest implements generic APIRequest interface.
type apiRequest[R Result] struct {
	requests []Sendable
	before   []func(ctx context.Context, sender Sender) error
	after    []func(ctx context.Context, sender Sender, result R, err error) error
	result   R
}

func (r httpRequest) Method() string {
	if r.method == "" {
		panic(fmt.Errorf("request method is not set"))
	}
	return r.method
}

func (r httpRequest) URL() string {
	if r.url == nil {
		panic(fmt.Errorf("request url is not set"))
	}
	var outURL *url.URL
	if r.baseURL == nil {
		outURL = r.url
	} else if v, err := url.Parse(r.baseURL.String() + "/" + strings.TrimLeft(r.url.String(), "/")); err == nil {
		outURL = v
	} else {
		panic(fmt.Errorf(`cannot parse url: %w`, err))
	}
	return outURL.String()
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

func (r httpRequest) WithOnComplete(fn func(ctx context.Context, sender Sender, response HTTPResponse, err error) error) HTTPRequest {
	r.listeners = append(r.listeners, fn)
	return r
}

func (r httpRequest) WithOnSuccess(fn func(ctx context.Context, sender Sender, response HTTPResponse) error) HTTPRequest {
	r.listeners = append(r.listeners, func(ctx context.Context, sender Sender, response HTTPResponse, err error) error {
		if err == nil {
			return fn(ctx, sender, response)
		}
		return err
	})
	return r
}

func (r httpRequest) WithOnError(fn func(ctx context.Context, sender Sender, response HTTPResponse, err error) error) HTTPRequest {
	r.listeners = append(r.listeners, func(ctx context.Context, sender Sender, response HTTPResponse, err error) error {
		if err != nil {
			return fn(ctx, sender, response, err)
		}
		return err
	})
	return r
}

func (r httpRequest) Send(ctx context.Context, sender Sender) (HTTPResponse, any, error) {
	// Stop if context has been cancelled
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}

	// Send request
	rawResponse, result, err := sender.Send(ctx, r)
	out := &httpResponse{httpRequest: r, rawResponse: rawResponse, result: result, err: err}

	// Invoke listeners
	for _, fn := range r.listeners {
		// Stop if context has been cancelled
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		out.err = fn(ctx, sender, out, out.err)
	}

	return out, out.result, out.err
}

func (r httpRequest) SendOrErr(ctx context.Context, sender Sender) error {
	_, _, err := r.Send(ctx, sender)
	return err
}

func (r httpResponse) ResponseHeader() http.Header {
	return r.rawResponse.Header
}

func (r httpResponse) StatusCode() int {
	return r.rawResponse.StatusCode
}

func (r httpResponse) RawRequest() *http.Request {
	if r.rawResponse != nil && r.rawResponse.Request != nil {
		return r.rawResponse.Request
	}
	return nil
}

func (r httpResponse) RawResponse() *http.Response {
	return r.rawResponse
}

func (r httpResponse) IsSuccess() bool {
	return r.StatusCode() > 199 && r.StatusCode() < 300
}

func (r httpResponse) IsError() bool {
	return r.StatusCode() > 399
}

func (r httpResponse) Result() any {
	return r.result
}

func (r httpResponse) Error() error {
	return r.err
}

func (r apiRequest[R]) WithBefore(fn func(ctx context.Context, sender Sender) error) APIRequest[R] {
	r.before = append(r.before, fn)
	return r
}

func (r apiRequest[R]) WithOnComplete(fn func(ctx context.Context, sender Sender, result R, err error) error) APIRequest[R] {
	r.after = append(r.after, fn)
	return r
}

func (r apiRequest[R]) WithOnSuccess(fn func(ctx context.Context, sender Sender, result R) error) APIRequest[R] {
	r.after = append(r.after, func(ctx context.Context, sender Sender, result R, err error) error {
		if err == nil {
			err = fn(ctx, sender, result)
		}
		return err
	})
	return r
}

func (r apiRequest[R]) WithOnError(fn func(ctx context.Context, sender Sender, err error) error) APIRequest[R] {
	r.after = append(r.after, func(ctx context.Context, sender Sender, result R, err error) error {
		if err != nil {
			err = fn(ctx, sender, err)
		}
		return err
	})
	return r
}

func (r apiRequest[R]) Send(ctx context.Context, sender Sender) (result R, err error) {
	// Stop if context has been cancelled
	if err := ctx.Err(); err != nil {
		return r.result, err
	}

	// Invoke "before" listeners
	for _, fn := range r.before {
		if err := fn(ctx, sender); err != nil {
			return r.result, err
		}
	}

	// Stop if context has been cancelled
	if err := ctx.Err(); err != nil {
		return r.result, err
	}

	// Send requests in parallel
	wg := NewWaitGroup(ctx, sender)
	for _, request := range r.requests {
		wg.Send(request)
	}

	// Process error by listener, if any
	err = wg.Wait()

	// Invoke "after" listeners
	for _, fn := range r.after {
		// Stop if context has been cancelled
		if err := ctx.Err(); err != nil {
			return r.result, err
		}
		err = fn(ctx, sender, r.result, err)
	}

	return r.result, err
}

func (r apiRequest[R]) SendOrErr(ctx context.Context, sender Sender) error {
	_, err := r.Send(ctx, sender)
	return err
}

// ToFormBody converts a JSON like map to form body map, any type is mapped to string.
func ToFormBody(in map[string]any) (out map[string]string) {
	out = make(map[string]string)
	for k, v := range in {
		ty := reflect.TypeOf(v)
		if ty.Kind() == reflect.Slice {
			for i, s := range v.([]string) {
				out[fmt.Sprintf("%s[%d]", k, i)] = s
			}
		} else if ty.Kind() == reflect.Map && ty.Elem().Kind() == reflect.String {
			for i, s := range v.(map[string]string) {
				out[fmt.Sprintf("%s[%s]", k, i)] = s
			}
		} else {
			out[k] = castToString(v)
		}
	}
	return out
}

// StructToMap converts a struct to values map.
// Only defined allowedFields are converted.
// If allowedFields = nil, then all fields are exported.
//
// Field name is read from `writeas` tag or from "json" tag as fallback.
// Field with tag `readonly:"true"` is ignored.
// Field with tag `writeoptional` is exported only if value is not empty.
func StructToMap(in any, allowedFields []string) (out map[string]any) {
	out = make(map[string]any)
	structToMap(reflect.ValueOf(in), out, allowedFields)
	return out
}

func structToMap(in reflect.Value, out map[string]any, allowedFields []string) {
	// Initialize
	for in.Kind() == reflect.Ptr || in.Kind() == reflect.Interface {
		in = in.Elem()
	}
	t := in.Type()

	// Convert allowed slice to map
	allowed := make(map[string]bool)
	for _, field := range allowedFields {
		allowed[field] = true
	}

	// Iterate over fields
	numFields := t.NumField()
	for i := 0; i < numFields; i++ {
		field := t.Field(i)
		fieldValue := in.Field(i)

		// Process embedded type
		if field.Anonymous {
			structToMap(fieldValue, out, allowedFields)
			continue
		}

		// Skip filed with tag `readonly:"true"`
		if field.Tag.Get("readonly") == "true" {
			continue
		}

		// Skip field with tag `writeoptional:"true"` and empty value
		if field.Tag.Get("writeoptional") == "true" && fieldValue.IsZero() {
			continue
		}

		// Get field name
		var fieldName string
		if v := field.Tag.Get("writeas"); v != "" {
			fieldName = v
		} else if v := strings.Split(field.Tag.Get("json"), ",")[0]; v != "" {
			fieldName = v
		} else {
			panic(fmt.Errorf(`field "%s" of %s has no json name`, field.Name, t.String()))
		}

		// Skip ignored fields
		if fieldName == "-" {
			continue
		}

		// Is allowed?
		if len(allowedFields) > 0 && !allowed[fieldName] {
			continue
		}

		// Ok, add to map
		out[fieldName] = fieldValue.Interface()
	}
}

func cloneParams(in map[string]string) (out map[string]string) {
	out = make(map[string]string)
	for k, v := range in {
		out[k] = v
	}
	return out
}

func cloneURLValues(in url.Values) (out url.Values) {
	out = make(url.Values)
	for k, values := range in {
		for _, v := range values {
			out.Add(k, v)
		}
	}
	return out
}

func castToString(v any) string {
	// Ordered map
	if orderedMap, ok := v.(*orderedmap.OrderedMap); ok {
		// Standard json encoding library is used.
		// JsonIter lib returns non-compact JSON,
		// if custom OrderedMap.MarshalJSON method is used.
		if v, err := jsonlib.Marshal(orderedMap); err != nil {
			panic(fmt.Errorf(`cannot cast %T to string %w`, v, err))
		} else {
			return string(v)
		}
	}

	// Other types
	if v, err := cast.ToStringE(v); err != nil {
		panic(fmt.Errorf(`cannot cast %T to string %w`, v, err))
	} else {
		return v
	}
}
