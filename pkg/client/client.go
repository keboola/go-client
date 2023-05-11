// Package client provides support for sending requests defined by the request package.
//
// Client is a default implementation of the request.Sender interface.
// Client is based on the standard net/http package and contains retry and tracing/telemetry support.
// It is easy to implement your custom HTTP client, by implementing request.Sender interface.
package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/keboola/go-client/pkg/client/decode"
	"github.com/keboola/go-client/pkg/client/trace"
	"github.com/keboola/go-client/pkg/request"
)

const RetryAttemptContextKey = ContextKey("retryAttempt")

type ContextKey string

// Client is a default and configurable implementation of the Sender interface by Go native http.Client.
// It supports retry and tracing/telemetry.
type Client struct {
	transport      http.RoundTripper
	baseURL        *url.URL
	header         http.Header
	retry          RetryConfig
	traceFactories []trace.Factory
}

// New creates new HTTP Client.
func New() Client {
	c := Client{transport: DefaultTransport(), header: make(http.Header), retry: DefaultRetry()}
	c.header.Set("User-Agent", "keboola-go-client")
	c.header.Set("Accept-Encoding", "gzip, br")
	return c
}

// WithBaseURL returns a clone of the Client with base url set.
func (c Client) WithBaseURL(baseURLStr string) Client {
	baseURL, err := url.Parse(baseURLStr)
	if err != nil {
		panic(fmt.Errorf(`base url "%s" is not valid: %w`, baseURLStr, err))
	}
	// Normalize base URL, so r.baseURL.ResolveReference(...) will work
	baseURL.Path = strings.TrimRight(baseURL.Path, "/") + "/"
	c.baseURL = baseURL
	return c
}

// WithUserAgent returns a clone of the Client with user agent set.
func (c Client) WithUserAgent(v string) Client {
	c.header.Set("User-Agent", v)
	return c
}

// WithHeader returns a clone of the Client with common header set.
func (c Client) WithHeader(key, value string) Client {
	c.header = c.header.Clone()
	c.header.Set(key, value)
	return c
}

// WithHeaders returns a clone of the Client with common headers set.
func (c Client) WithHeaders(headers map[string]string) Client {
	c.header = c.header.Clone()
	for k, v := range headers {
		c.header.Set(k, v)
	}
	return c
}

// WithTransport returns a clone of the Client with a HTTP transport set.
func (c Client) WithTransport(transport http.RoundTripper) Client {
	if transport == nil || transport == http.RoundTripper(nil) {
		panic(fmt.Errorf("transport cannot be nil"))
	}
	c.transport = transport
	return c
}

// WithRetry returns a clone of the Client with retry config set.
func (c Client) WithRetry(retry RetryConfig) Client {
	c.retry = retry
	return c
}

// AndTrace returns a clone of the Client with Trace hooks added.
// The last registered hook is executed first.
func (c Client) AndTrace(fn trace.Factory) Client {
	c.traceFactories = append(c.traceFactories, fn)
	return c
}

// Send method sends HTTP request and returns HTTP response, it implements the Sender interface.
func (c Client) Send(ctx context.Context, reqDef request.HTTPRequest) (res *http.Response, result any, err error) {
	// Method cannot be called on an empty value
	if c.transport == nil {
		panic(fmt.Errorf("client value is not initialized"))
	}

	// If method or url is not set, panic occurs. So we get these values first.
	method := reqDef.Method()
	reqURL := reqDef.URL()

	// Init trace
	var tc *trace.ClientTrace
	for _, fn := range c.traceFactories {
		oldTrace := tc
		ctx, tc = fn(ctx, reqDef)
		tc.Compose(oldTrace)
	}

	// Replace path parameters
	for k, v := range reqDef.PathParams() {
		reqURL.Path = strings.ReplaceAll(reqURL.Path, "{"+k+"}", url.PathEscape(v))
	}

	// Convert to absolute url
	if c.baseURL != nil && !reqURL.IsAbs() {
		reqURL = c.baseURL.ResolveReference(reqURL)
	}
	if err != nil {
		return nil, nil, err
	}

	// Set query parameters
	reqURL.RawQuery = reqDef.QueryParams().Encode()

	// Create request
	req, err := http.NewRequestWithContext(ctx, method, reqURL.String(), nil)
	if err != nil {
		return nil, nil, err
	}

	// Global headers
	for k, values := range c.header {
		for _, v := range values {
			req.Header.Set(k, v)
		}
	}

	// Request headers
	for k, values := range reqDef.RequestHeader() {
		req.Header.Del(k) // clear global values
		for _, v := range values {
			req.Header.Add(k, v)
		}
	}

	// Body
	if reqDef.RequestBody() != nil {
		// GetBody factory is used for requests when a redirect/retry requires reading the body more than once.
		req.GetBody = func() (io.ReadCloser, error) {
			if body, err := requestBody(reqDef); err == nil {
				return body, nil
			} else {
				return nil, fmt.Errorf(`request %s "%s": cannot prepare request body: %w`, req.Method, req.URL.String(), err)
			}
		}
		req.Body, err = req.GetBody()
		if err != nil {
			return nil, nil, err
		}
	}

	// Setup native client
	nativeClient := http.Client{
		Timeout:   c.retry.TotalRequestTimeout,
		Transport: roundTripper{retry: c.retry, trace: tc, wrapped: c.transport}, // wrapped transport for trace/retry
	}

	// Send request
	startedAt := time.Now()
	res, err = nativeClient.Do(req)

	// Trace request processed (defer!)
	if tc != nil && tc.RequestProcessed != nil {
		defer func() {
			tc.RequestProcessed(result, err)
		}()
	}

	// Handle send error
	if err != nil {
		return nil, nil, handleSendError(startedAt, c.retry.TotalRequestTimeout, req, err)
	}

	// Parse body
	if tc != nil && tc.BodyParseStart != nil {
		tc.BodyParseStart(res)
	}
	var parseError error
	result, err, parseError = handleResponseBody(res, reqDef.ResultDef(), reqDef.ErrorDef())
	if tc != nil && tc.BodyParseDone != nil {
		tc.BodyParseDone(res, result, err, parseError)
	}
	if parseError != nil {
		// Unexpected error
		err = fmt.Errorf(`cannot process response body %s "%s": %w`, req.Method, req.URL.String(), parseError)
	}

	// Generic HTTP error
	if err == nil && res.StatusCode > 399 {
		return res, nil, fmt.Errorf(`request %s "%s" failed: %d %s`, req.Method, req.URL.String(), res.StatusCode, http.StatusText(res.StatusCode))
	}

	return res, result, err
}

func requestBody(r request.HTTPRequest) (io.ReadCloser, error) {
	contentType := r.RequestHeader().Get("Content-Type")
	body := r.RequestBody()
	if v, ok := body.(string); ok {
		return io.NopCloser(strings.NewReader(v)), nil
	}
	if v, ok := body.([]byte); ok {
		return io.NopCloser(bytes.NewReader(v)), nil
	}
	if v, ok := body.(io.ReadSeekCloser); ok {
		// io.ReadSeekCloser stream
		if _, err := v.Seek(0, io.SeekStart); err != nil {
			return nil, err
		}
		return v, nil
	}
	if v, ok := body.(io.ReadSeeker); ok {
		// io.ReadSeeker stream
		if _, err := v.Seek(0, io.SeekStart); err != nil {
			return nil, err
		}
		return io.NopCloser(v), nil
	}
	if body != nil && isJSONContentType(contentType) {
		// Json body
		c, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf(`cannot encode JSON body: %w`, err)
		}
		return io.NopCloser(bytes.NewReader(c)), nil
	}
	// empty body
	return nil, nil
}

func handleResponseBody(r *http.Response, resultDef any, errDef error) (result any, err error, parseError error) {
	defer r.Body.Close()

	if r.StatusCode == http.StatusNoContent {
		return nil, nil, nil
	}

	// Process content encoding
	decodedBody, err := decode.Decode(r.Body, r.Header.Get("Content-Encoding"))
	if err != nil {
		return nil, nil, fmt.Errorf("cannot decode response body: %w", err)
	}

	// Process content type, for example "application/json; charset=utf-8"
	contentType, _, _ := strings.Cut(r.Header.Get("Content-Type"), ";")
	contentType = strings.TrimSpace(contentType)
	if v, ok := resultDef.(*[]byte); ok {
		// Load response body as []byte
		bodyBytes, err := io.ReadAll(decodedBody)
		if err != nil {
			return nil, nil, fmt.Errorf(`cannot read resonse body: %w`, err)
		}
		*v = bodyBytes
		return v, nil, nil
	} else if v, ok := resultDef.(*string); ok {
		// Load response body as string
		bodyBytes, err := io.ReadAll(decodedBody)
		if err != nil {
			return nil, nil, fmt.Errorf(`cannot read resonse body: %w`, err)
		}
		*v = string(bodyBytes)
		return v, nil, nil
	} else if v, ok := resultDef.(io.WriteCloser); ok {
		// Stream response to io.WriteCloser
		if _, err := io.Copy(v, decodedBody); err != nil {
			return nil, nil, fmt.Errorf(`cannot read resonse body: %w`, err)
		}
		if err := v.Close(); err != nil {
			return nil, nil, fmt.Errorf(`cannot read resonse body: %w`, err)
		}
	} else if v, ok := resultDef.(io.Writer); ok {
		// Stream response to io.Writer
		if _, err := io.Copy(v, decodedBody); err != nil {
			return nil, nil, fmt.Errorf(`cannot read resonse body: %w`, err)
		}
	} else if isJSONContentType(contentType) {
		// Map JSON response
		if r.StatusCode > 199 && r.StatusCode < 300 && resultDef != nil {
			// Map JSON response to defined result
			if err := json.NewDecoder(decodedBody).Decode(resultDef); err != nil {
				return nil, nil, fmt.Errorf(`cannot decode JSON result: %w`, err)
			}
			return resultDef, nil, nil
		} else if r.StatusCode > 399 && errDef != nil {
			// Map JSON response to defined error
			if err := json.NewDecoder(decodedBody).Decode(errDef); err != nil {
				return nil, nil, fmt.Errorf(`cannot decode JSON error: %w`, err)
			}
			// Set HTTP request
			var errWithReq errorWithRequest
			if errors.As(errDef, &errWithReq) {
				errWithReq.SetRequest(r.Request)
				errDef = errWithReq
			}
			// Set HTTP response
			var errWithRes errorWithResponse
			if errors.As(errDef, &errWithRes) {
				errWithRes.SetResponse(r)
				errDef = errWithRes
			}
			return nil, errDef, nil
		}
	}
	return nil, nil, nil
}

func handleSendError(startedAt time.Time, clientTimeout time.Duration, req *http.Request, err error) error {
	// Timeout
	var netErr net.Error
	if deadline, ok := req.Context().Deadline(); ok && errors.Is(err, context.DeadlineExceeded) {
		err = urlError(req, fmt.Errorf("timeout after %s", deadline.Sub(startedAt)))
	} else if errors.Is(err, context.Canceled) {
		err = urlError(req, fmt.Errorf("canceled after %s", time.Since(startedAt)))
	} else if errors.As(err, &netErr) && netErr.Timeout() {
		if strings.Contains(err.Error(), "Client.Timeout exceeded") {
			err = urlError(req, fmt.Errorf("timeout after %s", clientTimeout))
		} else {
			err = urlError(req, fmt.Errorf("timeout after %s", time.Since(startedAt)))
		}
	}

	// Url error
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		err = fmt.Errorf(`request %s "%s" failed: %w`, strings.ToUpper(urlErr.Op), urlErr.URL, urlErr.Err)
	}

	return err
}

// roundTripper wraps a http.RoundTripper and adds trace and retry functionality.
type roundTripper struct {
	trace   *trace.ClientTrace
	retry   RetryConfig
	wrapped http.RoundTripper
}

func (rt roundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx := req.Context()
	state := rt.retry.NewBackoff()
	attempt := 0
	for {
		// Trace request start
		if rt.trace != nil && rt.trace.HTTPRequestStart != nil {
			rt.trace.HTTPRequestStart(req)
		}

		// Register low-level tracing
		if rt.trace != nil {
			req = req.WithContext(httptrace.WithClientTrace(ctx, &rt.trace.ClientTrace))
		}

		// Send
		res, err := rt.wrapped.RoundTrip(req)

		// Trace request done
		if rt.trace != nil && rt.trace.HTTPRequestDone != nil {
			rt.trace.HTTPRequestDone(res, err)
		}

		// Check if we should retry
		if rt.retry.Condition == nil || !rt.retry.Condition(res, err) || attempt >= rt.retry.Count {
			// No retry
			return res, err
		}

		// Get next delay
		delay := state.NextBackOff()
		if delay == backoff.Stop {
			// Stop
			return res, err
		}

		// Trace retry
		attempt++
		if rt.trace != nil && rt.trace.RetryDelay != nil {
			rt.trace.RetryDelay(attempt, delay)
		}

		// Set retry attempt to the request context
		req = req.WithContext(context.WithValue(req.Context(), RetryAttemptContextKey, attempt))

		// Rewind body before retry
		if req.GetBody != nil {
			req.Body, err = req.GetBody()
			if err != nil {
				return nil, fmt.Errorf("cannot rewind body: %w", err)
			}
		}

		// Wait
		select {
		case <-req.Context().Done():
			// context is canceled
			return nil, req.Context().Err()
		case <-time.After(delay):
			// time elapsed, retry
		}
	}
}

func ContextRetryAttempt(ctx context.Context) (int, bool) {
	v := ctx.Value(RetryAttemptContextKey)
	if v == nil {
		return 0, false
	}
	return v.(int), true
}

type errorWithRequest interface {
	error
	SetRequest(request *http.Request)
}

type errorWithResponse interface {
	error
	SetResponse(response *http.Response)
}

func urlError(req *http.Request, err error) *url.Error {
	return &url.Error{Op: req.Method, URL: req.URL.String(), Err: err}
}
