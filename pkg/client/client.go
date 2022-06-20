// Package client provides support for defining an HTTP client for an API.
//
// Use HTTPRequest interface to define immutable HTTP requests, see NewHTTPRequest function.
// Requests are sent using the Sender interface.
//
// Client is a default implementation of the Sender interface.
// Client is based on the standard net/http package and contains retry and tracing/telemetry support.
// It is easy to implement your custom HTTP client, by implementing Sender interface.
//
// APIRequest[R Result] is a generic type that contains
// target data type to which the API response will be mapped.
// Use NewAPIRequest function to create a APIRequest from a HTTPRequest.
//
// RunGroup and WaitGroup are helpers for concurrent requests.
package client

import (
	"bytes"
	"compress/gzip"
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

	"github.com/andybalholm/brotli"
	"github.com/cenkalti/backoff/v4"
)

// Client is a default and configurable implementation of the Sender interface by Go native http.Client.
// It supports retry and tracing/telemetry.
type Client struct {
	transport    http.RoundTripper
	baseURL      *url.URL
	header       http.Header
	retry        RetryConfig
	traceFactory TraceFactory
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

// WithTrace returns a clone of the Client with Trace hooks set.
func (c Client) WithTrace(fn TraceFactory) Client {
	c.traceFactory = fn
	return c
}

// Send method sends HTTP request and returns HTTP response, it implements the Sender interface.
func (c Client) Send(ctx context.Context, reqDef HTTPRequest) (res *http.Response, result any, err error) {
	// Method cannot be called on an empty value
	if c.transport == nil {
		panic(fmt.Errorf("client value is not initialized"))
	}

	// If method or url is not set, panic occurs. So we get these values first.
	method := reqDef.Method()
	reqURLStr := reqDef.URL()

	// Init trace
	var trace *Trace
	if c.traceFactory != nil {
		trace = c.traceFactory()
		if trace != nil {
			ctx = httptrace.WithClientTrace(ctx, &trace.ClientTrace)
		}
	}

	// Trace got request
	if trace != nil && trace.GotRequest != nil {
		trace.GotRequest(reqDef)
	}

	// Replace path parameters
	for k, v := range reqDef.PathParams() {
		reqURLStr = strings.ReplaceAll(reqURLStr, url.PathEscape("{"+k+"}"), url.PathEscape(v))
	}

	// Convert to absolute url
	var reqURL *url.URL
	if c.baseURL == nil {
		reqURL, err = url.Parse(reqURLStr)
	} else {
		reqURL, err = c.baseURL.Parse(reqURLStr)
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
		Transport: roundTripper{ctx: ctx, retry: c.retry, trace: trace, wrapped: c.transport}, // wrapped transport for trace/retry
	}

	// Send request
	startedAt := time.Now()
	res, err = nativeClient.Do(req)

	// Trace request processed
	if trace != nil && trace.RequestProcessed != nil {
		defer func() {
			trace.RequestProcessed(result, err)
		}()
	}

	// Handle send error
	if err != nil {
		return nil, nil, handleSendError(startedAt, c.retry.TotalRequestTimeout, req, err)
	}

	// Process body
	if r, e, unexpectedErr := handleResponseBody(res, reqDef.ResultDef(), reqDef.ErrorDef()); unexpectedErr == nil {
		// No unexpected error, set result/error result
		result, err = r, e
	} else {
		// Unexpected error
		err = fmt.Errorf(`cannot process request %s "%s": %w`, req.Method, req.URL.String(), unexpectedErr)
	}

	// Generic HTTP error
	if err == nil && res.StatusCode > 399 {
		return res, nil, fmt.Errorf(`request %s "%s" failed: %d %s`, req.Method, req.URL.String(), res.StatusCode, http.StatusText(res.StatusCode))
	}

	return res, result, err
}

func requestBody(r HTTPRequest) (io.ReadCloser, error) {
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
	if body != nil && contentType == "application/json" {
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

func handleResponseBody(r *http.Response, resultDef any, errDef error) (result any, err error, unexpectedErr error) {
	defer r.Body.Close()

	if r.StatusCode == http.StatusNoContent {
		return nil, nil, nil
	}

	// Process content encoding
	contentEncoding := strings.ToLower(r.Header.Get("Content-Encoding"))
	switch contentEncoding {
	case "gzip":
		if v, err := gzip.NewReader(r.Body); err == nil {
			r.Body = v
		} else {
			return nil, nil, fmt.Errorf("cannot decode gzip response: %w", err)
		}
	case "br":
		r.Body = io.NopCloser(brotli.NewReader(r.Body))
	}

	// Process content type
	contentType := r.Header.Get("Content-Type")
	if v, ok := resultDef.(*[]byte); ok {
		// Load response body as []byte
		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			return nil, nil, fmt.Errorf(`cannot read resonse body: %w`, err)
		}
		*v = bodyBytes
		return v, nil, nil

	} else if v, ok := resultDef.(*string); ok {
		// Load response body as string
		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			return nil, nil, fmt.Errorf(`cannot read resonse body: %w`, err)
		}
		*v = string(bodyBytes)
		return v, nil, nil

	} else if v, ok := resultDef.(io.WriteCloser); ok {
		// Stream response to io.WriteCloser
		if _, err := io.Copy(v, r.Body); err != nil {
			return nil, nil, fmt.Errorf(`cannot read resonse body: %w`, err)
		}
		if err := v.Close(); err != nil {
			return nil, nil, fmt.Errorf(`cannot read resonse body: %w`, err)
		}
	} else if v, ok := resultDef.(io.Writer); ok {
		// Stream response to io.Writer
		if _, err := io.Copy(v, r.Body); err != nil {
			return nil, nil, fmt.Errorf(`cannot read resonse body: %w`, err)
		}
	} else if contentType == "application/json" {
		// Map JSON response
		if r.StatusCode > 199 && r.StatusCode < 300 && resultDef != nil {
			// Map JSON response to defined result
			if err := json.NewDecoder(r.Body).Decode(resultDef); err != nil {
				return nil, nil, fmt.Errorf(`cannot decode JSON result: %w`, err)
			}
			return resultDef, nil, nil

		} else if r.StatusCode > 399 && errDef != nil {
			// Map JSON response to defined error
			if err := json.NewDecoder(r.Body).Decode(errDef); err != nil {
				return nil, nil, fmt.Errorf(`cannot decode JSON error: %w`, err)
			}
			// Set HTTP request
			if v, ok := errDef.(errorWithRequest); ok {
				v.SetRequest(r.Request)
			}
			// Set HTTP response
			if v, ok := errDef.(errorWithResponse); ok {
				v.SetResponse(r)
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
	ctx     context.Context
	trace   *Trace
	retry   RetryConfig
	wrapped http.RoundTripper
}

func (rt roundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	state := rt.retry.NewBackoff()
	attempt := 0
	for {
		// Trace request start
		if rt.trace != nil && rt.trace.HTTPRequestStart != nil {
			rt.trace.HTTPRequestStart(req)
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
		if rt.trace != nil && rt.trace.HTTPRequestRetry != nil {
			rt.trace.HTTPRequestRetry(attempt, delay)
		}

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
		case <-time.NewTimer(delay).C:
			// time elapsed, retry
		}
	}
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
