package client_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"

	. "github.com/keboola/go-client/pkg/client"
)

type testStruct struct {
	Foo string `json:"foo"`
}

type testError struct {
	ErrorMsg string `json:"error"`
}

type testWriteCloser struct {
	io.Writer
}

func (v testWriteCloser) Close() error {
	_, err := v.Write([]byte("<CLOSE>"))
	return err
}

func (e testError) Error() string {
	return e.ErrorMsg
}

func TestNew(t *testing.T) {
	t.Parallel()
	c := New()
	assert.NotNil(t, c)
}

func TestRequest(t *testing.T) {
	t.Parallel()

	// Mocked response
	transport := httpmock.NewMockTransport()
	transport.RegisterResponder("GET", `https://example.com`, httpmock.NewStringResponder(200, "test"))

	ctx := context.Background()
	c := New().WithTransport(transport).WithRetry(TestingRetry())
	_, _, err := NewHTTPRequest().WithGet("https://example.com").Send(ctx, c)
	assert.NoError(t, err)
	assert.Equal(t, 1, transport.GetCallCountInfo()["GET https://example.com"])
}

func TestBytesResult(t *testing.T) {
	t.Parallel()

	// Mocked response
	transport := httpmock.NewMockTransport()
	transport.RegisterResponder("GET", "https://example.com", httpmock.NewJsonResponderOrPanic(200, map[string]any{"foo": "bar"}))

	ctx := context.Background()
	c := New().WithTransport(transport).WithRetry(TestingRetry())
	var resultDef []byte
	_, result, err := NewHTTPRequest().WithGet("https://example.com").WithResult(&resultDef).Send(ctx, c)
	assert.NoError(t, err)
	assert.Same(t, &resultDef, result)
	assert.Equal(t, []byte(`{"foo":"bar"}`), resultDef)
	assert.Equal(t, 1, transport.GetCallCountInfo()["GET https://example.com"])
}

func TestWriterResult(t *testing.T) {
	t.Parallel()

	// Mocked response
	transport := httpmock.NewMockTransport()
	transport.RegisterResponder("GET", "https://example.com", httpmock.NewJsonResponderOrPanic(200, map[string]any{"foo": "bar"}))

	ctx := context.Background()
	c := New().WithTransport(transport).WithRetry(TestingRetry())
	var out strings.Builder
	_, _, err := NewHTTPRequest().WithGet("https://example.com").WithResult(io.Writer(&out)).Send(ctx, c)
	assert.NoError(t, err)
	assert.Equal(t, `{"foo":"bar"}`, out.String())
	assert.Equal(t, 1, transport.GetCallCountInfo()["GET https://example.com"])
}

func TestWriteCloserResult(t *testing.T) {
	t.Parallel()

	// Mocked response
	transport := httpmock.NewMockTransport()
	transport.RegisterResponder("GET", "https://example.com", httpmock.NewJsonResponderOrPanic(200, map[string]any{"foo": "bar"}))

	ctx := context.Background()
	c := New().WithTransport(transport).WithRetry(TestingRetry())
	var out strings.Builder
	_, _, err := NewHTTPRequest().WithGet("https://example.com").WithResult(testWriteCloser{Writer: &out}).Send(ctx, c)
	assert.NoError(t, err)
	assert.Equal(t, `{"foo":"bar"}<CLOSE>`, out.String())
	assert.Equal(t, 1, transport.GetCallCountInfo()["GET https://example.com"])
}

func TestJsonMapResult(t *testing.T) {
	t.Parallel()

	// Mocked response
	transport := httpmock.NewMockTransport()
	transport.RegisterResponder("GET", `https://example.com`, httpmock.NewJsonResponderOrPanic(200, map[string]any{"foo": "bar"}))

	ctx := context.Background()
	c := New().WithTransport(transport).WithRetry(TestingRetry())
	resultDef := make(map[string]any)
	_, result, err := NewHTTPRequest().WithGet("https://example.com").WithResult(&resultDef).Send(ctx, c)
	assert.NoError(t, err)
	assert.Same(t, &resultDef, result)
	assert.Equal(t, &map[string]any{"foo": "bar"}, result)
	assert.Equal(t, 1, transport.GetCallCountInfo()["GET https://example.com"])
}

func TestJsonStructResult(t *testing.T) {
	t.Parallel()

	// Mocked response
	transport := httpmock.NewMockTransport()
	transport.RegisterResponder("GET", `https://example.com`, httpmock.NewJsonResponderOrPanic(200, map[string]any{"foo": "bar"}))

	ctx := context.Background()
	c := New().WithTransport(transport).WithRetry(TestingRetry())
	resultDef := &testStruct{}
	_, result, err := NewHTTPRequest().WithGet("https://example.com").WithResult(resultDef).Send(ctx, c)
	assert.NoError(t, err)
	assert.Same(t, resultDef, result)
	assert.Equal(t, &testStruct{Foo: "bar"}, result)
	assert.Equal(t, 1, transport.GetCallCountInfo()["GET https://example.com"])
}

func TestJsonErrorResult(t *testing.T) {
	t.Parallel()

	// Mocked response
	transport := httpmock.NewMockTransport()
	transport.RegisterResponder("GET", `https://example.com`, httpmock.NewJsonResponderOrPanic(400, map[string]any{"error": "error message"}))

	ctx := context.Background()
	c := New().WithTransport(transport).WithRetry(TestingRetry())
	errDef := &testError{}
	_, _, err := NewHTTPRequest().WithGet("https://example.com").WithError(errDef).Send(ctx, c)
	assert.Error(t, err)
	assert.Same(t, errDef, err)
	assert.Equal(t, &testError{ErrorMsg: "error message"}, err)
	assert.Equal(t, 1, transport.GetCallCountInfo()["GET https://example.com"])
}

func TestWithBaseUrl(t *testing.T) {
	t.Parallel()

	// Mocked response
	transport := httpmock.NewMockTransport()
	transport.RegisterResponder("GET", "https://example.com/baz", httpmock.NewStringResponder(200, "test"))

	ctx := context.Background()
	c := New().WithTransport(transport).WithRetry(TestingRetry()).WithBaseURL("https://example.com")
	_, _, err := NewHTTPRequest().WithGet("baz").Send(ctx, c)
	assert.NoError(t, err)
	assert.Equal(t, 1, transport.GetCallCountInfo()["GET https://example.com/baz"])
}

func TestRequestContext(t *testing.T) {
	t.Parallel()

	// Mocked response
	transport := httpmock.NewMockTransport()
	transport.RegisterResponder("GET", `https://example.com`, func(request *http.Request) (*http.Response, error) {
		// Request context should be used by HTTP request
		assert.Equal(t, "testValue", request.Context().Value("testKey"))
		return httpmock.NewStringResponse(200, "test"), nil
	})
	//lint:ignore SA1029 it is ok to use "testKey" without custom type in this test
	ctx := context.WithValue(context.Background(), "testKey", "testValue")
	c := New().WithTransport(transport).WithRetry(TestingRetry())
	_, _, err := NewHTTPRequest().WithGet("https://example.com").Send(ctx, c)
	assert.NoError(t, err)
	assert.Equal(t, 1, transport.GetCallCountInfo()["GET https://example.com"])
}

func TestDefaultUserAgent(t *testing.T) {
	t.Parallel()

	// Mocked response
	transport := httpmock.NewMockTransport()
	transport.RegisterResponder("GET", "https://example.com", func(request *http.Request) (*http.Response, error) {
		assert.Equal(t, http.Header{
			"User-Agent": []string{"keboola-go-client"},
		}, request.Header)
		return httpmock.NewStringResponse(200, "test"), nil
	})

	ctx := context.Background()
	c := New().WithTransport(transport).WithRetry(TestingRetry())
	_, _, err := NewHTTPRequest().WithGet("https://example.com").Send(ctx, c)
	assert.NoError(t, err)
	assert.Equal(t, 1, transport.GetCallCountInfo()["GET https://example.com"])
}

func TestWithUserAgent(t *testing.T) {
	t.Parallel()

	// Mocked response
	transport := httpmock.NewMockTransport()
	transport.RegisterResponder("GET", `https://example.com`, func(request *http.Request) (*http.Response, error) {
		assert.Equal(t, http.Header{
			"User-Agent": []string{"my-user-agent"},
		}, request.Header)
		return httpmock.NewStringResponse(200, "test"), nil
	})

	ctx := context.Background()
	c := New().WithTransport(transport).WithRetry(TestingRetry()).WithUserAgent("my-user-agent")
	_, _, err := NewHTTPRequest().WithGet("https://example.com").Send(ctx, c)
	assert.NoError(t, err)
	assert.Equal(t, 1, transport.GetCallCountInfo()["GET https://example.com"])
}

func TestWithHeader(t *testing.T) {
	t.Parallel()

	// Mocked response
	transport := httpmock.NewMockTransport()
	transport.RegisterResponder("GET", `https://example.com`, func(request *http.Request) (*http.Response, error) {
		assert.Equal(t, http.Header{
			"User-Agent": []string{"keboola-go-client"},
			"My-Header":  []string{"my-value"},
		}, request.Header)
		return httpmock.NewStringResponse(200, "test"), nil
	})

	ctx := context.Background()
	c := New().WithTransport(transport).WithRetry(TestingRetry()).WithHeader("my-header", "my-value")
	_, _, err := NewHTTPRequest().WithGet("https://example.com").Send(ctx, c)
	assert.NoError(t, err)
	assert.Equal(t, 1, transport.GetCallCountInfo()["GET https://example.com"])
}

func TestWithHeaders(t *testing.T) {
	t.Parallel()

	// Mocked response
	transport := httpmock.NewMockTransport()
	transport.RegisterResponder("GET", `https://example.com`, func(request *http.Request) (*http.Response, error) {
		assert.Equal(t, http.Header{
			"User-Agent": []string{"keboola-go-client"},
			"Key1":       []string{"value1"},
			"Key2":       []string{"value2"},
		}, request.Header)
		return httpmock.NewStringResponse(200, "test"), nil
	})

	ctx := context.Background()
	c := New().WithTransport(transport).WithRetry(TestingRetry()).WithHeaders(map[string]string{
		"key1": "value1",
		"key2": "value2",
	})
	_, _, err := NewHTTPRequest().WithGet("https://example.com").Send(ctx, c)
	assert.NoError(t, err)
	assert.Equal(t, 1, transport.GetCallCountInfo()["GET https://example.com"])
}

func TestRetryCount(t *testing.T) {
	t.Parallel()

	// Mocked response
	transport := httpmock.NewMockTransport()
	transport.RegisterResponder("GET", `https://example.com`, httpmock.NewStringResponder(504, "test"))

	// Setup
	retryCount := 10
	var delays []time.Duration

	// Create client
	ctx := context.Background()
	c := New().
		WithTransport(transport).
		WithRetry(RetryConfig{
			Condition:     DefaultRetryCondition(),
			Count:         retryCount,
			WaitTimeStart: 1 * time.Microsecond,
			WaitTimeMax:   20 * time.Microsecond,
		}).
		AndTrace(func() *Trace {
			return &Trace{
				HTTPRequestRetry: func(_ int, delay time.Duration) {
					delays = append(delays, delay)
				},
			}
		})

	// Get
	_, _, err := NewHTTPRequest().WithGet("https://example.com").Send(ctx, c)
	assert.Error(t, err)
	assert.Equal(t, `request GET "https://example.com" failed: 504 Gateway Timeout`, err.Error())

	// Check number of requests
	assert.Equal(t, 1+retryCount, transport.GetCallCountInfo()["GET https://example.com"])

	// Check delays
	assert.Equal(t, []time.Duration{
		1 * time.Microsecond,
		2 * time.Microsecond,
		4 * time.Microsecond,
		8 * time.Microsecond,
		16 * time.Microsecond,
		20 * time.Microsecond,
		20 * time.Microsecond,
		20 * time.Microsecond,
		20 * time.Microsecond,
		20 * time.Microsecond,
	}, delays)
}

func TestRequestTimeout(t *testing.T) {
	t.Parallel()

	// Mocked response
	transport := httpmock.NewMockTransport()
	transport.RegisterResponder("GET", "https://example.com", func(request *http.Request) (*http.Response, error) {
		time.Sleep(100 * time.Millisecond) // <<<<<<<
		return httpmock.NewStringResponse(504, "test"), nil
	})

	// Create client
	ctx := context.Background()
	c := New().
		WithTransport(transport).
		WithRetry(RetryConfig{
			Condition:           DefaultRetryCondition(),
			Count:               10,
			TotalRequestTimeout: 5 * time.Millisecond, // <<<<<<<
		})

	// Get
	_, _, err := NewHTTPRequest().WithGet("https://example.com").Send(ctx, c)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), `request GET "https://example.com" failed: timeout after`)
}

func TestContext_DeadlineExceeded(t *testing.T) {
	t.Parallel()

	// Mocked response
	transport := httpmock.NewMockTransport()
	transport.RegisterResponder("GET", "https://example.com", func(request *http.Request) (*http.Response, error) {
		time.Sleep(100 * time.Millisecond) // <<<<<<<
		return httpmock.NewStringResponse(504, "test"), nil
	})

	// Create client
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(50*time.Millisecond))
	defer cancel()
	c := New().WithTransport(transport)

	wg := NewWaitGroup(ctx, c)
	wg.Send(NewHTTPRequest().WithGet("https://example.com"))
	err := wg.Wait()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), `request GET "https://example.com" failed: timeout after`)
}

func TestContext_Canceled(t *testing.T) {
	t.Parallel()

	// Mocked response
	transport := httpmock.NewMockTransport()
	transport.RegisterResponder("GET", "https://example.com", func(request *http.Request) (*http.Response, error) {
		time.Sleep(100 * time.Millisecond) // <<<<<<<
		return httpmock.NewStringResponse(504, "test"), nil
	})

	// Create client
	ctx, cancel := context.WithCancel(context.Background())
	c := New().WithTransport(transport)

	wg := NewWaitGroup(ctx, c)
	wg.Send(NewHTTPRequest().WithGet("https://example.com"))

	time.Sleep(50 * time.Millisecond)
	cancel()

	err := wg.Wait()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), `request GET "https://example.com" failed: canceled after`)
}

func TestStopRetryOnRequestTimeout(t *testing.T) {
	t.Parallel()

	// Mocked response
	transport := httpmock.NewMockTransport()
	transport.RegisterResponder("GET", "https://example.com", httpmock.NewStringResponder(504, "test"))

	// Setup
	var delays []time.Duration

	// Create client
	ctx := context.Background()
	c := New().
		WithTransport(transport).
		WithRetry(RetryConfig{
			Condition:           DefaultRetryCondition(),
			Count:               10,
			TotalRequestTimeout: 30 * time.Millisecond, // <<<<<<<
			WaitTimeStart:       40 * time.Millisecond, // <<<<<<<
			WaitTimeMax:         40 * time.Millisecond,
		}).
		AndTrace(func() *Trace {
			return &Trace{
				HTTPRequestRetry: func(_ int, delay time.Duration) {
					delays = append(delays, delay)
				},
			}
		})

	// Get
	_, _, err := NewHTTPRequest().WithGet("https://example.com").Send(ctx, c)
	assert.Error(t, err)
	assert.Equal(t, `request GET "https://example.com" failed: 504 Gateway Timeout`, err.Error())

	// Check number of requests
	assert.Equal(t, 1, transport.GetCallCountInfo()["GET https://example.com"])

	// Check delays
	assert.Empty(t, delays)
}

func TestDoNotRetry(t *testing.T) {
	t.Parallel()

	// Mocked response
	transport := httpmock.NewMockTransport()
	transport.RegisterResponder("GET", "https://example.com", httpmock.NewStringResponder(403, "test"))

	// Setup
	var delays []time.Duration

	// Create client
	ctx := context.Background()
	c := New().
		WithTransport(transport).
		WithRetry(RetryConfig{
			Condition:     DefaultRetryCondition(),
			Count:         10,
			WaitTimeStart: 1 * time.Microsecond,
			WaitTimeMax:   20 * time.Microsecond,
		}).
		AndTrace(func() *Trace {
			return &Trace{
				HTTPRequestRetry: func(_ int, delay time.Duration) {
					delays = append(delays, delay)
				},
			}
		})

	// Get
	_, _, err := NewHTTPRequest().WithGet("https://example.com").Send(ctx, c)
	assert.Error(t, err)
	assert.Equal(t, `request GET "https://example.com" failed: 403 Forbidden`, err.Error())

	// Check number of requests
	assert.Equal(t, 1, transport.GetCallCountInfo()["GET https://example.com"])

	// Check delays
	assert.Empty(t, delays)
}
