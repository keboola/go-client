package client_test

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"

	. "github.com/keboola/go-client/pkg/client"
	. "github.com/keboola/go-client/pkg/request"
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
	_, _, err := NewHTTPRequest(c).WithGet("https://example.com").Send(ctx)
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
	_, result, err := NewHTTPRequest(c).WithGet("https://example.com").WithResult(&resultDef).Send(ctx)
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
	_, _, err := NewHTTPRequest(c).WithGet("https://example.com").WithResult(io.Writer(&out)).Send(ctx)
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
	_, _, err := NewHTTPRequest(c).WithGet("https://example.com").WithResult(testWriteCloser{Writer: &out}).Send(ctx)
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
	_, result, err := NewHTTPRequest(c).WithGet("https://example.com").WithResult(&resultDef).Send(ctx)
	assert.NoError(t, err)
	assert.Same(t, &resultDef, result)
	assert.Equal(t, &map[string]any{"foo": "bar"}, result)
	assert.Equal(t, 1, transport.GetCallCountInfo()["GET https://example.com"])
}

func TestJsonMapResult_ContentTypeWithCharset(t *testing.T) {
	t.Parallel()

	// Mocked response
	transport := httpmock.NewMockTransport()
	transport.RegisterResponder("GET", `https://example.com`, func(request *http.Request) (*http.Response, error) {
		response := httpmock.NewBytesResponse(200, []byte(`{"foo":"bar"}`))
		response.Header.Set("Content-Type", "application/json; charset=utf-8")
		return response, nil
	})

	ctx := context.Background()
	c := New().WithTransport(transport).WithRetry(TestingRetry())
	resultDef := make(map[string]any)
	_, result, err := NewHTTPRequest(c).WithGet("https://example.com").WithResult(&resultDef).Send(ctx)
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
	_, result, err := NewHTTPRequest(c).WithGet("https://example.com").WithResult(resultDef).Send(ctx)
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
	_, _, err := NewHTTPRequest(c).WithGet("https://example.com").WithError(errDef).Send(ctx)
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
	_, _, err := NewHTTPRequest(c).WithGet("baz").Send(ctx)
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
	ctx := context.WithValue(context.Background(), "testKey", "testValue") //nolint:staticcheck
	c := New().WithTransport(transport).WithRetry(TestingRetry())
	_, _, err := NewHTTPRequest(c).WithGet("https://example.com").Send(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 1, transport.GetCallCountInfo()["GET https://example.com"])
}

func TestDefaultHeaders(t *testing.T) {
	t.Parallel()

	// Mocked response
	transport := httpmock.NewMockTransport()
	transport.RegisterResponder("GET", "https://example.com", func(request *http.Request) (*http.Response, error) {
		assert.Equal(t, http.Header{
			"User-Agent":      []string{"keboola-go-client"},
			"Accept-Encoding": []string{"gzip, br"},
		}, request.Header)
		return httpmock.NewStringResponse(200, "test"), nil
	})

	ctx := context.Background()
	c := New().WithTransport(transport).WithRetry(TestingRetry())
	_, _, err := NewHTTPRequest(c).WithGet("https://example.com").Send(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 1, transport.GetCallCountInfo()["GET https://example.com"])
}

func TestWithUserAgent(t *testing.T) {
	t.Parallel()

	// Mocked response
	transport := httpmock.NewMockTransport()
	transport.RegisterResponder("GET", `https://example.com`, func(request *http.Request) (*http.Response, error) {
		assert.Equal(t, http.Header{
			"User-Agent":      []string{"my-user-agent"},
			"Accept-Encoding": []string{"gzip, br"},
		}, request.Header)
		return httpmock.NewStringResponse(200, "test"), nil
	})

	ctx := context.Background()
	c := New().WithTransport(transport).WithRetry(TestingRetry()).WithUserAgent("my-user-agent")
	_, _, err := NewHTTPRequest(c).WithGet("https://example.com").Send(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 1, transport.GetCallCountInfo()["GET https://example.com"])
}

func TestWithHeader(t *testing.T) {
	t.Parallel()

	// Mocked response
	transport := httpmock.NewMockTransport()
	transport.RegisterResponder("GET", `https://example.com`, func(request *http.Request) (*http.Response, error) {
		assert.Equal(t, http.Header{
			"User-Agent":      []string{"keboola-go-client"},
			"Accept-Encoding": []string{"gzip, br"},
			"My-Header":       []string{"my-value"},
		}, request.Header)
		return httpmock.NewStringResponse(200, "test"), nil
	})

	ctx := context.Background()
	c := New().WithTransport(transport).WithRetry(TestingRetry()).WithHeader("my-header", "my-value")
	_, _, err := NewHTTPRequest(c).WithGet("https://example.com").Send(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 1, transport.GetCallCountInfo()["GET https://example.com"])
}

func TestWithHeaders(t *testing.T) {
	t.Parallel()

	// Mocked response
	transport := httpmock.NewMockTransport()
	transport.RegisterResponder("GET", `https://example.com`, func(request *http.Request) (*http.Response, error) {
		assert.Equal(t, http.Header{
			"User-Agent":      []string{"keboola-go-client"},
			"Accept-Encoding": []string{"gzip, br"},
			"Key1":            []string{"value1"},
			"Key2":            []string{"value2"},
		}, request.Header)
		return httpmock.NewStringResponse(200, "test"), nil
	})

	ctx := context.Background()
	c := New().WithTransport(transport).WithRetry(TestingRetry()).WithHeaders(map[string]string{
		"key1": "value1",
		"key2": "value2",
	})
	_, _, err := NewHTTPRequest(c).WithGet("https://example.com").Send(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 1, transport.GetCallCountInfo()["GET https://example.com"])
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
	_, _, err := NewHTTPRequest(c).WithGet("https://example.com").Send(ctx)
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

	wg := NewWaitGroup(ctx)
	wg.Send(NewHTTPRequest(c).WithGet("https://example.com"))
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

	wg := NewWaitGroup(ctx)
	wg.Send(NewHTTPRequest(c).WithGet("https://example.com"))

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
	_, _, err := NewHTTPRequest(c).WithGet("https://example.com").Send(ctx)
	assert.Error(t, err)
	assert.Equal(t, `request GET "https://example.com" failed: 504 Gateway Timeout`, err.Error())

	// Check number of requests
	assert.Equal(t, 1, transport.GetCallCountInfo()["GET https://example.com"])

	// Check delays
	assert.Empty(t, delays)
}
