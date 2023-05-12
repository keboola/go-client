package request_test

import (
	"context"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/request"
)

type error1 struct {
	error
}

type error2 struct {
	error
}

type result1 struct{}

type result2 struct{}

func TestHttpRequest_Immutability(t *testing.T) {
	t.Parallel()
	var a, b request.HTTPRequest
	c := client.New()
	a = request.NewHTTPRequest(c)

	// WithGet
	a = a.WithGet("/foo1")
	b = a.WithGet("/foo2")
	assert.Equal(t, http.MethodGet, a.Method())
	assert.Equal(t, "/foo1", a.URL())
	assert.Equal(t, http.MethodGet, b.Method())
	assert.Equal(t, "/foo2", b.URL())

	// WithPost
	a = a.WithPost("/foo1")
	b = a.WithPost("/foo2")
	assert.Equal(t, http.MethodPost, a.Method())
	assert.Equal(t, "/foo1", a.URL())
	assert.Equal(t, http.MethodPost, b.Method())
	assert.Equal(t, "/foo2", b.URL())

	// WithPut
	a = a.WithPut("/foo1")
	b = a.WithPut("/foo2")
	assert.Equal(t, http.MethodPut, a.Method())
	assert.Equal(t, "/foo1", a.URL())
	assert.Equal(t, http.MethodPut, b.Method())
	assert.Equal(t, "/foo2", b.URL())

	// WithDelete
	a = a.WithDelete("/foo1")
	b = a.WithDelete("/foo2")
	assert.Equal(t, http.MethodDelete, a.Method())
	assert.Equal(t, "/foo1", a.URL())
	assert.Equal(t, http.MethodDelete, b.Method())
	assert.Equal(t, "/foo2", b.URL())

	// WithMethod
	a = a.WithMethod(http.MethodGet)
	b = a.WithMethod(http.MethodPost)
	assert.Equal(t, http.MethodGet, a.Method())
	assert.Equal(t, http.MethodPost, b.Method())

	// WithBaseURL
	a = a.WithBaseURL("/base1")
	b = a.WithBaseURL("/base2")
	assert.Equal(t, "/base1/foo1", a.URL())
	assert.Equal(t, "/base2/foo1", b.URL())

	// WithURL
	a = a.WithURL("/url1")
	b = a.WithURL("/url2")
	assert.Equal(t, "/base1/url1", a.URL())
	assert.Equal(t, "/base1/url2", b.URL())

	// AndHeader
	a = a.AndHeader("key1", "value1")
	b = a.AndHeader("key2", "value2")
	assert.Equal(t, http.Header{"Key1": []string{"value1"}}, a.RequestHeader())
	assert.Equal(t, http.Header{"Key1": []string{"value1"}, "Key2": []string{"value2"}}, b.RequestHeader())

	// AndQueryParam
	a = a.AndQueryParam("key1", "value1")
	b = a.AndQueryParam("key2", "value2")
	assert.Equal(t, url.Values{"key1": []string{"value1"}}, a.QueryParams())
	assert.Equal(t, url.Values{"key1": []string{"value1"}, "key2": []string{"value2"}}, b.QueryParams())

	// WithQueryParams
	a = a.WithQueryParams(map[string]string{"foo1": "bar1"})
	b = a.WithQueryParams(map[string]string{"foo2": "bar2"})
	assert.Equal(t, url.Values{"foo1": []string{"bar1"}}, a.QueryParams())
	assert.Equal(t, url.Values{"foo2": []string{"bar2"}}, b.QueryParams())

	// AndPathParam
	a = a.AndPathParam("key1", "value1")
	b = a.AndPathParam("key2", "value2")
	assert.Equal(t, map[string]string{"key1": "value1"}, a.PathParams())
	assert.Equal(t, map[string]string{"key1": "value1", "key2": "value2"}, b.PathParams())

	// WithPathParams
	a = a.WithPathParams(map[string]string{"foo1": "bar1"})
	b = a.WithPathParams(map[string]string{"foo2": "bar2"})
	assert.Equal(t, map[string]string{"foo1": "bar1"}, a.PathParams())
	assert.Equal(t, map[string]string{"foo2": "bar2"}, b.PathParams())

	// WithFormBody
	a = a.WithFormBody(map[string]string{"foo1": "bar1"})
	b = a.WithFormBody(map[string]string{"foo2": "bar2"})
	assert.NotEqual(t, a, b)
	assert.Equal(t, "foo1=bar1", a.RequestBody())
	assert.Equal(t, "foo2=bar2", b.RequestBody())

	// WithPathParams
	a = a.WithJSONBody(123)
	b = a.WithJSONBody(456)
	assert.Equal(t, 123, a.RequestBody())
	assert.Equal(t, 456, b.RequestBody())

	// WithError
	a = a.WithError(&error1{})
	b = a.WithError(&error2{})
	assert.Equal(t, &error1{}, a.ErrorDef())
	assert.Equal(t, &error2{}, b.ErrorDef())

	// WithResult
	a = a.WithResult(&result1{})
	b = a.WithResult(&result2{})
	assert.Equal(t, &result1{}, a.ResultDef())
	assert.Equal(t, &result2{}, b.ResultDef())

	// WithOnComplete
	l1 := func(ctx context.Context, response request.HTTPResponse, err error) error {
		return nil
	}
	l2 := func(ctx context.Context, response request.HTTPResponse, err error) error {
		return nil
	}
	a = a.WithOnComplete(l1)
	b = a.WithOnComplete(l2)
	assert.NotEqual(t, a, b)

	// WithOnSuccess
	l3 := func(ctx context.Context, response request.HTTPResponse) error {
		return nil
	}
	l4 := func(ctx context.Context, response request.HTTPResponse) error {
		return nil
	}
	a = a.WithOnSuccess(l3)
	b = a.WithOnSuccess(l4)
	assert.NotEqual(t, a, b)

	// WithOnError
	l5 := func(ctx context.Context, response request.HTTPResponse, err error) error {
		return nil
	}
	l6 := func(ctx context.Context, response request.HTTPResponse, err error) error {
		return nil
	}
	a = a.WithOnError(l5)
	b = a.WithOnError(l6)
	assert.NotEqual(t, a, b)
}

func TestToFormBody(t *testing.T) {
	t.Parallel()

	data := map[string]any{
		"string": "test",
		"number": 100,
		"slice":  []string{"a", "b", "c"},
		"map":    map[string]string{"k0": "v0", "k1": "v1"},
	}

	expected := map[string]string{
		"string":   "test",
		"number":   "100",
		"slice[0]": "a",
		"slice[1]": "b",
		"slice[2]": "c",
		"map[k0]":  "v0",
		"map[k1]":  "v1",
	}
	actual := request.ToFormBody(data)

	assert.Equal(t, expected, actual)
}
