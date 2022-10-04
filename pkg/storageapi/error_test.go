package storageapi_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"

	. "github.com/keboola/go-client/pkg/storageapi"
)

func TestErrorMsg1(t *testing.T) {
	t.Parallel()
	reqUrl, _ := url.Parse("https://example.com")
	e := &Error{Message: "msg"}
	e.SetRequest(&http.Request{URL: reqUrl, Method: http.MethodGet})
	e.SetResponse(&http.Response{StatusCode: 404})
	assert.Equal(t, `msg, method: "GET", url: "https://example.com", httpCode: "404"`, e.Error())
}

func TestErrorMsg2(t *testing.T) {
	t.Parallel()
	reqUrl, _ := url.Parse("https://example.com")
	e := &Error{Message: "msg", ErrCode: "errCode", ExceptionID: "exceptionId"}
	e.SetRequest(&http.Request{URL: reqUrl, Method: http.MethodGet})
	e.SetResponse(&http.Response{StatusCode: 404})
	assert.Equal(t, `msg, method: "GET", url: "https://example.com", httpCode: "404", errCode: "errCode", exceptionId: "exceptionId"`, e.Error())
}

func TestErrorHttpStatus(t *testing.T) {
	t.Parallel()
	e := &Error{}
	e.SetRequest(&http.Request{Method: http.MethodGet})
	e.SetResponse(&http.Response{StatusCode: 123})
	assert.Equal(t, 123, e.StatusCode())
}
