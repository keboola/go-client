package otel

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsSuccess(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "", errorType(nil, nil))
	assert.Equal(t, "other", errorType(nil, errors.New("some error")))
	assert.Equal(t, "other", errorType(&http.Response{}, errors.New("some error")))
	assert.Equal(t, "context_canceled", errorType(nil, fmt.Errorf(`some error: %w`, context.Canceled)))
	assert.Equal(t, "deadline_exceeded", errorType(nil, fmt.Errorf(`some error: %w`, context.DeadlineExceeded)))
	assert.Equal(t, "net", errorType(nil, &net.DNSError{}))
	assert.Equal(t, "net_timeout", errorType(nil, &net.DNSError{IsTimeout: true}))
	assert.Equal(t, "http_4xx_code", errorType(&http.Response{StatusCode: http.StatusBadRequest}, errors.New("some error")))
	assert.Equal(t, "http_5xx_code", errorType(&http.Response{StatusCode: http.StatusInternalServerError}, errors.New("some error")))
	assert.Equal(t, "other", errorType(&http.Response{StatusCode: http.StatusOK}, errors.New("some error")))
	assert.Equal(t, "", errorType(&http.Response{StatusCode: http.StatusOK}, nil))
}

func TestIsRedirection(t *testing.T) {
	t.Parallel()
	assert.False(t, isRedirection(nil))
	assert.False(t, isRedirection(&http.Response{}))
	assert.False(t, isRedirection(&http.Response{StatusCode: http.StatusOK}))
	assert.False(t, isRedirection(&http.Response{StatusCode: http.StatusBadRequest}))
	assert.True(t, isRedirection(&http.Response{StatusCode: http.StatusTemporaryRedirect}))
}
