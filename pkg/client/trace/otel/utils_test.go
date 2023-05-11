package otel

import (
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsSuccess(t *testing.T) {
	t.Parallel()
	assert.False(t, isSuccess(nil, nil))
	assert.False(t, isSuccess(nil, errors.New("some error")))
	assert.False(t, isSuccess(&http.Response{}, errors.New("some error")))
	assert.False(t, isSuccess(&http.Response{StatusCode: http.StatusBadRequest}, errors.New("some error")))
	assert.False(t, isSuccess(&http.Response{StatusCode: http.StatusOK}, errors.New("some error")))
	assert.True(t, isSuccess(&http.Response{StatusCode: http.StatusOK}, nil))
}

func TestIsRedirection(t *testing.T) {
	t.Parallel()
	assert.False(t, isRedirection(nil))
	assert.False(t, isRedirection(&http.Response{}))
	assert.False(t, isRedirection(&http.Response{StatusCode: http.StatusOK}))
	assert.False(t, isRedirection(&http.Response{StatusCode: http.StatusBadRequest}))
	assert.True(t, isRedirection(&http.Response{StatusCode: http.StatusTemporaryRedirect}))
}
