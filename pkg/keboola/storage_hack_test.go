package keboola

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/go-client/pkg/client"
)

func TestIsResourceAlreadyExistsError(t *testing.T) {
	t.Parallel()
	assert.False(t, isResourceAlreadyExistsError(
		&http.Response{},
		errors.New("foo bar"),
	))
	assert.False(t, isResourceAlreadyExistsError(
		&http.Response{StatusCode: http.StatusBadRequest},
		errors.New("foo bar"),
	))
	assert.False(t, isResourceAlreadyExistsError(
		&http.Response{StatusCode: http.StatusBadRequest},
		fmt.Errorf("foo: %w", &StorageError{ErrCode: "foo.notFound"}),
	))
	assert.True(t, isResourceAlreadyExistsError(
		&http.Response{
			StatusCode: http.StatusBadRequest,
			Request:    (&http.Request{}).WithContext(context.WithValue(context.Background(), client.RetryAttemptContextKey, 1)),
		},
		fmt.Errorf("foo: %w", &StorageError{ErrCode: "fooAlreadyExists"}),
	))
}

func TestIsResourceNotFoundError(t *testing.T) {
	t.Parallel()
	assert.False(t, isResourceNotFoundError(
		&http.Response{},
		errors.New("foo bar"),
	))
	assert.False(t, isResourceNotFoundError(
		&http.Response{StatusCode: http.StatusNotFound},
		errors.New("foo bar"),
	))
	assert.False(t, isResourceNotFoundError(
		&http.Response{StatusCode: http.StatusNotFound},
		fmt.Errorf("foo: %w", &StorageError{ErrCode: "foo.notFound"}),
	))
	assert.True(t, isResourceNotFoundError(
		&http.Response{
			StatusCode: http.StatusNotFound,
			Request:    (&http.Request{}).WithContext(context.WithValue(context.Background(), client.RetryAttemptContextKey, 1)),
		},
		fmt.Errorf("foo: %w", &StorageError{ErrCode: "foo.notFound"}),
	))
}

func TestHack_CreateConfigRequest_AlreadyExists(t *testing.T) {
	t.Parallel()
	// Mocked response
	transport := httpmock.NewMockTransport()
	transport.RegisterResponder(http.MethodGet, `https://connection.keboola.com/v2/storage/?exclude=components`, httpmock.NewStringResponder(200, `{
		"services": [],
		"features": []
	}`))
	transport.RegisterResponder(
		http.MethodPost,
		`https://connection.keboola.com/v2/storage/branch/123/components/foo.bar/configs`,
		httpmock.ResponderFromMultipleResponses([]*http.Response{
			{
				StatusCode: http.StatusInternalServerError,
				Body:       io.NopCloser(strings.NewReader("internal error")),
			},
			{
				StatusCode: http.StatusBadRequest,
				Header:     map[string][]string{"Content-Type": {"application/json"}},
				Body:       io.NopCloser(strings.NewReader(`{"code": "configurationAlreadyExists"}`)),
			},
		}),
	)
	transport.RegisterResponder(
		http.MethodGet,
		`https://connection.keboola.com/v2/storage/branch/123/components/foo.bar/configs/123`,
		httpmock.NewJsonResponderOrPanic(http.StatusOK, map[string]any{
			"componentId": "foo.bar",
			"id":          "123",
			"name":        "Name from the GET response",
		}),
	)

	// Create client
	c := client.New().WithTransport(transport).WithRetry(client.TestingRetry())
	api, err := NewAuthorizedAPI(context.Background(), "https://connection.keboola.com", "my-token", WithClient(&c))
	assert.NoError(t, err)

	// Run request
	config := &ConfigWithRows{Config: &Config{ConfigKey: ConfigKey{BranchID: 123, ComponentID: "foo.bar", ID: "123"}}}
	_, err = api.CreateConfigRequest(config).Send(context.Background())

	// The request ended without an error, the config was loaded via a GET request
	assert.NoError(t, err)
	assert.Equal(t, "Name from the GET response", config.Name)

	// Check HTTP requests count
	assert.Equal(t, map[string]int{
		"GET https://connection.keboola.com/v2/storage/?exclude=components":                       1,
		"POST https://connection.keboola.com/v2/storage/branch/123/components/foo.bar/configs":    2,
		"GET https://connection.keboola.com/v2/storage/branch/123/components/foo.bar/configs/123": 1,
	}, transport.GetCallCountInfo())
}

func TestHack_DeleteTableRequest_NotFound(t *testing.T) {
	t.Parallel()
	// Mocked response
	transport := httpmock.NewMockTransport()
	transport.RegisterResponder(http.MethodGet, `https://connection.keboola.com/v2/storage/?exclude=components`, httpmock.NewStringResponder(200, `{
		"services": [],
		"features": []
	}`))
	transport.RegisterResponder(
		http.MethodDelete,
		`https://connection.keboola.com/v2/storage/branch/123/tables/in.c-bucket.table`,
		httpmock.ResponderFromMultipleResponses([]*http.Response{
			{
				StatusCode: http.StatusInternalServerError,
				Body:       io.NopCloser(strings.NewReader("internal error")),
			},
			{
				StatusCode: http.StatusNotFound,
				Header:     map[string][]string{"Content-Type": {"application/json"}},
				Body:       io.NopCloser(strings.NewReader(`{"code": "storage.table.notFound"}`)),
			},
		}),
	)

	// Create client
	c := client.New().WithTransport(transport).WithRetry(client.TestingRetry())
	api, err := NewAuthorizedAPI(context.Background(), "https://connection.keboola.com", "my-token", WithClient(&c))
	assert.NoError(t, err)

	// Run request
	id := MustParseTableID("in.c-bucket.table")
	_, err = api.DeleteTableRequest(123, id).Send(context.Background())

	// The request ended without an error
	assert.NoError(t, err)

	// Check HTTP requests count
	assert.Equal(t, map[string]int{
		"GET https://connection.keboola.com/v2/storage/?exclude=components":                    1,
		"DELETE https://connection.keboola.com/v2/storage/branch/123/tables/in.c-bucket.table": 2,
	}, transport.GetCallCountInfo())
}
