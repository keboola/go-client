package storageapi

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/keboola/go-client/pkg/client"
)

// ignoreResourceAlreadyExistsError is a workaround for problems with the Storage API.
// Sometimes it happens that the HTTP request ends with a 500 error, but the operation was performed.
// In that case, a retry is performed, which ends with an "already exists" error.
// The error should be ignored, because the CREATE operation was performed.
func ignoreResourceAlreadyExistsError(getFn func(context.Context) error) func(context.Context, client.HTTPResponse, error) error {
	return func(ctx context.Context, response client.HTTPResponse, err error) error {
		if isResourceAlreadyExistsError(response.RawResponse(), err) {
			// Fill result with the GET request
			return getFn(ctx)
		}
		return err
	}
}

// ignoreResourceNotFoundError is a workaround for problems with the Storage API.
// Sometimes it happens that the HTTP request ends with a 500 error, but the operation was performed.
// In that case, a retry is performed, which ends with a "not found" error.
// The error should be ignored, because the DELETE operation was performed.
func ignoreResourceNotFoundError() func(context.Context, client.HTTPResponse, error) error {
	return func(_ context.Context, response client.HTTPResponse, err error) error {
		if isResourceNotFoundError(response.RawResponse(), err) {
			return nil
		}
		return err
	}
}

func isResourceAlreadyExistsError(response *http.Response, err error) bool {
	var storageApiError *Error

	// There must be an HTTP response
	if response == nil {
		return false
	}

	// There must be an HTTP request
	if response.Request == nil {
		return false
	}

	// There must be a retry, so the operation was performed but the HTTP request ended with an error.
	if attempt, _ := client.ContextRetryAttempt(response.Request.Context()); attempt == 0 {
		return false
	}

	// It must be a Storage API error
	if !errors.As(err, &storageApiError) {
		return false
	}

	// The error HTTP code must match
	if response.StatusCode != http.StatusBadRequest {
		return false
	}

	// The error code must match, for example "configurationAlreadyExists"
	if !strings.HasSuffix(storageApiError.ErrCode, "AlreadyExists") {
		return false
	}

	return true
}

func isResourceNotFoundError(response *http.Response, err error) bool {
	var storageApiError *Error

	// There must be an HTTP response
	if response == nil {
		return false
	}

	// There must be an HTTP request
	if response.Request == nil {
		return false
	}

	// There must be a retry, so the operation was performed but the HTTP request ended with an error.
	if attempt, _ := client.ContextRetryAttempt(response.Request.Context()); attempt == 0 {
		return false
	}

	// It must be a Storage API error
	if !errors.As(err, &storageApiError) {
		return false
	}

	// The error HTTP code must match
	if response.StatusCode != http.StatusNotFound {
		return false
	}

	// The error code must match, for example "storage.bucket.notFound"
	if !strings.HasSuffix(storageApiError.ErrCode, "notFound") {
		return false
	}

	return true
}
