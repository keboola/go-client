package otel

import "net/http"

func isSuccess(r *http.Response, err error) bool {
	if err != nil {
		return false
	}
	return r != nil && r.StatusCode < http.StatusBadRequest
}

func isRedirection(r *http.Response) bool {
	return r != nil && r.StatusCode >= http.StatusMultipleChoices && r.StatusCode < http.StatusBadRequest
}
