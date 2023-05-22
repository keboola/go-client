package otel

import (
	"context"
	"errors"
	"net"
	"net/http"
)

func errorType(res *http.Response, err error) string {
	var netErr net.Error
	errors.As(err, &netErr)
	switch {
	case res != nil && res.StatusCode >= http.StatusInternalServerError:
		return "http_5xx_code"
	case res != nil && res.StatusCode >= http.StatusBadRequest:
		return "http_4xx_code"
	case err == nil:
		return ""
	case errors.Is(err, context.Canceled):
		return "context_canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	case netErr != nil && netErr.Timeout():
		return "net_timeout"
	case netErr != nil:
		return "net"
	default:
		return "other"
	}
}

func isRedirection(r *http.Response) bool {
	return r != nil && r.StatusCode >= http.StatusMultipleChoices && r.StatusCode < http.StatusBadRequest
}
