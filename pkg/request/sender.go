package request

import (
	"context"
	"net/http"
)

// Sender represents an HTTP client, the client.Client is a default implementation using the standard net/http package.
type Sender interface {
	// Send method sends defined request and returns response.
	// Type of the return value "result" must be the same as type of the HTTPRequest.ResultDef(), otherwise panic will occur.
	//   In Go, this rule cannot be written using generic types yet, methods cannot have generic types.
	//   Send[R Result](ctx context.Context, request HTTPRequest[R]) (rawResponse *http.Response, result R, error error)
	Send(ctx context.Context, request HTTPRequest) (rawResponse *http.Response, result any, err error)
}

// Sendable is HTTPRequest or APIRequest.
type Sendable interface {
	SendOrErr(ctx context.Context) error
}

// ReqDefinitionError can be used as the Sendable interface.
// So the error will be returned when you try to send the request.
// This simplifies usage, the error is checked only once, in one place.
type ReqDefinitionError struct {
	error
}

func NewReqDefinitionError(err error) Sendable {
	return ReqDefinitionError{error: err}
}

func (v ReqDefinitionError) SendOrErr(_ context.Context) error {
	return v
}

func (v ReqDefinitionError) Unwrap() error {
	return v.error
}
