package keboola

import (
	"fmt"
	"net/http"
)

// QueueError represents the structure of Jobs Queue API error.
type QueueError struct {
	Message     string `json:"error"`
	ErrCode     int    `json:"code"`
	ExceptionID string `json:"exceptionId"`
	request     *http.Request
	response    *http.Response
}

func (e *QueueError) Error() string {
	return fmt.Sprintf("jobs queue api error[%d]: %s", e.ErrCode, e.Message)
}

// ErrorName returns a human-readable name of the error.
func (e *QueueError) ErrorName() string {
	return http.StatusText(e.ErrCode)
}

// ErrorUserMessage returns error message for end user.
func (e *QueueError) ErrorUserMessage() string {
	return e.Message
}

// ErrorExceptionID returns exception ID to find details in logs.
func (e *QueueError) ErrorExceptionID() string {
	return e.ExceptionID
}

// StatusCode returns HTTP status code.
func (e *QueueError) StatusCode() int {
	return e.response.StatusCode
}

// SetRequest method allows injection of HTTP request to the error, it implements client.errorWithRequest.
func (e *QueueError) SetRequest(request *http.Request) {
	e.request = request
}

// SetResponse method allows injection of HTTP response to the error, it implements client.errorWithResponse.
func (e *QueueError) SetResponse(response *http.Response) {
	e.response = response
}
