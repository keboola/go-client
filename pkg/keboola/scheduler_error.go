// nolint: dupl
package keboola

import (
	"fmt"
	"net/http"
)

// SchedulerError represents the structure Scheduler API error.
type SchedulerError struct {
	Message     string `json:"error"`
	ErrCode     int    `json:"code"`
	ExceptionID string `json:"exceptionId"`
	request     *http.Request
	response    *http.Response
}

func (e *SchedulerError) Error() string {
	return fmt.Sprintf("scheduler api error[%d]: %s", e.ErrCode, e.Message)
}

// ErrorName returns a human-readable name of the error.
func (e *SchedulerError) ErrorName() string {
	return http.StatusText(e.ErrCode)
}

// ErrorUserMessage returns error message for end user.
func (e *SchedulerError) ErrorUserMessage() string {
	return e.Message
}

// ErrorExceptionID returns exception ID to find details in logs.
func (e *SchedulerError) ErrorExceptionID() string {
	return e.ExceptionID
}

// StatusCode returns HTTP status code.
func (e *SchedulerError) StatusCode() int {
	return e.response.StatusCode
}

// SetRequest method allows injection of HTTP request to the error, it implements client.errorWithRequest.
func (e *SchedulerError) SetRequest(request *http.Request) {
	e.request = request
}

// SetResponse method allows injection of HTTP response to the error, it implements client.errorWithResponse.
func (e *SchedulerError) SetResponse(response *http.Response) {
	e.response = response
}
