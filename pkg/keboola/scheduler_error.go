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
	msg := fmt.Sprintf("scheduler api error[%d]: %s", e.ErrCode, e.Message)
	if e.request != nil {
		msg += fmt.Sprintf(`, method: "%s", url: "%s"`, e.request.Method, e.request.URL)
	}
	if e.response != nil {
		msg += fmt.Sprintf(`, httpCode: "%d"`, e.StatusCode())
	}
	if len(e.ExceptionID) > 0 {
		msg += fmt.Sprintf(`, exceptionId: "%s"`, e.ExceptionID)
	}
	return msg
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
	if e.response == nil {
		return 0
	}
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
