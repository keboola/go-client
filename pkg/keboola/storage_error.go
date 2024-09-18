// nolint: dupl
package keboola

import (
	"fmt"
	"net/http"
)

// StorageError represents the structure of Storage API error.
type StorageError struct {
	Message     string `json:"error"`
	ErrCode     string `json:"code"`
	ExceptionID string `json:"exceptionId"`
	request     *http.Request
	response    *http.Response
}

func (e *StorageError) Error() string {
	msg := e.Message

	if len(e.ExceptionID) > 0 {
		msg += fmt.Sprintf(` (%s)`, e.ExceptionID)
	}
	return msg
}

// ErrorName returns a human-readable name of the error.
func (e *StorageError) ErrorName() string {
	return e.ErrCode
}

// ErrorUserMessage returns error message for end user.
func (e *StorageError) ErrorUserMessage() string {
	return e.Message
}

// ErrorExceptionID returns exception ID to find details in logs.
func (e *StorageError) ErrorExceptionID() string {
	return e.ExceptionID
}

// StatusCode returns HTTP status code.
func (e *StorageError) StatusCode() int {
	if e.response == nil {
		return 0
	}
	return e.response.StatusCode
}

// SetRequest method allows injection of HTTP request to the error, it implements client.errorWithRequest.
func (e *StorageError) SetRequest(request *http.Request) {
	e.request = request
}

// SetResponse method allows injection of HTTP response to the error, it implements client.errorWithResponse.
func (e *StorageError) SetResponse(response *http.Response) {
	e.response = response
}
