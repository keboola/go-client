// nolint: dupl
package keboola

import (
	"fmt"
	"net/http"
)

// EncryptionError represents the structure Encryption API error.
type EncryptionError struct {
	Message     string `json:"error"`
	ErrCode     int    `json:"code"`
	ExceptionID string `json:"exceptionId"`
	request     *http.Request
	response    *http.Response
}

func (e *EncryptionError) Error() string {
	msg := e.Message
	if e.request != nil {
		msg += fmt.Sprintf(`, method: "%s", url: "%s"`, e.request.Method, e.request.URL)
	}
	if e.response != nil {
		msg += fmt.Sprintf(`, httpCode: "%d"`, e.StatusCode())
	}
	if e.ErrCode > 0 {
		msg += fmt.Sprintf(`, errCode: "%d"`, e.ErrCode)
	}
	if len(e.ExceptionID) > 0 {
		msg += fmt.Sprintf(`, exceptionId: "%s"`, e.ExceptionID)
	}
	return msg
}

// ErrorName returns a human-readable name of the error.
func (e *EncryptionError) ErrorName() string {
	return http.StatusText(e.ErrCode)
}

// ErrorUserMessage returns error message for end user.
func (e *EncryptionError) ErrorUserMessage() string {
	return e.Message
}

// ErrorExceptionID returns exception ID to find details in logs.
func (e *EncryptionError) ErrorExceptionID() string {
	return e.ExceptionID
}

// StatusCode returns HTTP status code.
func (e *EncryptionError) StatusCode() int {
	if e.response == nil {
		return 0
	}
	return e.response.StatusCode
}

// SetRequest method allows injection of HTTP request to the error, it implements client.errorWithRequest.
func (e *EncryptionError) SetRequest(request *http.Request) {
	e.request = request
}

// SetResponse method allows injection of HTTP response to the error, it implements client.errorWithResponse.
func (e *EncryptionError) SetResponse(response *http.Response) {
	e.response = response
}
