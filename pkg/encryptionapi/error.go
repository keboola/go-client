package encryptionapi

import (
	"fmt"
	"net/http"
)

// Error represents the structure Encryption API error.
type Error struct {
	Message     string `json:"error"`
	ErrCode     int    `json:"code"`
	ExceptionID string `json:"exceptionId"`
	request     *http.Request
	response    *http.Response
}

func (e Error) Error() string {
	req := e.request
	msg := fmt.Sprintf(`%s, method: "%s", url: "%s", httpCode: "%d"`, e.Message, req.Method, req.URL, e.StatusCode())
	if e.ErrCode > 0 {
		msg += fmt.Sprintf(`, errCode: "%d"`, e.ErrCode)
	}
	if len(e.ExceptionID) > 0 {
		msg += fmt.Sprintf(`, exceptionId: "%s"`, e.ExceptionID)
	}
	return msg
}

// ErrorName returns a human-readable name of the error.
func (e Error) ErrorName() string {
	return http.StatusText(e.ErrCode)
}

// ErrorUserMessage returns error message for end user.
func (e Error) ErrorUserMessage() string {
	return e.Message
}

// ErrorExceptionID returns exception ID to find details in logs.
func (e Error) ErrorExceptionID() string {
	return e.ExceptionID
}

// StatusCode returns HTTP status code.
func (e Error) StatusCode() int {
	return e.response.StatusCode
}

// SetRequest method allows injection of HTTP request to the error, it implements client.errorWithRequest.
func (e *Error) SetRequest(request *http.Request) {
	e.request = request
}

// SetResponse method allows injection of HTTP response to the error, it implements client.errorWithResponse.
func (e *Error) SetResponse(response *http.Response) {
	e.response = response
}
