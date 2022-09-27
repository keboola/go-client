package sandboxapi

import (
	"fmt"
	"net/http"
)

// Error represents the structure of Storage API error.
type Error struct {
	Message   string `json:"messsage"`
	ErrorInfo string `json:"error"`
	request   *http.Request
	response  *http.Response
}

func (e Error) Error() string {
	if e.request == nil {
		panic(fmt.Errorf("http request is not set"))
	}
	if e.response == nil {
		panic(fmt.Errorf("http response is not set"))
	}
	msg := fmt.Sprintf(`%s, method: "%s", url: "%s", httpCode: "%d"`, e.Message, e.request.Method, e.request.URL, e.StatusCode())
	return msg
}

// ErrorName returns a human-readable name of the error.
func (e Error) ErrorName() string {
	return e.ErrorInfo
}

// ErrorUserMessage returns error message for end user.
func (e Error) ErrorUserMessage() string {
	return e.Message
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
