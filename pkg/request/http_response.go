package request

import "net/http"

// HTTPResponse with response mapped to the Result() value.
type HTTPResponse interface {
	httpRequestReadOnly
	httpResponseCommon
	// Result method returns the response mapped as a data type, if any.
	Result() any
}

type httpResponseCommon interface {
	// ResponseHeader method returns HTTP response headers.
	ResponseHeader() http.Header
	// StatusCode method returns HTTP status code.
	StatusCode() int
	// RawRequest method returns the standard HTTP request, from the last retry attempt.
	RawRequest() *http.Request
	// RawResponse method returns the standard HTTP response.
	RawResponse() *http.Response
	// IsSuccess method returns true if HTTP status `code >= 200 and <= 299` otherwise false.
	IsSuccess() bool
	// IsError method returns true if HTTP status `code >= 400` otherwise false.
	IsError() bool
	// Error method returns the error response mapped to a data type specified by ErrorDef if any.
	// It can also return native HTTP errors, e.g. some network problems.
	Error() error
}

// httpResponse implements HTTPResponse interface.
type httpResponse struct {
	httpRequest
	rawResponse *http.Response
	result      any
	err         error
}

func (r httpResponse) ResponseHeader() http.Header {
	return r.rawResponse.Header
}

func (r httpResponse) StatusCode() int {
	return r.rawResponse.StatusCode
}

func (r httpResponse) RawRequest() *http.Request {
	if r.rawResponse != nil && r.rawResponse.Request != nil {
		return r.rawResponse.Request
	}
	return nil
}

func (r httpResponse) RawResponse() *http.Response {
	return r.rawResponse
}

func (r httpResponse) IsSuccess() bool {
	return r.StatusCode() > 199 && r.StatusCode() < 300
}

func (r httpResponse) IsError() bool {
	return r.StatusCode() > 399
}

func (r httpResponse) Result() any {
	return r.result
}

func (r httpResponse) Error() error {
	return r.err
}
