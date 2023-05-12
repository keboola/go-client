// Package request provides to define immutable HTTP requests, see NewHTTPRequest function.
//
// Requests are sent using the Sender interface.
// The client.Client is a default implementation of the request.Sender
// interface based on the standard net/http package.
//
// APIRequest[R Result] is a generic type that wraps one or more HTTPRequest.
// It contains target data type to which the API response will be mapped.
// Use NewAPIRequest function to create a APIRequest from a HTTPRequest.
//
// RunGroup, WaitGroup, ParallelAPIRequests are helpers for concurrent requests.
package request
