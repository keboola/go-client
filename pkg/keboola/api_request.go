package keboola

import (
	"fmt"

	"github.com/keboola/go-client/pkg/request"
)

// newRequest adds Storage API token header.
func (a *AuthorizedAPI) newRequest(s ServiceType) request.HTTPRequest {
	// Authorize
	return a.PublicAPI.newRequest(s).AndHeader(storageAPITokenHeader, a.token)
}

// newRequest Creates request, sets base URL and default error type.
func (a *PublicAPI) newRequest(s ServiceType) request.HTTPRequest {
	// Set request base URL according to the ServiceType
	r := request.NewHTTPRequest(a.sender).WithBaseURL(a.baseURLForService(s))

	// Set error schema
	switch s {
	case StorageAPI:
		r = r.WithError(&StorageError{})
	case EncryptionAPI:
		r = r.WithError(&EncryptionError{})
	case QueueAPI:
		r = r.WithError(&QueueError{})
	case SchedulerAPI:
		r = r.WithError(&SchedulerError{})
	case WorkspacesAPI:
		r = r.WithError(&WorkspacesError{})
	}

	return r
}

func (a *PublicAPI) baseURLForService(s ServiceType) string {
	if s == StorageAPI {
		return "v2/storage"
	}

	url, found := a.index.Services.ToMap().URLByID(ServiceID(s))
	if !found {
		panic(fmt.Errorf(`service not found "%s"`, s))
	}
	return url.String()
}
