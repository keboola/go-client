package sandboxapi

import (
	"strings"

	"github.com/keboola/go-client/pkg/client"
)

// ClientWithHost returns HTTP client with api host set.
func ClientWithHost(c client.Client, apiHost string) client.Client {
	apiHost = strings.TrimPrefix(apiHost, "https://")
	return c.WithBaseURL(`https://` + apiHost)
}

// ClientWithToken returns HTTP client with api token set.
func ClientWithToken(c client.Client, apiToken string) client.Client {
	return c.WithHeader("X-StorageApi-Token", apiToken)
}

// ClientWithHostAndToken returns HTTP client with api host and token set.
func ClientWithHostAndToken(c client.Client, apiHost, apiToken string) client.Client {
	return ClientWithToken(ClientWithHost(c, apiHost), apiToken)
}

func newRequest() client.HTTPRequest {
	// Create request and set default error type
	return client.NewHTTPRequest().WithError(&Error{})
}
