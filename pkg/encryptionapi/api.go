// Package encryptionapi contains request definitions for the Encryption API.
// Requests can be sent by any HTTP client that implements the client.Sender interface.
// It is necessary to set API host in the HTTP client, see the ClientWithHost function.
package encryptionapi

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/spf13/cast"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/storageapi"
)

// ComponentID is id of a Keboola component.
type ComponentID = storageapi.ComponentID

// ClientWithHost returns HTTP client with api host set.
func ClientWithHost(c client.Client, apiHost string) client.Client {
	apiHost = strings.TrimPrefix(apiHost, "https://")
	return c.WithBaseURL(`https://` + apiHost)
}

func newRequest() client.HTTPRequest {
	// Create request and set default error type
	return client.NewHTTPRequest().WithError(&Error{})
}

// EncryptRequest - https://keboolaencryption.docs.apiary.io/#reference/encrypt/encryption/encrypt-data
func EncryptRequest(projectID int, componentID ComponentID, data map[string]string) client.APIRequest[*map[string]string] {
	if componentID.String() == "" {
		panic(fmt.Errorf("the componentId parameter is required"))
	}
	result := make(map[string]string)
	request := newRequest().
		WithResult(&result).
		WithMethod(http.MethodPost).
		WithURL("encrypt").
		AndQueryParam("componentId", componentID.String()).
		AndQueryParam("projectId", cast.ToString(projectID)).
		WithJSONBody(data)
	return client.NewAPIRequest(&result, request)
}
