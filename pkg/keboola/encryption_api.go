// Contains request definitions for the Encryption API.
// Requests can be sent by any HTTP client that implements the client.Sender interface.
package keboola

import (
	"fmt"
	"net/http"

	"github.com/spf13/cast"

	"github.com/keboola/go-client/pkg/client"
)

// EncryptRequest - https://keboolaencryption.docs.apiary.io/#reference/encrypt/encryption/encrypt-data
func (a *API) EncryptRequest(projectID int, componentID ComponentID, data map[string]string) client.APIRequest[*map[string]string] {
	if componentID.String() == "" {
		panic(fmt.Errorf("the componentId parameter is required"))
	}
	result := make(map[string]string)
	request := a.newRequest(EncryptionAPI).
		WithResult(&result).
		WithMethod(http.MethodPost).
		WithURL("encrypt").
		AndQueryParam("componentId", componentID.String()).
		AndQueryParam("projectId", cast.ToString(projectID)).
		WithJSONBody(data)
	return client.NewAPIRequest(&result, request)
}
