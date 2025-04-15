package keboola

// The file contains request definitions for the Encryption API.
// Requests can be sent by any HTTP client that implements the client.Sender interface.

import (
	"fmt"
	"net/http"

	"github.com/spf13/cast"

	"github.com/keboola/go-client/pkg/request"
)

// EncryptRequest - https://keboolaencryption.docs.apiary.io/#reference/encrypt/encryption/encrypt-data
func (a *PublicAPI) EncryptRequest(projectID int, componentID ComponentID, data map[string]string) request.APIRequest[*map[string]string] {
	if componentID.String() == "" {
		panic(fmt.Errorf("the componentId parameter is required"))
	}
	result := make(map[string]string)
	req := a.newRequest(EncryptionAPI).
		WithResult(&result).
		WithMethod(http.MethodPost).
		WithURL(EncryptionAPIEncrypt).
		AndQueryParam("componentId", componentID.String()).
		AndQueryParam("projectId", cast.ToString(projectID)).
		WithJSONBody(data)
	return request.NewAPIRequest(&result, req)
}
