// Package storageapi contains request definitions for the Storage API.
// The definitions are not complete and can be extended as needed.
// Requests can be sent by any HTTP client that implements the client.Sender interface.
// It is necessary to set API host and "X-StorageApi-Token" header in the HTTP client,
// see the APIClient and the APIClientWithToken functions.
package storageapi

import (
	"fmt"
	"strings"

	"github.com/keboola/go-client/pkg/client"
)

// APIClient creates HTTP client with api host set.
func APIClient(c client.Client, apiHost string) client.Client {
	apiHost = strings.TrimPrefix(apiHost, "https://")
	return c.WithBaseURL(`https://` + apiHost)
}

// APIClientWithToken creates HTTP client with api host and token set.
func APIClientWithToken(c client.Client, apiHost, apiToken string) client.Client {
	return APIClient(c, apiHost).WithHeader("X-StorageApi-Token", apiToken)
}

func newRequest() client.HTTPRequest {
	// Create request and set default error type
	return client.NewHTTPRequest().WithBaseURL("v2/storage").WithError(&Error{})
}

// CreateRequest creates request to create object according its type.
func CreateRequest[R client.Result](object R) client.Sendable {
	switch v := any(object).(type) {
	case *Branch:
		return CreateBranchRequest(v)
	case *Config:
		return CreateConfigRequest(&ConfigWithRows{Config: v})
	case *ConfigWithRows:
		return CreateConfigRequest(v)
	case *ConfigRow:
		return CreateConfigRowRequest(v)
	default:
		panic(fmt.Errorf(`unexpected type "%T"`, object))
	}
}

// UpdateRequest creates request to update object according its type.
func UpdateRequest[R client.Result](object R, changedFields []string) client.Sendable {
	switch v := any(object).(type) {
	case *Branch:
		return UpdateBranchRequest(v, changedFields)
	case *ConfigWithRows:
		return UpdateConfigRequest(v.Config, changedFields)
	case *Config:
		return UpdateConfigRequest(v, changedFields)
	case *ConfigRow:
		return UpdateConfigRowRequest(v, changedFields)
	default:
		panic(fmt.Errorf(`unexpected type "%T"`, object))
	}
}

// DeleteRequest creates request to delete object according its type.
func DeleteRequest(key any) client.APIRequest[client.NoResult] {
	switch k := key.(type) {
	case BranchKey:
		return DeleteBranchRequest(k)
	case ConfigKey:
		return DeleteConfigRequest(k)
	case ConfigRowKey:
		return DeleteConfigRowRequest(k)
	default:
		panic(fmt.Errorf(`unexpected type "%T"`, key))
	}
}

// AppendMetadataRequest creates request to append object metadata according its type.
func AppendMetadataRequest(object any, metadata map[string]string) client.APIRequest[client.NoResult] {
	switch v := object.(type) {
	case *Branch:
		return AppendBranchMetadataRequest(v.BranchKey, metadata)
	case *ConfigWithRows:
		return AppendConfigMetadataRequest(v.ConfigKey, metadata)
	case *Config:
		return AppendConfigMetadataRequest(v.ConfigKey, metadata)
	default:
		panic(fmt.Errorf(`unexpected type "%T"`, object))
	}
}

// Metadata - object metadata.
type Metadata map[string]string

// MetadataDetails - metadata with details (id, timestamp).
type MetadataDetails []MetadataDetail

// MetadataDetail - metadata with details (id, timestamp).
type MetadataDetail struct {
	ID        string `json:"id"`
	Key       string `json:"key"`
	Value     string `json:"value"`
	Timestamp string `json:"timestamp"`
}

// ToMap converts slice to map.
func (v MetadataDetails) ToMap() Metadata {
	out := make(Metadata)
	for _, item := range v {
		out[item.Key] = item.Value
	}
	return out
}
