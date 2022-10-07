// Package storageapi contains request definitions for the Storage API.
// The definitions are not complete and can be extended as needed.
// Requests can be sent by any HTTP client that implements the client.Sender interface.
// It is necessary to set API host and "X-StorageApi-Token" header in the HTTP client,
// see the ClientWithHost and the ClientWithHostAndToken functions.
package storageapi

import (
	"fmt"
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
	return client.NewHTTPRequest().WithBaseURL("v2/storage").WithError(&Error{})
}

type Object interface {
	ObjectId() any
}

// CreateRequest creates request to create object according its type.
func CreateRequest(object Object) client.APIRequest[Object] {
	switch v := object.(type) {
	case *Branch:
		return client.NewAPIRequest(object, CreateBranchRequest(v))
	case *Config:
		return client.NewAPIRequest(object, CreateConfigRequest(&ConfigWithRows{Config: v}))
	case *ConfigWithRows:
		return client.NewAPIRequest(object, CreateConfigRequest(v))
	case *ConfigRow:
		return client.NewAPIRequest(object, CreateConfigRowRequest(v))
	default:
		panic(fmt.Errorf(`unexpected type "%T"`, object))
	}
}

// UpdateRequest creates request to update object according its type.
func UpdateRequest(object Object, changedFields []string) client.APIRequest[Object] {
	switch v := object.(type) {
	case *Branch:
		return client.NewAPIRequest(object, UpdateBranchRequest(v, changedFields))
	case *ConfigWithRows:
		return client.NewAPIRequest(object, UpdateConfigRequest(v.Config, changedFields))
	case *Config:
		return client.NewAPIRequest(object, UpdateConfigRequest(v, changedFields))
	case *ConfigRow:
		return client.NewAPIRequest(object, UpdateConfigRowRequest(v, changedFields))
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
func AppendMetadataRequest(key any, metadata map[string]string) client.APIRequest[client.NoResult] {
	switch v := key.(type) {
	case BranchKey:
		return AppendBranchMetadataRequest(v, metadata)
	case ConfigKey:
		return AppendConfigMetadataRequest(v, metadata)
	default:
		panic(fmt.Errorf(`unexpected type "%T"`, key))
	}
}

// DeleteMetadataRequest creates request to delete object metadata according its type.
func DeleteMetadataRequest(key any, metaID string) client.APIRequest[client.NoResult] {
	switch v := key.(type) {
	case BranchKey:
		return DeleteBranchMetadataRequest(v, metaID)
	case ConfigKey:
		return DeleteConfigMetadataRequest(v, metaID)
	default:
		panic(fmt.Errorf(`unexpected type "%T"`, key))
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

// DeleteOption for requests to delete bucket or table.
type DeleteOption func(c *deleteConfig)

type deleteConfig struct {
	force bool
}

func WithForce() DeleteOption {
	return func(c *deleteConfig) {
		c.force = true
	}
}
