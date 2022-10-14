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

type ServiceType string

const StorageAPI = ServiceType("storage api")

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

func (a *Api) newRequest(s ServiceType) client.HTTPRequest {
	switch s {
	case StorageAPI:
		// Create request, set base URL and default error type
		return client.
			NewHTTPRequest(a.senderForService(s)).
			WithBaseURL("v2/storage").
			WithError(&Error{})
	default:
		panic(fmt.Errorf(`unexpected service "%s"`, s))
	}
}

func (a *Api) senderForService(s ServiceType) client.Sender {
	// TODO, API should contains hosts for all Services
	return nil
}

type Api struct {
	sender client.Sender
}

type Object interface {
	ObjectId() any
}

// CreateRequest creates request to create object according its type.
func (a *Api) CreateRequest(object Object) client.APIRequest[Object] {
	switch v := object.(type) {
	case *Branch:
		return client.NewAPIRequest(object, a.CreateBranchRequest(v))
	case *Config:
		return client.NewAPIRequest(object, a.CreateConfigRequest(&ConfigWithRows{Config: v}))
	case *ConfigWithRows:
		return client.NewAPIRequest(object, a.CreateConfigRequest(v))
	case *ConfigRow:
		return client.NewAPIRequest(object, a.CreateConfigRowRequest(v))
	default:
		panic(fmt.Errorf(`unexpected type "%T"`, object))
	}
}

// UpdateRequest creates request to update object according its type.
func (a *Api) UpdateRequest(object Object, changedFields []string) client.APIRequest[Object] {
	switch v := object.(type) {
	case *Branch:
		return client.NewAPIRequest(object, a.UpdateBranchRequest(v, changedFields))
	case *ConfigWithRows:
		return client.NewAPIRequest(object, a.UpdateConfigRequest(v.Config, changedFields))
	case *Config:
		return client.NewAPIRequest(object, a.UpdateConfigRequest(v, changedFields))
	case *ConfigRow:
		return client.NewAPIRequest(object, a.UpdateConfigRowRequest(v, changedFields))
	default:
		panic(fmt.Errorf(`unexpected type "%T"`, object))
	}
}

// DeleteRequest creates request to delete object according its type.
func (a *Api) DeleteRequest(key any) client.APIRequest[client.NoResult] {
	switch k := key.(type) {
	case BranchKey:
		return a.DeleteBranchRequest(k)
	case ConfigKey:
		return a.DeleteConfigRequest(k)
	case ConfigRowKey:
		return a.DeleteConfigRowRequest(k)
	default:
		panic(fmt.Errorf(`unexpected type "%T"`, key))
	}
}

// AppendMetadataRequest creates request to append object metadata according its type.
func (a *Api) AppendMetadataRequest(key any, metadata map[string]string) client.APIRequest[client.NoResult] {
	switch v := key.(type) {
	case BranchKey:
		return a.AppendBranchMetadataRequest(v, metadata)
	case ConfigKey:
		return a.AppendConfigMetadataRequest(v, metadata)
	default:
		panic(fmt.Errorf(`unexpected type "%T"`, key))
	}
}

// DeleteMetadataRequest creates request to delete object metadata according its type.
func (a *Api) DeleteMetadataRequest(key any, metaID string) client.APIRequest[client.NoResult] {
	switch v := key.(type) {
	case BranchKey:
		return a.DeleteBranchMetadataRequest(v, metaID)
	case ConfigKey:
		return a.DeleteConfigMetadataRequest(v, metaID)
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
