// Package keboola contains request definitions for all supported Keboola APIs.
// The definitions are not complete and can be extended as needed.
// Requests can be sent by any HTTP client that implements the client.Sender interface.
// It is necessary to set API host and "X-StorageApi-Token" header in the HTTP client,
// see the NewAPI function.
package keboola

import (
	"context"
	"fmt"
	"strings"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/client/trace/otel"
	"github.com/keboola/go-client/pkg/request"
)

type ServiceType string

const (
	EncryptionAPI = ServiceType("encryption")
	QueueAPI      = ServiceType("queue")
	SchedulerAPI  = ServiceType("scheduler")
	StorageAPI    = ServiceType("storage")
	WorkspacesAPI = ServiceType("sandboxes")
	// Deprecated: Syrup and old queue should no longer be used.
	// See https://changelog.keboola.com/2021-11-10-what-is-new-queue/ for information on how to migrate your project.
	SyrupAPI              = ServiceType("syrup")
	appName               = "go-client-keboola-api"
	storageAPITokenHeader = "X-StorageApi-Token" //nolint: gosec // it is not a token value
)

type API struct {
	sender request.Sender
	index  *Index
	token  string
}

func APIIndex(ctx context.Context, host string, opts ...APIOption) (*Index, error) {
	cfg := newAPIConfig(opts)
	c := newClient(host, cfg)
	return newAPI(c, nil, cfg).IndexRequest().Send(ctx)
}

func APIIndexWithComponents(ctx context.Context, host string, opts ...APIOption) (*IndexComponents, error) {
	cfg := newAPIConfig(opts)
	c := newClient(host, cfg)
	return newAPI(c, nil, cfg).IndexComponentsRequest().Send(ctx)
}

func NewAPI(ctx context.Context, host string, opts ...APIOption) (*API, error) {
	index, err := APIIndex(ctx, host, opts...)
	if err != nil {
		return nil, err
	}
	return NewAPIFromIndex(host, index, opts...), nil
}

func NewAPIFromIndex(host string, index *Index, opts ...APIOption) *API {
	cfg := newAPIConfig(opts)
	c := newClient(host, cfg)
	return newAPI(c, index, cfg)
}

func newAPI(sender request.Sender, index *Index, cfg apiConfig) *API {
	return &API{sender: sender, index: index, token: cfg.token}
}

func newClient(host string, cfg apiConfig) client.Client {
	if !strings.HasPrefix(host, "https://") && !strings.HasPrefix(host, "http://") {
		host = "https://" + host
	}

	// Get client
	var c client.Client
	if cfg.client != nil {
		c = *cfg.client
	} else {
		c = client.New()
	}

	// Set host
	c = c.WithBaseURL(host)

	// Enable telemetry
	if cfg.tracerProvider != nil || cfg.meterProvider != nil {
		c = c.WithTelemetry(cfg.tracerProvider, cfg.meterProvider, otel.WithRedactedHeaders(storageAPITokenHeader))
	}

	return c
}

func (a *API) Client() request.Sender {
	return a.sender
}

func (a *API) Index() *Index {
	return a.index
}

// WithToken returns a new authorized instance of the API.
func (a *API) WithToken(token string) *API {
	clone := *a
	clone.token = token
	return &clone
}

// CreateRequest creates request to create object according its type.
func (a *API) CreateRequest(object Object) request.APIRequest[Object] {
	switch v := object.(type) {
	case *Branch:
		return request.NewAPIRequest(object, a.CreateBranchRequest(v))
	case *Config:
		return request.NewAPIRequest(object, a.CreateConfigRequest(&ConfigWithRows{Config: v}))
	case *ConfigWithRows:
		return request.NewAPIRequest(object, a.CreateConfigRequest(v))
	case *ConfigRow:
		return request.NewAPIRequest(object, a.CreateConfigRowRequest(v))
	default:
		panic(fmt.Errorf(`unexpected type "%T"`, object))
	}
}

// UpdateRequest creates request to update object according its type.
func (a *API) UpdateRequest(object Object, changedFields []string) request.APIRequest[Object] {
	switch v := object.(type) {
	case *Branch:
		return request.NewAPIRequest(object, a.UpdateBranchRequest(v, changedFields))
	case *ConfigWithRows:
		return request.NewAPIRequest(object, a.UpdateConfigRequest(v.Config, changedFields))
	case *Config:
		return request.NewAPIRequest(object, a.UpdateConfigRequest(v, changedFields))
	case *ConfigRow:
		return request.NewAPIRequest(object, a.UpdateConfigRowRequest(v, changedFields))
	default:
		panic(fmt.Errorf(`unexpected type "%T"`, object))
	}
}

// DeleteRequest creates request to delete object according its type.
func (a *API) DeleteRequest(key any) request.APIRequest[request.NoResult] {
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
func (a *API) AppendMetadataRequest(key any, metadata map[string]string) request.APIRequest[request.NoResult] {
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
func (a *API) DeleteMetadataRequest(key any, metaID string) request.APIRequest[request.NoResult] {
	switch v := key.(type) {
	case BranchKey:
		return a.DeleteBranchMetadataRequest(v, metaID)
	case ConfigKey:
		return a.DeleteConfigMetadataRequest(v, metaID)
	default:
		panic(fmt.Errorf(`unexpected type "%T"`, key))
	}
}
