// Package keboola contains request definitions for all supported Keboola APIs.
// The definitions are not complete and can be extended as needed.
// Requests can be sent by any HTTP client that implements the client.Sender interface.
// It is necessary to set API host and "X-StorageApi-Token" header in the HTTP client,
// see the NewAPI function.
package keboola

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

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

type PublicAPI struct {
	sender request.Sender
	index  *Index
}

type AuthorizedAPI struct {
	*PublicAPI
	onSuccessTimeout time.Duration
	token            string
}

func APIIndex(ctx context.Context, host string, opts ...APIOption) (*Index, error) {
	cfg := newAPIConfig(opts)
	c := newClient(host, cfg)
	return newPublicAPI(c, nil).IndexRequest().Send(ctx)
}

func APIIndexWithComponents(ctx context.Context, host string, opts ...APIOption) (*IndexComponents, error) {
	cfg := newAPIConfig(opts)
	c := newClient(host, cfg)
	return newPublicAPI(c, nil).IndexComponentsRequest().Send(ctx)
}

func NewAuthorizedAPI(ctx context.Context, host, token string, opts ...APIOption) (*AuthorizedAPI, error) {
	if token == "" {
		panic(errors.New("token must be specified"))
	}

	publicAPI, err := NewPublicAPI(ctx, host, opts...)
	if err != nil {
		return nil, err
	}

	cfg := newAPIConfig(opts)
	authorizedAPI := publicAPI.NewAuthorizedAPI(token, cfg.onSuccessTimeout)
	return authorizedAPI, nil
}

func NewPublicAPI(ctx context.Context, host string, opts ...APIOption) (*PublicAPI, error) {
	index, err := APIIndex(ctx, host, opts...)
	if err != nil {
		return nil, err
	}
	return NewPublicAPIFromIndex(host, index, opts...), nil
}

func NewPublicAPIFromIndex(host string, index *Index, opts ...APIOption) *PublicAPI {
	cfg := newAPIConfig(opts)
	c := newClient(host, cfg)
	return newPublicAPI(c, index)
}

func newPublicAPI(sender request.Sender, index *Index) *PublicAPI {
	return &PublicAPI{sender: sender, index: index}
}

func newClient(host string, cfg apiConfig) client.Client {
	if host == "" {
		panic(errors.New("host must be specified"))
	}

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

func (a *PublicAPI) Client() request.Sender {
	return a.sender
}

func (a *PublicAPI) Index() *Index {
	return a.index
}

// NewAuthorizedAPI returns a new authorized instance of the API.
func (a *PublicAPI) NewAuthorizedAPI(token string, timeout time.Duration) *AuthorizedAPI {
	return &AuthorizedAPI{
		PublicAPI:        a,
		token:            token,
		onSuccessTimeout: timeout,
	}
}

// CreateRequest creates request to create object according its type.
func (a *AuthorizedAPI) CreateRequest(object Object) request.APIRequest[Object] {
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
func (a *AuthorizedAPI) UpdateRequest(object Object, changedFields []string) request.APIRequest[Object] {
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
func (a *AuthorizedAPI) DeleteRequest(key any) request.APIRequest[request.NoResult] {
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
func (a *AuthorizedAPI) AppendMetadataRequest(key any, metadata map[string]string) request.APIRequest[request.NoResult] {
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
func (a *AuthorizedAPI) DeleteMetadataRequest(key any, metaID string) request.APIRequest[request.NoResult] {
	switch v := key.(type) {
	case BranchKey:
		return a.DeleteBranchMetadataRequest(v, metaID)
	case ConfigKey:
		return a.DeleteConfigMetadataRequest(v, metaID)
	default:
		panic(fmt.Errorf(`unexpected type "%T"`, key))
	}
}
