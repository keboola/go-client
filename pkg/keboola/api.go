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
	SyrupAPI = ServiceType("syrup")
)

// newRequest Creates request, sets base URL and default error type.
func (a *API) newRequest(s ServiceType) request.HTTPRequest {
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

func (a *API) baseURLForService(s ServiceType) string {
	if s == StorageAPI {
		return "v2/storage"
	}

	url, found := a.index.Services.ToMap().URLByID(ServiceID(s))
	if !found {
		panic(fmt.Errorf(`service not found "%s"`, s))
	}
	return url.String()
}

type API struct {
	sender request.Sender
	index  *Index
}

type apiConfig struct {
	client *client.Client
	token  string
}

type APIOption func(c *apiConfig)

func WithClient(cl *client.Client) APIOption {
	return func(c *apiConfig) {
		c.client = cl
	}
}

func WithToken(token string) APIOption {
	return func(c *apiConfig) {
		c.token = token
	}
}

type Object interface {
	ObjectID() any
}

func APIIndex(ctx context.Context, host string, opts ...APIOption) (*Index, error) {
	c := newClient(host, opts)
	return (&API{sender: c}).IndexRequest().Send(ctx)
}

func APIIndexWithComponents(ctx context.Context, host string, opts ...APIOption) (*IndexComponents, error) {
	c := newClient(host, opts)
	return (&API{sender: c}).IndexComponentsRequest().Send(ctx)
}

func NewAPI(ctx context.Context, host string, opts ...APIOption) (*API, error) {
	index, err := APIIndex(ctx, host, opts...)
	if err != nil {
		return nil, err
	}
	return NewAPIFromIndex(host, index, opts...), nil
}

func NewAPIFromIndex(host string, index *Index, opts ...APIOption) *API {
	c := newClient(host, opts)
	return &API{sender: c, index: index}
}

func newClient(host string, opts []APIOption) client.Client {
	if !strings.HasPrefix(host, "https://") && !strings.HasPrefix(host, "http://") {
		host = "https://" + host
	}
	config := apiConfig{}
	for _, opt := range opts {
		opt(&config)
	}
	var c client.Client
	if config.client != nil {
		c = *config.client
	} else {
		c = client.New()
	}
	if config.token != "" {
		c = c.WithHeader("X-StorageApi-Token", config.token)
	}
	c = c.WithBaseURL(host)
	return c
}

func (a *API) Client() request.Sender {
	return a.sender
}

func (a *API) Index() *Index {
	return a.index
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
