package sandboxesapi

import (
	"context"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/keboola"
)

type params struct {
	Type             string
	Shared           bool
	ExpireAfterHours uint64
	Size             string
	ImageVersion     string
}

type Option func(p *params)

func WithShared(v bool) Option {
	return func(p *params) { p.Shared = v }
}

func WithExpireAfterHours(v uint64) Option {
	return func(p *params) { p.ExpireAfterHours = v }
}

func WithSize(v string) Option {
	return func(p *params) { p.Size = v }
}

func WithImageVersion(v string) Option {
	return func(p *params) { p.ImageVersion = v }
}

func newParams(type_ string, opts ...Option) params {
	p := params{
		Type:             type_,
		Shared:           false,
		ExpireAfterHours: 0,
	}
	for _, opt := range opts {
		opt(&p)
	}
	return p
}

func (p params) toMap() map[string]any {
	m := map[string]any{
		"task":                 "create",
		"type":                 p.Type,
		"shared":               p.Shared,
		"expirationAfterHours": p.ExpireAfterHours,
	}
	if len(p.Size) > 0 {
		m["size"] = p.Size
	}
	if len(p.ImageVersion) > 0 {
		m["imageVersion"] = p.ImageVersion
	}
	return m
}

func CreateJobRequest(configId ConfigID, sandboxType string, opts ...Option) client.APIRequest[client.NoResult] {
	params := newParams(sandboxType, opts...)
	request := keboola.CreateJobConfigDataRequest(Component, configId, map[string]any{"parameters": params.toMap()}).
		WithOnSuccess(func(ctx context.Context, sender client.Sender, result *keboola.QueueJob) error {
			return keboola.WaitForQueueJob(ctx, sender, result)
		})
	return client.NewAPIRequest(client.NoResult{}, request)
}

func DeleteJobRequest(sandboxId SandboxID) client.APIRequest[client.NoResult] {
	configData := map[string]any{
		"parameters": map[string]any{
			"task": "delete",
			"id":   sandboxId.String(),
		},
	}
	request := keboola.CreateJobConfigDataRequest(Component, "", configData).
		WithOnSuccess(func(ctx context.Context, sender client.Sender, result *keboola.QueueJob) error {
			return keboola.WaitForQueueJob(ctx, sender, result)
		})
	return client.NewAPIRequest(client.NoResult{}, request)
}
