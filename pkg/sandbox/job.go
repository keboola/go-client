package sandbox

import (
	"context"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/jobsqueueapi"
)

type Params struct {
	Type             string
	Shared           bool
	ExpireAfterHours uint64
	Size             string
	ImageVersion     string
}

func CreateJobRequest(configId ConfigID, sandbox Params) client.APIRequest[client.NoResult] {
	parameters := map[string]any{
		"task":                 "create",
		"type":                 sandbox.Type,
		"shared":               sandbox.Shared,
		"expirationAfterHours": sandbox.ExpireAfterHours,
		"size":                 sandbox.Size,
	}
	if sandbox.ImageVersion != "" {
		parameters["imageVersion"] = sandbox.ImageVersion
	}
	configData := map[string]any{
		"parameters": parameters,
	}

	request := jobsqueueapi.
		CreateJobConfigDataRequest(Component, configId, configData).
		WithOnSuccess(func(ctx context.Context, sender client.Sender, result *jobsqueueapi.Job) error {
			return jobsqueueapi.WaitForJob(ctx, sender, result)
		})
	return client.NewAPIRequest(client.NoResult{}, request)
}

func DeleteJobRequest(configId ConfigID, sandboxId SandboxID) client.APIRequest[client.NoResult] {
	configData := map[string]any{
		"parameters": map[string]any{
			"task": "delete",
			"id":   sandboxId.String(),
		},
	}
	request := jobsqueueapi.
		CreateJobConfigDataRequest(Component, configId, configData).
		WithOnSuccess(func(ctx context.Context, sender client.Sender, result *jobsqueueapi.Job) error {
			return jobsqueueapi.WaitForJob(ctx, sender, result)
		})
	return client.NewAPIRequest(client.NoResult{}, request)
}
