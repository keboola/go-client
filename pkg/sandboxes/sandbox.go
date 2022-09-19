package sandboxes

import (
	"context"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/jobsqueueapi"
	"github.com/keboola/go-client/pkg/storageapi"
)

const componentId = "keboola.sandboxes"

const (
	SandboxSizeSmall  = "small"
	SandboxSizeMedium = "medium"
	SandboxSizeLarge  = "large"
)

type SandboxParams struct {
	Name             string
	Type             string
	Shared           bool
	ExpireAfterHours uint64
	Size             string
	ImageVersion     string
}

func CreateSandboxRequest(branchId storageapi.BranchID, params SandboxParams) client.APIRequest[*storageapi.ConfigWithRows] {
	config := &storageapi.ConfigWithRows{
		Config: &storageapi.Config{
			ConfigKey: storageapi.ConfigKey{
				BranchID:    branchId,
				ComponentID: componentId,
			},
			Name: params.Name,
		},
	}
	jobParams := map[string]any{
		"task":                 "create",
		"type":                 params.Type,
		"shared":               params.Shared,
		"expirationAfterHours": params.ExpireAfterHours,
		"size":                 params.Size,
	}
	if params.ImageVersion != "" {
		jobParams["imageVersion"] = params.ImageVersion
	}

	request := storageapi.CreateConfigRequest(config).
		WithOnSuccess(func(ctx context.Context, sender client.Sender, result *storageapi.ConfigWithRows) error {
			job, err := jobsqueueapi.
				CreateJobConfigDataRequest(componentId, result.Config.ID, map[string]any{"parameters": jobParams}).
				Send(ctx, sender)
			if err != nil {
				return err
			}
			return jobsqueueapi.WaitForJob(ctx, sender, job)
		})
	return client.NewAPIRequest(config, request)
}

func DeleteSandboxRequest(configId storageapi.ConfigID) client.APIRequest[client.NoResult] {
	parameters := map[string]any{
		"task": "delete",
		"id":   configId,
	}
	request := jobsqueueapi.
		CreateJobConfigDataRequest(componentId, configId, map[string]any{"parameters": parameters}).
		WithOnSuccess(func(ctx context.Context, sender client.Sender, result *jobsqueueapi.Job) error {
			return jobsqueueapi.WaitForJob(ctx, sender, result)
		})
	return client.NewAPIRequest(client.NoResult{}, request)
}
