package sandbox

import (
	"context"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/jobsqueueapi"
	"github.com/keboola/go-client/pkg/storageapi"
)

type BranchID = storageapi.BranchID
type ConfigID = storageapi.ConfigID

type SandboxID string

func (v SandboxID) String() string {
	return string(v)
}

const Component = "keboola.sandboxes"

const (
	SizeSmall  = "small"
	SizeMedium = "medium"
	SizeLarge  = "large"
)

type Params struct {
	Type             string
	Shared           bool
	ExpireAfterHours uint64
	Size             string
	ImageVersion     string
}

func GetSandboxConfigRequest(branchId BranchID, configId ConfigID) client.APIRequest[*storageapi.Config] {
	key := storageapi.ConfigKey{
		BranchID:    branchId,
		ComponentID: Component,
		ID:          configId,
	}
	return storageapi.GetConfigRequest(key)
}

func CreateSandboxConfigRequest(branchId BranchID, name string) client.APIRequest[*storageapi.ConfigWithRows] {
	config := &storageapi.ConfigWithRows{
		Config: &storageapi.Config{
			ConfigKey: storageapi.ConfigKey{
				BranchID:    branchId,
				ComponentID: Component,
			},
			Name: name,
		},
	}
	return storageapi.CreateConfigRequest(config)
}

func CreateSandboxJobRequest(configId ConfigID, sandbox Params) client.APIRequest[client.NoResult] {
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

func DeleteSandboxConfigRequest(branchId BranchID, configId ConfigID) client.APIRequest[client.NoResult] {
	request := storageapi.DeleteConfigRequest(storageapi.ConfigKey{
		BranchID:    branchId,
		ComponentID: Component,
		ID:          configId,
	})
	return client.NewAPIRequest(client.NoResult{}, request)
}

func DeleteSandboxJobRequest(configId ConfigID, sandboxId SandboxID) client.APIRequest[client.NoResult] {
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
