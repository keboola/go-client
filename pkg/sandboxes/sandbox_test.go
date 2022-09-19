package sandboxes_test

import (
	"context"
	"testing"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/jobsqueueapi"
	"github.com/keboola/go-client/pkg/sandboxes"
	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/keboola/go-utils/pkg/testproject"
	"github.com/stretchr/testify/assert"
)

func TestCreateAndDeleteSandbox(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, sapiClient, queueClient := clientsForAnEmptyProject(t)

	// Get default branch
	branch, err := storageapi.GetDefaultBranchRequest().Send(ctx, sapiClient)
	assert.NoError(t, err)
	assert.NotNil(t, branch)

	var configId sandboxes.ConfigID
	var sandboxId sandboxes.SandboxID

	// Create sandbox
	{
		// Create sandbox config (so UI can see it)
		sandboxConfig, err0 := sandboxes.CreateSandboxConfigRequest(branch.ID, "test").Send(ctx, sapiClient)
		assert.NoError(t, err0)
		assert.NotNil(t, sandboxConfig)

		// Create sandbox from config
		params := sandboxes.SandboxParams{
			Type:             "python",
			Shared:           false,
			ExpireAfterHours: 1,
			Size:             sandboxes.SandboxSizeSmall,
		}
		_, err1 := sandboxes.CreateSandboxJobRequest(sandboxConfig.ID, params).Send(ctx, queueClient)
		assert.NoError(t, err1)

		// Get sandbox config
		// The initial config does not have the sandbox id, because the sandbox has not been created yet,
		// so we need to fetch the sandbox config after the sandbox create job finishes.
		// The sandbox id is separate from the sandbox config id, and we need both to delete the sandbox.
		config, err2 := sandboxes.GetSandboxConfigRequest(branch.ID, sandboxConfig.ID).Send(ctx, sapiClient)
		assert.NoError(t, err2)
		assert.NotNil(t, config)

		configId = config.ID
		idParam, found, err3 := config.Content.GetNested("parameters.id")
		assert.NoError(t, err3)
		assert.True(t, found, "configuration is missing parameters.id")
		sandboxId = sandboxes.SandboxID(idParam.(string))
	}

	// Delete sandbox
	{
		// Delete sandbox (this stops the instance and deletes it)
		_, err0 := sandboxes.DeleteSandboxJobRequest(configId, sandboxId).Send(ctx, queueClient)
		assert.NoError(t, err0)

		// Delete sandbox config (so it is no longer visible in UI)
		_, err1 := sandboxes.DeleteSandboxConfigRequest(branch.ID, configId).Send(ctx, sapiClient)
		assert.NoError(t, err1)
	}
}

func clientsForAnEmptyProject(t *testing.T) (*testproject.Project, client.Sender, client.Sender) {
	ctx := context.Background()
	project := testproject.GetTestProject(t)

	// Get Storage API client
	storageApiClient := storageapi.ClientWithHostAndToken(client.NewTestClient(), project.StorageAPIHost(), project.StorageAPIToken())

	// Clean project
	if _, err := storageapi.CleanProjectRequest().Send(ctx, storageApiClient); err != nil {
		t.Fatalf(`cannot clean project "%d": %s`, project.ID(), err)
	}

	// Get Queue API and Sandboxes API hosts
	index, err := storageapi.IndexRequest().Send(ctx, storageApiClient)
	assert.NoError(t, err)
	jobsQueueHost, found := index.AllServices().URLByID("queue")
	assert.True(t, found)

	// Get Queue client
	jobsQueueApiClient := jobsqueueapi.ClientWithHostAndToken(client.NewTestClient(), jobsQueueHost.String(), project.StorageAPIToken())

	return project, storageApiClient, jobsQueueApiClient
}
